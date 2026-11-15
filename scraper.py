import asyncio
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin
import httpx

# কনফিগারেশন ও রুট পাথ
ROOT_DIR = Path(__file__).resolve().parent

API_BASE = "https://limon-iptv-link.limon20202.workers.dev"
CHANNELS_API = f"{API_BASE}/api/channels"
PLAY_API = f"{API_BASE}/api/play"

REFERER_URL = "https://limon-iptv-pro.vercel.app/"
ORIGIN_URL = "https://limon-iptv-pro.vercel.app"

M3U_OUTPUT = ROOT_DIR / "playlist.m3u"
JSON_OUTPUT = ROOT_DIR / "playlist.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Referer": REFERER_URL,
    "Origin": ORIGIN_URL,
    "Accept": "*/*"
}

# সার্ভার যাতে রেট-লিমিট না দেয় সেজন্য সেমাফোর ৫ রাখা হয়েছে
SEMAPHORE = asyncio.Semaphore(5)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger("IPTV_SCRAPER")


async def fetch_channels_raw(client: httpx.AsyncClient) -> list[dict]:
    """চ্যানেল তালিকা ও সংশ্লিষ্ট টোকেন সংগ্রহ করা"""
    channels = []
    try:
        log.info(f"Fetching channels from {CHANNELS_API} ...")
        resp = await client.get(CHANNELS_API, headers=HEADERS, timeout=20.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch channels: Status {resp.status_code}")
            return channels

        lines = resp.text.splitlines()
        current_extinf = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("#EXTINF"):
                current_extinf = line
            elif line.startswith("gw:"):
                token = line[3:].strip()
                if current_extinf and token:
                    channels.append({
                        "extinf": current_extinf,
                        "token": token
                    })
                    current_extinf = None
            elif line.startswith("http"):
                if current_extinf:
                    channels.append({
                        "extinf": current_extinf,
                        "direct_url": line
                    })
                    current_extinf = None

        log.info(f"Parsed {len(channels)} total channels from response.")
    except Exception as e:
        log.error(f"Error reading channels API: {e}")

    return channels


async def resolve_channel_stream(client: httpx.AsyncClient, channel: dict) -> dict | None:
    """
    টোকেন দিয়ে /api/play কল করে /s/ মাস্টার লিঙ্ক নেওয়া,
    এবং তা থেকে সরাসরি প্লেয়াবল /r/ স্ট্রিম লিঙ্ক বের করা।
    """
    if "direct_url" in channel:
        direct = channel["direct_url"]
        if "/s/" in direct:
            direct = direct.replace("/s/", "/r/")
        return {
            "extinf": channel["extinf"],
            "url": direct
        }

    token = channel.get("token")
    if not token:
        return None

    async with SEMAPHORE:
        try:
            # ধাপ ১: /api/play থেকে প্রাথমিক URL নেওয়া
            params = {"c": token}
            resp = await client.get(PLAY_API, params=params, headers=HEADERS, timeout=12.0)
            if resp.status_code != 200:
                return None

            data = resp.json()
            initial_url = data.get("url")
            if not initial_url:
                return None

            # ধাপ ২: /s/ মাস্টার ফাইল ফেচ করে আসল /r/ স্ট্রিম লিঙ্কটি বের করা
            final_stream_url = None
            try:
                manifest_resp = await client.get(initial_url, headers=HEADERS, timeout=10.0)
                if manifest_resp.status_code == 200:
                    text = manifest_resp.text
                    # মাস্টার ফাইলে থাকা /r/ লিঙ্ক খোঁজা
                    r_match = re.search(r'(https?://[^\s"\'<>]*/r/[^\s"\'<>]+)', text)
                    if r_match:
                        final_stream_url = r_match.group(1).strip()
                    else:
                        for line in text.splitlines():
                            line = line.strip()
                            if line and not line.startswith("#"):
                                final_stream_url = urljoin(initial_url, line)
                                break
            except Exception:
                pass

            # যদি কোনো কারণে মাস্টার ফেচ না হয়, তবে সরাসরি /s/ কে /r/ দিয়ে রূপান্তর করা
            if not final_stream_url:
                if "/s/" in initial_url:
                    final_stream_url = initial_url.replace("/s/", "/r/")
                else:
                    final_stream_url = initial_url

            # নিশ্চিত করা যেন লিঙ্কটি সরাসরি /r/ হয়
            if "/s/" in final_stream_url:
                final_stream_url = final_stream_url.replace("/s/", "/r/")

            # লিঙ্কটির শেষে .m3u8 নিশ্চিত করা
            if not final_stream_url.endswith(".m3u8") and "?" not in final_stream_url:
                final_stream_url += ".m3u8"

            return {
                "extinf": channel["extinf"],
                "url": final_stream_url
            }

        except Exception as e:
            log.debug(f"Error resolving token: {e}")
            return None


async def main():
    log.info("Starting IPTV Playlist Generator...")
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        raw_channels = await fetch_channels_raw(client)
        if not raw_channels:
            log.warning("No channels found! Exiting.")
            return

        log.info(f"Resolving real playable /r/ streams for {len(raw_channels)} channels...")
        tasks = [resolve_channel_stream(client, ch) for ch in raw_channels]
        resolved_results = await asyncio.gather(*tasks)

        valid_streams = [res for res in resolved_results if res is not None]
        log.info(f"Successfully resolved {len(valid_streams)} playable /r/ streams.")

        # M3U প্লেলিস্ট ও JSON তৈরি
        m3u_lines = ["#EXTM3U"]
        json_data = []

        for item in valid_streams:
            extinf = item["extinf"]
            stream_url = item["url"]

            # যেকোনো আইপিটিভি প্লেয়ারের জন্য পাইপলাইন হেডার বাইন্ডিং
            play_link = f"{stream_url}|Referer={REFERER_URL}&Origin={ORIGIN_URL}&User-Agent={USER_AGENT}"

            m3u_lines.append(extinf)
            m3u_lines.append(f"#EXTVLCOPT:http-referrer={REFERER_URL}")
            m3u_lines.append(f"#EXTVLCOPT:http-origin={ORIGIN_URL}")
            m3u_lines.append(f"#EXTVLCOPT:http-user-agent={USER_AGENT}")
            m3u_lines.append(play_link)

            json_data.append({
                "info": extinf,
                "url": stream_url,
                "playable_link": play_link
            })

        # ফাইল সেভ
        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")
        JSON_OUTPUT.write_text(json.dumps(json_data, indent=2, ensure_ascii=False), encoding="utf-8")

        log.info(f"Saved {len(valid_streams)} playable channels to {M3U_OUTPUT.name} and {JSON_OUTPUT.name}.")


if __name__ == "__main__":
    asyncio.run(main())
