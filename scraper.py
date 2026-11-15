import asyncio
import json
import logging
from pathlib import Path
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

# সার্ভার রেট-লিমিট যেন না দেয় সেজন্য সেমাফোর ৫ রাখা হয়েছে
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
                # যদি কোনো চ্যানেলের সরাসরি ইউআরএল থাকে
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
    """প্রতিটি চ্যানেলের gw টোকেন দিয়ে /api/play থেকে রিয়েল স্ট্রিমিং URL আনা"""
    if "direct_url" in channel:
        return {
            "extinf": channel["extinf"],
            "url": channel["direct_url"]
        }

    token = channel.get("token")
    if not token:
        return None

    async with SEMAPHORE:
        try:
            params = {"c": token}
            resp = await client.get(PLAY_API, params=params, headers=HEADERS, timeout=12.0)
            if resp.status_code == 200:
                data = resp.json()
                stream_url = data.get("url")
                if stream_url:
                    return {
                        "extinf": channel["extinf"],
                        "url": stream_url
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

        log.info(f"Resolving real stream URLs for {len(raw_channels)} channels...")
        tasks = [resolve_channel_stream(client, ch) for ch in raw_channels]
        resolved_results = await asyncio.gather(*tasks)

        valid_streams = [res for res in resolved_results if res is not None]
        log.info(f"Successfully resolved {len(valid_streams)} playable streams.")

        # ১. M3U প্লেলিস্ট তৈরি (হেডার পাইপ সহ যেন সব প্লেয়ারে সাপোর্ট করে)
        m3u_lines = ["#EXTM3U"]
        json_data = []

        for item in valid_streams:
            extinf = item["extinf"]
            stream_url = item["url"]

            # প্লেয়ারের জন্য রেফারার ও ইউজার এজেন্ট বাইন্ডিং
            play_link = f"{stream_url}|Referer={REFERER_URL}&Origin={ORIGIN_URL}&User-Agent={USER_AGENT}"

            m3u_lines.append(extinf)
            m3u_lines.append(f"#EXTVLCOPT:http-referrer={REFERER_URL}")
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

        log.info(f"Saved {len(valid_streams)} channels to {M3U_OUTPUT.name} and {JSON_OUTPUT.name}.")


if __name__ == "__main__":
    asyncio.run(main())
