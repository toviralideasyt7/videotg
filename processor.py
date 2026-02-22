import os
import sys
import subprocess
import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
import re
import asyncio
import json
import urllib.parse
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None

# Configuration from Environment Variables
API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_SESSION = os.getenv('TELEGRAM_SESSION', '')
API_URL = os.getenv('API_URL', '')
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')
PDF_PROXY_URL = os.getenv('PDF_PROXY_URL', 'https://careerwillvideo-worker.xapipro.workers.dev/api/proxy-download')
DOWNLOAD_CHUNK_SIZE = int(os.getenv('DOWNLOAD_CHUNK_SIZE', 262144))  # 256KB
UPLOAD_PART_SIZE_KB = max(64, min(512, int(os.getenv('UPLOAD_PART_SIZE_KB', 512))))
UPLOAD_PROGRESS_STEP_PERCENT = max(1, min(25, int(os.getenv('UPLOAD_PROGRESS_STEP_PERCENT', 5))))
ITEM_COOLDOWN_SECONDS = float(os.getenv('ITEM_COOLDOWN_SECONDS', '1'))


def create_http_session():
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP_SESSION = create_http_session()


def stream_response_to_file(response, output_filename):
    with open(output_filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if chunk:
                f.write(chunk)


def build_browser_headers(target_url):
    target_origin = urllib.parse.urlsplit(target_url).scheme + "://" + urllib.parse.urlsplit(target_url).netloc
    return {
        'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="120", "Chromium";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-IN,en;q=0.9,hi-IN;q=0.8,hi;q=0.7',
        'Referer': target_origin + '/',
        'Origin': target_origin,
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }


def is_auth_block(status_code, text):
    t = (text or '').lower()
    return status_code in (401, 403) and (
        'not authorized' in t or
        'unauthorized' in t or
        'access denied' in t
    )


def download_pdf_via_worker(pdf_url, output_filename):
    encoded_target = urllib.parse.quote(pdf_url, safe='')
    proxy_url = f"{PDF_PROXY_URL}?url={encoded_target}"
    headers = build_browser_headers(pdf_url)
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"📡 Requesting download from Cloudflare Worker proxy... (attempt {attempt}/{max_attempts})")
            r = HTTP_SESSION.get(proxy_url, headers=headers, stream=True, timeout=(20, 300))
            if r.status_code == 200:
                stream_response_to_file(r, output_filename)
                return True

            error_preview = (r.text or '')[:1000]
            cf_ray = r.headers.get('cf-ray', 'n/a')
            server = r.headers.get('server', 'n/a')
            print(f"❌ PDF Proxy Download Error: {r.status_code} - {error_preview}")
            print(f"📋 Proxy response headers: server={server}, cf-ray={cf_ray}")
            if r.status_code == 403:
                # Hard auth/WAF block is generally not transient; move to next strategy immediately.
                return False
            if attempt < max_attempts and r.status_code in (403, 408, 409, 425, 429, 500, 502, 503, 504):
                sleep_s = attempt * 3
                print(f"⏳ Retrying proxy in {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            return False
        except requests.RequestException as e:
            print(f"❌ PDF Proxy Exception: {e}")
            if attempt < max_attempts:
                sleep_s = attempt * 3
                print(f"⏳ Retrying proxy in {sleep_s}s...")
                time.sleep(sleep_s)
            else:
                return False

    return False


def download_pdf_via_curl_cffi(pdf_url, output_filename):
    if curl_requests is None:
        print("⚠️ curl_cffi not available, skipping direct browser-impersonation fallback.")
        return False

    safe_pdf_url = pdf_url.replace(" ", "%20")
    headers = build_browser_headers(safe_pdf_url)

    max_attempts = 2
    impersonations = [
        os.getenv('CURL_IMPERSONATE', 'chrome120'),
        'chrome124',
        'chrome'
    ]
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"📡 Trying direct browser-impersonated PDF download... (attempt {attempt}/{max_attempts})")
            for imp in impersonations:
                r = curl_requests.get(
                    safe_pdf_url,
                    headers=headers,
                    impersonate=imp,
                    timeout=300
                )
                if r.status_code == 200:
                    with open(output_filename, 'wb') as f:
                        f.write(r.content)
                    return True
                error_preview = (r.text or '')[:500]
                print(f"❌ Direct PDF fallback error [{imp}]: {r.status_code} - {error_preview}")
                if is_auth_block(r.status_code, error_preview):
                    return False
        except Exception as e:
            print(f"❌ Direct PDF fallback exception: {e}")
        if attempt < max_attempts:
            sleep_s = attempt * 2
            print(f"⏳ Retrying direct fallback in {sleep_s}s...")
            time.sleep(sleep_s)
    return False

async def process_item(client, item):
    url = item.get('url', '')
    title = item.get('title', '')
    folder_id = item.get('folder_id')
    peer = item.get('peer', 'me')
    media_type = item.get('type', 'video')
    db_id = item.get('id')
    bearer_token = item.get('token', ADMIN_TOKEN)

    # Create a safe filename from the title
    safe_title = re.sub(r'[^\w\s-]', '', title).strip().lower()
    safe_title = re.sub(r'[-\s]+', '_', safe_title)
    
    is_pdf = (media_type == 'pdf') or url.lower().endswith('.pdf')
    is_youtube = 'youtube.com' in url.lower() or 'youtu.be' in url.lower()
    
    if is_pdf:
        output_filename = f"{safe_title}.pdf"
        mime_type = "application/pdf"
    else:
        output_filename = f"{safe_title}.mp4"
        mime_type = "video/mp4"

    print(f"\n🎬 Processing Remote Upload:")
    print(f"🔗 URL: {url}")
    print(f"📂 Title: {title} -> {output_filename}")
    print(f"📁 Folder ID: {folder_id}")
    print(f"🏷️ Type: {media_type}")

    # Step 1: Download
    print("⏳ Stage 1: Downloading...")
    if is_pdf:
        # Strategy order: Worker -> direct browser impersonation.
        if not download_pdf_via_worker(url, output_filename):
            print("⚠️ Worker proxy path failed. Falling back to direct browser-impersonation...")
            if not download_pdf_via_curl_cffi(url, output_filename):
                print("❌ PDF download failed on all strategies.")
                return
    elif is_youtube:
        print(f"⏭️ SKIPPED: YouTube videos are currently ignored per configuration.")
        return
    else:
        # Stream via FFmpeg
        command = [
            'ffmpeg',
            '-i', url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            output_filename,
            '-y'
        ]
        process = subprocess.run(command, capture_output=True, text=True)
        if process.returncode != 0:
            print(f"❌ FFmpeg Error: {process.stderr}")
            return

    print("✅ Download Complete!")

    # Step 2: Upload to Telegram via Telethon
    print("⏳ Stage 2: Uploading to Telegram...")
    # Determine the file size for metadata
    file_size = os.path.getsize(output_filename)
        
    # Bot-compatible peer resolution: Convert string to int for channel IDs
    target_peer = peer
    if peer.startswith('-100'):
        # This is a channel ID, convert to integer for bot compatibility
        target_peer = int(peer)
        
    caption = f"📄 {title}" if is_pdf else f"🎥 {title}"
    
    last_printed_percent = -1
    def progress_callback(current, total):
        nonlocal last_printed_percent
        percent = int((current / total) * 100)
        # Log periodically to reduce console overhead in CI.
        if percent % UPLOAD_PROGRESS_STEP_PERCENT == 0 and percent != last_printed_percent:
            print(f"📡 Uploading... {percent}% ({current / 1024 / 1024:.2f}MB / {total / 1024 / 1024:.2f}MB)")
            last_printed_percent = percent
            
    # Uploading with Retry Logic for Connection Drops [Errno 104]
    max_retries = 3
    retry_count = 0
    msg = None
    
    while retry_count < max_retries:
        try:
            msg = await client.send_file(
                target_peer,
                output_filename,
                caption=caption,
                force_document=True,
                part_size_kb=UPLOAD_PART_SIZE_KB,
                progress_callback=progress_callback
            )
            break # Success! Break out of the retry loop.
        except FloodWaitError as e:
            wait_time = int(getattr(e, 'seconds', 30)) + 1
            print(f"\n⚠️ Telegram FloodWait for {wait_time}s.")
            await asyncio.sleep(wait_time)
            retry_count += 1
            continue
        except (ConnectionError, asyncio.TimeoutError, OSError) as e:
            retry_count += 1
            print(f"\n⚠️ Telegram Connection Dropped (Attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                print("❌ Max retries reached. Upload failed.")
                return # Abort this item and move to next
            if not client.is_connected():
                await client.connect()
            sleep_s = retry_count * 8
            print(f"⏳ Cooling down for {sleep_s} seconds before retrying...")
            await asyncio.sleep(sleep_s)
        except Exception as e:
            print(f"\n❌ Fatal Upload Error: {e}")
            return
            
    if not msg:
        print("❌ Failed to resolve message object after uploads.")
        return
        
    telegram_id = str(msg.id)
    print(f"\n✅ Upload Complete! Telegram Message ID: {telegram_id}")

    # Step 3: Finalize in Cloudflare Worker
    print("⏳ Stage 3: Finalizing Database Entry...")
    
    final_name = title if title.endswith(f".{output_filename.split('.')[-1]}") else f"{title}.{output_filename.split('.')[-1]}"
    
    payload = {
        "name": final_name,
        "size": file_size,
        "mime_type": mime_type,
        "folder_id": folder_id,
        "telegram_id": telegram_id
    }
    
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }
    
    res = requests.post(f"{API_URL}/api/upload/finalize", json=payload, headers=headers)
    
    if res.status_code == 200:
        print("✨ Backend Complete! Notifying Cloudflare Worker Sync...")
        # Mark as uploaded strictly if successful
        sync_payload = {"id": db_id, "type": media_type}
        sync_headers = {
            "Authorization": f"Bearer {ADMIN_TOKEN}",
            "Content-Type": "application/json"
        }
        sync_res = requests.post(f"https://careerwillvideo-worker.xapipro.workers.dev/api/mark_uploaded", json=sync_payload, headers=sync_headers)
        if sync_res.status_code == 200:
             print("✅ Fully Synced across all databases!")
        else:
             print(f"⚠️ Failed to mark item as uploaded in Worker DB: {sync_res.text}")
    else:
        print(f"❌ Finalization failed: {res.text}")

    # Cleanup
    if os.path.exists(output_filename):
        os.remove(output_filename)

async def main():
    if len(sys.argv) < 2:
        print("Usage: python processor.py '[{\"url\":\"...\", ...}]'")
        return

    try:
        payload_str = sys.argv[1]
        items = json.loads(payload_str)
    except Exception as e:
        print(f"Failed to parse input JSON array: {e}")
        return

    if not isinstance(items, list):
        items = [items] # fallback if it's a single object

    print(f"🚀 Booting GitHub Remote Uploader. Preparing to process {len(items)} items sequentially...")

    # Boot the global reusable Telegram Auth Context
    client = TelegramClient(StringSession(TELEGRAM_SESSION), API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    
    if not TELEGRAM_SESSION:
        print("\n\n⚠️ IMPORTANT: No TELEGRAM_SESSION found in environment!")
        print("Please save the following string to your GitHub Secrets as 'TELEGRAM_SESSION' to prevent FloodWaitError bans on future runs:")
        print(client.session.save())
        print("--------------------------------------------------\n\n")

    async with client:
         for item in items:
             await process_item(client, item)
             # Cool down gracefully between sequential uploads to avoid telegram generic restrictions
             await asyncio.sleep(ITEM_COOLDOWN_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
