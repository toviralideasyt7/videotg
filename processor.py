import os
import sys
import subprocess
import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import re
import asyncio
import json
import urllib.parse

# Configuration from Environment Variables
API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_SESSION = os.getenv('TELEGRAM_SESSION', '')
API_URL = os.getenv('API_URL', '')
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')

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
        # Route through Cloudflare Worker proxy to bypass datacenter IP WAF blocks
        safe_url = url.replace(" ", "%20")
        proxy_url = f"https://careerwillvideo-worker.xapipro.workers.dev/api/proxy-download?url={safe_url}"
        try:
            r = requests.get(proxy_url, stream=True)
            if r.status_code == 200:
                with open(output_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            else:
                print(f"❌ PDF Proxy Download Error: {r.status_code} - {r.text[:200]}")
                return
        except Exception as e:
            print(f"❌ PDF Proxy Exception: {e}")
            return
    elif is_youtube:
        # YouTube aggressively blocks ALL datacenter IPs (GitHub Actions, Azure, AWS, etc.)
        # No library (yt-dlp, pytubefix) can bypass this without a residential proxy.
        print("⏭️ SKIPPED: YouTube videos cannot be downloaded from GitHub Actions datacenter IPs.")
        print("   ℹ️ Upload this video manually from your local PC using: python processor.py")
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
        # Log every 5% to avoid spamming the GitHub Action stdout
        if percent % 5 == 0 and percent != last_printed_percent:
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
                progress_callback=progress_callback
            )
            break # Success! Break out of the retry loop.
        except (ConnectionError, asyncio.TimeoutError) as e:
            retry_count += 1
            print(f"\n⚠️ Telegram Connection Dropped (Attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                print("❌ Max retries reached. Upload failed.")
                return # Abort this item and move to next
            print("⏳ Cooling down for 10 seconds before retrying...")
            await asyncio.sleep(10)
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

    print(f"🚀 Booting GitHub Remote Uploader. Preparing to serialize {len(items)} items concurrently...")

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
             await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
