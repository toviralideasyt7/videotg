import os
import sys
import subprocess
import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import re
import asyncio

# Configuration from Environment Variables
API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_SESSION = os.getenv('TELEGRAM_SESSION', '')
API_URL = os.getenv('API_URL', '')
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')

async def main():
    # Input from client_payload
    url = sys.argv[1]
    title = sys.argv[2]
    folder_id = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != 'null' else None
    peer = sys.argv[4] if len(sys.argv) > 4 else 'me'
    media_type = sys.argv[5] if len(sys.argv) > 5 else 'video'
    bearer_token = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] != 'null' else ADMIN_TOKEN

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

    print(f"🎬 Processing Remote Upload:")
    print(f"🔗 URL: {url}")
    print(f"📂 Title: {title} -> {output_filename}")
    print(f"📁 Folder ID: {folder_id}")
    print(f"🏷️ Type: {media_type}")

    # Step 1: Download
    print("⏳ Stage 1: Downloading...")
    if is_pdf:
        # Download Native PDF
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        r = requests.get(url, stream=True, headers=headers)
        if r.status_code == 200:
            with open(output_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            print(f"❌ PDF Download Error: {r.status_code}")
            return
    elif is_youtube:
        # Download via yt-dlp
        command = [
            'yt-dlp',
            '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            '-o', output_filename,
            url
        ]
        process = subprocess.run(command, capture_output=True, text=True)
        if process.returncode != 0:
            print(f"❌ yt-dlp Error: {process.stderr}")
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
    
    # Use StringSession to avoid FloodWaitErrors from repeated bot logins on GitHub Actions
    client = TelegramClient(StringSession(TELEGRAM_SESSION), API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    
    # If a new session was created (e.g. secret is empty), print it so the user can save it!
    if not TELEGRAM_SESSION:
        print("\n\n⚠️ IMPORTANT: No TELEGRAM_SESSION found in environment!")
        print("Please save the following string to your GitHub Secrets as 'TELEGRAM_SESSION' to prevent FloodWaitError bans on future runs:")
        print(client.session.save())
        print("--------------------------------------------------\n\n")
    async with client:
        # Determine the file size for metadata
        file_size = os.path.getsize(output_filename)
        
        # Bot-compatible peer resolution: Convert string to int for channel IDs
        target_peer = peer
        if peer.startswith('-100'):
            # This is a channel ID, convert to integer for bot compatibility
            target_peer = int(peer)
        
        caption = f"📄 {title}" if is_pdf else f"🎥 {title}"
        
        # Uploading...
        msg = await client.send_file(
            target_peer,
            output_filename,
            caption=caption,
            force_document=True
        )
        
        telegram_id = str(msg.id)
        print(f"✅ Upload Complete! Telegram Message ID: {telegram_id}")

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
        print("✨ All done! File is now visible in your Telegram Drive.")
    else:
        print(f"❌ Finalization failed: {res.text}")

    # Cleanup
    if os.path.exists(output_filename):
        os.remove(output_filename)

if __name__ == "__main__":
    asyncio.run(main())
