import os
import sys
import subprocess
import requests
from telethon import TelegramClient, events
import asyncio

# Configuration from Environment Variables
API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
API_URL = os.getenv('API_URL', '')
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')

async def main():
    # Input from client_payload
    url = sys.argv[1]
    title = sys.argv[2]
    folder_id = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != 'null' else None
    peer = sys.argv[4] if len(sys.argv) > 4 else 'me'

    print(f"🎬 Processing Remote Upload:")
    print(f"🔗 URL: {url}")
    print(f"📂 Title: {title}")
    print(f"📁 Folder ID: {folder_id}")

    output_filename = "temp_video.mp4"

    # Step 1: Download using FFmpeg
    print("⏳ Stage 1: Downloading & Converting...")
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
    client = TelegramClient('remote_bot', API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    async with client:
        # Determine the file size for metadata
        file_size = os.path.getsize(output_filename)
        
        # Bot-compatible peer resolution: Convert string to int for channel IDs
        target_peer = peer
        if peer.startswith('-100'):
            # This is a channel ID, convert to integer for bot compatibility
            target_peer = int(peer)
        
        # Uploading...
        msg = await client.send_file(
            target_peer,
            output_filename,
            caption=f"🎥 {title}",
            force_document=True
        )
        
        telegram_id = str(msg.id)
        print(f"✅ Upload Complete! Telegram Message ID: {telegram_id}")

    # Step 3: Finalize in Cloudflare Worker
    print("⏳ Stage 3: Finalizing Database Entry...")
    payload = {
        "name": title if title.endswith('.mp4') else f"{title}.mp4",
        "size": file_size,
        "mime_type": "video/mp4",
        "folder_id": folder_id,
        "telegram_id": telegram_id
    }
    
    headers = {
        "Authorization": f"Bearer {ADMIN_TOKEN}",
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
