import os
import sys
import subprocess
import requests
from telethon import TelegramClient
from telethon.sessions import StringSession, MemorySession
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError
import re
import asyncio
import json

# Configuration from Environment Variables
API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_SESSION = os.getenv('TELEGRAM_SESSION', '')
API_URL = os.getenv('API_URL', '')
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')

MAX_VIDEOS_PER_RUN = max(1, int(os.getenv('MAX_VIDEOS_PER_RUN', 3)))
UPLOAD_PART_SIZE_KB = max(64, min(512, int(os.getenv('UPLOAD_PART_SIZE_KB', 512))))
UPLOAD_PROGRESS_STEP_PERCENT = max(1, min(25, int(os.getenv('UPLOAD_PROGRESS_STEP_PERCENT', 10))))
ITEM_COOLDOWN_SECONDS = float(os.getenv('ITEM_COOLDOWN_SECONDS', '0.2'))


def is_video_item(item):
    media_type = (item.get('type') or '').lower()
    url = (item.get('url') or '').lower()
    if media_type and media_type != 'video':
        return False
    if url.endswith('.pdf'):
        return False
    return True


def select_videos(items):
    videos = [item for item in items if is_video_item(item)]
    selected = videos[:MAX_VIDEOS_PER_RUN]
    return selected, len(videos), len(selected)


def create_telegram_client(session):
    return TelegramClient(
        session,
        API_ID,
        API_HASH,
        auto_reconnect=True,
        connection_retries=6,
        request_retries=6,
        retry_delay=2,
        timeout=120
    )


async def create_started_client():
    # Prefer configured StringSession for continuity, but auto-heal if Telegram revoked it.
    if TELEGRAM_SESSION:
        client = create_telegram_client(StringSession(TELEGRAM_SESSION))
        try:
            await client.start(bot_token=BOT_TOKEN)
            return client, "string"
        except AuthKeyDuplicatedError:
            print("WARNING: TELEGRAM_SESSION auth key was duplicated across IPs and revoked by Telegram.")
            print("WARNING: Falling back to fresh in-memory bot session for this run.")
            try:
                await client.disconnect()
            except Exception:
                pass

    client = create_telegram_client(MemorySession())
    await client.start(bot_token=BOT_TOKEN)
    return client, "memory"


async def process_item(client, item):
    url = item.get('url', '')
    title = item.get('title', '')
    folder_id = item.get('folder_id')
    peer = item.get('peer', 'me')
    media_type = 'video'
    db_id = item.get('id')
    bearer_token = item.get('token', ADMIN_TOKEN)

    if 'youtube.com' in url.lower() or 'youtu.be' in url.lower():
        print("SKIPPED: YouTube videos are currently ignored per configuration.")
        return

    safe_title = re.sub(r'[^\w\s-]', '', title).strip().lower()
    safe_title = re.sub(r'[-\s]+', '_', safe_title)
    output_filename = f"{safe_title}.mp4"
    mime_type = "video/mp4"

    print("\nProcessing Remote Upload:")
    print(f"URL: {url}")
    print(f"Title: {title} -> {output_filename}")
    print(f"Folder ID: {folder_id}")
    print("Type: video")

    try:
        # Stage 1: Download video via FFmpeg
        print("Stage 1: Downloading...")
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
            print(f"FFmpeg Error: {process.stderr}")
            return

        print("Download Complete.")

        # Stage 2: Upload to Telegram via Telethon
        print("Stage 2: Uploading to Telegram...")
        file_size = os.path.getsize(output_filename)

        target_peer = peer
        if isinstance(peer, str) and peer.startswith('-100'):
            target_peer = int(peer)

        caption = f"VIDEO {title}"

        last_printed_percent = -1

        def progress_callback(current, total):
            nonlocal last_printed_percent
            percent = int((current / total) * 100)
            if percent % UPLOAD_PROGRESS_STEP_PERCENT == 0 and percent != last_printed_percent:
                print(f"Uploading... {percent}% ({current / 1024 / 1024:.2f}MB / {total / 1024 / 1024:.2f}MB)")
                last_printed_percent = percent

        max_retries = 4
        retry_count = 0
        msg = None

        while retry_count < max_retries:
            try:
                if not client.is_connected():
                    await client.connect()

                msg = await client.send_file(
                    target_peer,
                    output_filename,
                    caption=caption,
                    force_document=True,
                    part_size_kb=UPLOAD_PART_SIZE_KB,
                    progress_callback=progress_callback
                )
                break
            except asyncio.CancelledError as e:
                retry_count += 1
                print(f"Upload cancelled during network reset (Attempt {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    print("Max retries reached after cancellation. Upload failed.")
                    return
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(retry_count * 8)
            except FloodWaitError as e:
                retry_count += 1
                wait_time = int(getattr(e, 'seconds', 30)) + 1
                print(f"FloodWait for {wait_time}s.")
                if retry_count >= max_retries:
                    print("Max retries reached after FloodWait. Upload failed.")
                    return
                await asyncio.sleep(wait_time)
            except (ConnectionError, asyncio.TimeoutError, OSError) as e:
                retry_count += 1
                print(f"Telegram connection dropped (Attempt {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    print("Max retries reached. Upload failed.")
                    return
                try:
                    if not client.is_connected():
                        await client.connect()
                except Exception:
                    pass
                await asyncio.sleep(retry_count * 8)
            except Exception as e:
                print(f"Fatal upload error: {e}")
                return

        if not msg:
            print("Failed to resolve message object after upload retries.")
            return

        telegram_id = str(msg.id)
        print(f"Upload Complete! Telegram Message ID: {telegram_id}")

        # Stage 3: Finalize in backend and mark uploaded in worker DB
        print("Stage 3: Finalizing Database Entry...")

        final_name = title if title.endswith(".mp4") else f"{title}.mp4"
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
            print("Backend complete. Notifying Cloudflare Worker sync...")
            sync_payload = {"id": db_id, "type": media_type}
            sync_headers = {
                "Authorization": f"Bearer {ADMIN_TOKEN}",
                "Content-Type": "application/json"
            }
            sync_res = requests.post(
                "https://careerwillvideo-worker.xapipro.workers.dev/api/mark_uploaded",
                json=sync_payload,
                headers=sync_headers
            )
            if sync_res.status_code == 200:
                print("Fully synced across all databases.")
            else:
                print(f"Failed to mark item as uploaded in Worker DB: {sync_res.text}")
        else:
            print(f"Finalization failed: {res.text}")
    finally:
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
        items = [items]

    original_count = len(items)
    items, total_videos, selected_videos = select_videos(items)
    print(f"Booting GitHub Remote Uploader. Received {original_count} items.")
    print(f"Queue plan: videos only, uploading {selected_videos}/{total_videos} (max {MAX_VIDEOS_PER_RUN}).")

    if not items:
        print("No video items to process in this run.")
        return

    client, session_mode = await create_started_client()

    if session_mode == "memory" and TELEGRAM_SESSION:
        print("WARNING: Using temporary in-memory bot session because saved TELEGRAM_SESSION was invalid.")
        print("WARNING: Rotate/regenerate TELEGRAM_SESSION secret for persistent sessions.")
    elif not TELEGRAM_SESSION:
        print("WARNING: No TELEGRAM_SESSION found in environment.")
        print("Save this string to GitHub Secrets as TELEGRAM_SESSION to reduce reconnect issues:")
        print(client.session.save())

    async with client:
        for item in items:
            try:
                await process_item(client, item)
            except asyncio.CancelledError as e:
                print(f"Item processing cancelled due transient network issue: {e}")
                try:
                    if client.is_connected():
                        await client.disconnect()
                    await client.connect()
                except Exception as reconnect_error:
                    print(f"Reconnect after cancellation failed: {reconnect_error}")
            except Exception as e:
                print(f"Item processing crashed but runner will continue: {e}")

            await asyncio.sleep(ITEM_COOLDOWN_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        print("Main task was cancelled. Exiting gracefully.")
