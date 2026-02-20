import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

async def main():
    print("========================================")
    print("   Telegram Session String Generator    ")
    print("========================================\n")
    
    API_ID = input("1. Enter your TELEGRAM_API_ID: ").strip()
    API_HASH = input("2. Enter your TELEGRAM_API_HASH: ").strip()
    BOT_TOKEN = input("3. Enter your TELEGRAM_BOT_TOKEN: ").strip()

    print("\n⏳ Logging in to Telegram...")
    try:
        client = TelegramClient(StringSession(), int(API_ID), API_HASH)
        await client.start(bot_token=BOT_TOKEN)
        
        session_string = client.session.save()
        
        print("\n✅ SUCCESS! Here is your TELEGRAM_SESSION string:\n")
        print(session_string)
        print("\n========================================")
        print("👆 Copy the long string above.")
        print("Go to your GitHub videotg Repo -> Settings -> Secrets.")
        print("Add a new secret named: TELEGRAM_SESSION")
        print("Paste the string as the value.")
        print("========================================")
    except Exception as e:
        print(f"\n❌ Error logging in: {e}")

if __name__ == "__main__":
    asyncio.run(main())
