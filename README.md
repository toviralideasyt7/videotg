# 🚀 GitHub Video Uploader for Telegram Drive

This repository is a standalone background processor for your **Telegram Drive**. It downloads video streams (like M3U8) and uploads them directly to your Telegram channel using GitHub Actions.

## 🛠️ Setup Instructions

1.  **Fork / Create Repository**: Create a new private (or public) repository on GitHub and paste these files.
2.  **Configure Secrets**: Go to `Settings -> Secrets and variables -> Actions` in your GitHub repo and add:
    *   `TELEGRAM_API_ID`: Your Telegram API ID.
    *   `TELEGRAM_API_HASH`: Your Telegram API Hash.
    *   `TELEGRAM_BOT_TOKEN`: The token for your Telegram Bot (from [@BotFather](https://t.me/BotFather)).
    *   `API_URL`: The URL of your Cloudflare Worker.
    *   `ADMIN_TOKEN`: Your dashboard access token.
3.  **Enable Repository Dispatch**: This is enabled by default for GitHub Actions.

## ⚙️ How it Works
When you trigger a "Remote Upload" from your Telegram Drive dashboard, it sends a `repository_dispatch` event to this repo. The `remote-upload.yml` workflow then:
1.  Spins up an Ubuntu runner.
2.  Downloads the video using FFmpeg.
3.  Uploads it to your Telegram channel.
4.  Notifies your Drive dashboard to make the file visible.

## 📄 License
MIT
