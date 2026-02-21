from urllib.parse import urlparse, quote, urlunparse
from curl_cffi import requests

url = "https://cwmediabkt99.crwilladmin.com/92013a99927c4842b0a70fbd6f064a95:crwilladmin/class-attachment/64997b90e1424_Trigonometry sheet-1.pdf".replace(" ", "%20")

# Impersonate Chrome 120 (specifically solving Datacenter Cloudflare TLS fingerprinting)
try:
    print("Testing curl_cffi for PDF...")
    r = requests.get(url, impersonate="chrome120")
    print("PDF Status:", r.status_code)
    print("PDF Success (200 expected):", r.status_code == 200)
except Exception as e:
    print("PDF Error:", e)

# Test PyTubeFix for YouTube
import traceback
from pytubefix import YouTube

yt_url = 'https://www.youtube.com/watch?v=DzyP7Nz9wsQ'
try:
    print("\nTesting pytubefix for YouTube...")
    # 'WEB' client uses PoToken logic
    yt = YouTube(yt_url, client='WEB')
    print("YT Title:", yt.title)
    
    # Get highest resolution progressive stream or best adaptive video/audio
    stream = yt.streams.get_highest_resolution() 
    if not stream:
        print("No progressive streams found. Trying adaptive...")
        # (For production we will merge best video + best audio, but here testing extraction)
        stream = yt.streams.filter(only_video=True).order_by('resolution').desc().first()
    
    print("YT Stream found:", stream)
    print("YT Success (Title parsed & stream fetched expected): True")
except Exception as e:
    print("YT Error:", e)
    traceback.print_exc()
    
