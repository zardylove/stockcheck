import requests
from bs4 import BeautifulSoup
import time
import os
import random

# Load webhook from environment variable
DISCORD_WEBHOOK = os.getenv("_151_BB")
CHECK_INTERVAL = 10  # Time in seconds between full scans

# Desktop browser user-agent (default)
DESKTOP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
}

# Mobile browser user-agent (fallback for stricter sites)
MOBILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
}

# Out of stock phrases
OUT_OF_STOCK_TERMS = [
    "out of stock", "sold out", "unavailable", "notify when available",
    "currently unavailable", "temporarily out of stock", "pre-order",
    "coming soon", "not in stock", "no stock available", "stock: 0"
]

# In stock phrases
IN_STOCK_TERMS = ["in stock", "available now", "buy now"]

def load_urls(file_path="151bb.txt"):
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def send_alert(url):
    data = {
        "content": f"🚨 **STOCK ALERT** 🚨\nProduct may be in stock:\n{url}"
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=data)
        if r.status_code == 204:
            print(f"✅ Alert sent for {url}")
        else:
            print(f"⚠️ Failed to send alert ({r.status_code})")
    except Exception as e:
        print(f"❌ Discord error: {e}")

def get_headers_for_url(url):
    if "very.co.uk" in url or "freemans.com" in url:
        return MOBILE_HEADERS
    return DESKTOP_HEADERS

def check_url(url):
    try:
        headers = get_headers_for_url(url)
        r = requests.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text().lower()

        # Check for known out-of-stock indicators
        if any(term in text for term in OUT_OF_STOCK_TERMS):
            print(f"❌ Not in stock: {url}")
            return

        # Look for "add to cart" buttons (updated with string= for compatibility)
        add_buttons = soup.find_all(['button', 'input'], 
            string=lambda t: t and 'add to' in t.lower())
        enabled_buttons = [
            btn for btn in add_buttons 
            if not btn.get('disabled') and 'disabled' not in btn.get('class', [])
        ]

        if any(term in text for term in IN_STOCK_TERMS) or enabled_buttons:
            print(f"🔔 Possible stock at: {url}")
            send_alert(url)
        else:
            print(f"❌ Not in stock: {url}")
    except Exception as e:
        print(f"⚠️ Error checking {url}: {e}")

def main():
    urls = load_urls()
    while True:
        print(f"🔍 Checking {len(urls)} URLs...")
        for url in urls:
            check_url(url)
            time.sleep(random.uniform(1, 2))  # delay to avoid rate limits
        print(f"⏱️ Waiting {CHECK_INTERVAL}s before next scan...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
