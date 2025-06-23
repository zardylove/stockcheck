import requests
from bs4 import BeautifulSoup
import time
import os
import random

# Load webhook from environment variable
DISCORD_WEBHOOK = os.getenv("THE_KEY")
CHECK_INTERVAL = 30  # Time in seconds between full scans

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def load_urls(file_path="urls.txt"):
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

def check_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text().lower()
        
        # Check for out of stock indicators first
        out_of_stock_terms = [
            "out of stock", "sold out", "unavailable", "notify when available",
            "currently unavailable", "temporarily out of stock", "pre-order",
            "coming soon", "not in stock", "stock: 0"
        ]
        
        if any(term in text for term in out_of_stock_terms):
            print(f"❌ Not in stock: {url}")
            return
        
        # Look for positive stock indicators
        in_stock_terms = ["in stock", "available now", "buy now"]
        
        # Check for enabled add to cart buttons (more reliable)
        add_buttons = soup.find_all(['button', 'input'], 
                                   text=lambda t: t and 'add to' in t.lower())
        enabled_buttons = [btn for btn in add_buttons 
                          if not btn.get('disabled') and 'disabled' not in btn.get('class', [])]
        
        if any(term in text for term in in_stock_terms) or enabled_buttons:
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
            time.sleep(random.uniform(1, 2))  # Delay between requests
        print(f"⏱️ Waiting {CHECK_INTERVAL}s before next scan...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()

