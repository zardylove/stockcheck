import requests
from bs4 import BeautifulSoup
import time
import os
import random
import re
import psycopg2
from psycopg2 import pool
import traceback
from urllib.parse import urljoin, urlparse, urlunparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# === PLAYWRIGHT SUPPORT ===
PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False

# === DECODO / SMARTPROXY SUPPORT ===
DECODO_BLOCKED_DOMAINS = {
    "thetoyplanet.co.uk",
    "johnlewis.com",
    "very.co.uk",
    "game.co.uk",
    "sportsdirect.com",
    "freemans.com",
    "studio.co.uk",
    "argos.co.uk",
    "currys.co.uk",
    # Add more as you discover blocks
}

def _host_for_url(url: str) -> str:
    try:
        host = url.split("/")[2].lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host

def proxies_for_url(url: str):
    host = _host_for_url(url)
    if not host or host not in DECODO_BLOCKED_DOMAINS:
        return None

    user = os.getenv("DECODO_USER")
    pw = os.getenv("DECODO_PASS")
    proxy_host = os.getenv("DECODO_HOST", "proxy.decodo.com")
    proxy_port = os.getenv("DECODO_PORT", "10000")

    if not user or not pw:
        return None

    proxy = f"http://{user}:{pw}@{proxy_host}:{proxy_port}"
    print(f"[PROXY] Using Decodo/Smartproxy for {url}")
    return {"http": proxy, "https": proxy}

def playwright_proxy_for_url(url: str):
    host = _host_for_url(url)
    if not host or host not in DECODO_BLOCKED_DOMAINS:
        return None

    user = os.getenv("DECODO_USER")
    pw = os.getenv("DECODO_PASS")
    proxy_host = os.getenv("DECODO_HOST", "proxy.decodo.com")
    proxy_port = os.getenv("DECODO_PORT", "10000")

    if not user or not pw:
        return None

    print(f"[PROXY] Playwright using Decodo/Smartproxy for {url}")
    return {
        "server": f"http://{proxy_host}:{proxy_port}",
        "username": user,
        "password": pw,
    }

# === DOMAINS THAT NEED PLAYWRIGHT ===
PLAYWRIGHT_DOMAINS = {
    "argos.co.uk",
    "freemans.com",
    "currys.co.uk",
    "forbiddenplanet.com",
    "game.co.uk",
    "houseoffraser.co.uk",
    "hamleys.com",
    "hillscards.co.uk",
    "hmv.com",
    "jdwilliams.co.uk",
    "johnlewis.com",
    "sportsdirect.com",
    "very.co.uk",
    "waylandgames.co.uk",
    "alchemistsworkshops.com",
    "chaoscards.co.uk",
    "board-game.co.uk",
    "newrealitiesgaming.com",
    "firestormgames.co.uk",
    "tritex-games.co.uk",
}

def should_use_playwright(url: str) -> bool:
    host = _host_for_url(url)
    return host in PLAYWRIGHT_DOMAINS

# === REALISTIC UA + LANGUAGE + REFERER ROTATION ===
REAL_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

LANG_HEADERS = [
    "en-GB,en;q=0.9",
    "en-US,en;q=0.9",
    "en-GB,en-US;q=0.9,en;q=0.8",
]

REFERERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://www.google.co.uk/",
    None,
]

def get_headers_for_url(url):
    ua = random.choice(REAL_UAS)
    lang = random.choice(LANG_HEADERS)
    referer = random.choice(REFERERS)

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": lang,
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    if referer:
        headers["Referer"] = referer

    if USE_MOBILE_HEADERS:
        headers["User-Agent"] = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1"

    return headers

# === REQUEST RETRIES ===
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
SESSION = requests.Session()
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)

# === DATABASE & OTHER GLOBALS ===
DB_POOL = None
MAX_TIMEOUT = 60
IS_PRODUCTION = os.getenv("REPLIT_DEPLOYMENT") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

FRANCHISES = [
    {
        "name": "Pokemon",
        "role_id": os.getenv("POKE_ROLE"),
        "direct_files": [
            "Pokemon/Poke-30A.txt",
            "Pokemon/Poke-AH.txt",
            "Pokemon/Poke-DR.txt",
            "Pokemon/Poke-ME.txt",
            "Pokemon/Poke-PO.txt",
            "Pokemon/Poke-Other.txt"
        ],
        "webhook_secrets": [
            {"file": "Pokemon/Poke-30A.txt", "webhook": os.getenv("POKE30A")},
            {"file": "Pokemon/Poke-AH.txt", "webhook": os.getenv("POKEAH")},
            {"file": "Pokemon/Poke-DR.txt", "webhook": os.getenv("POKEDR")},
            {"file": "Pokemon/Poke-ME.txt", "webhook": os.getenv("POKEME")},
            {"file": "Pokemon/Poke-PO.txt", "webhook": os.getenv("POKEPO")},
            {"file": "Pokemon/Poke-Other.txt", "webhook": os.getenv("POKEOTHER")},
        ],
        "dormant_files": ["Pokemon/Poke-Other.txt"]
    },
    {
        "name": "One Piece",
        "role_id": os.getenv("OP_ROLE"),
        "direct_files": [
            "One Piece/EB-02.txt",
            "One Piece/EB-03.txt",
            "One Piece/IB-V5.txt",
            "One Piece/IB-V6.txt",
            "One Piece/OP-13.txt",
            "One Piece/OP-14.txt",
            "One Piece/OP-Other.txt"
        ],
        "webhook_secrets": [
            {"file": "One Piece/EB-02.txt", "webhook": os.getenv("EB02")},
            {"file": "One Piece/EB-03.txt", "webhook": os.getenv("EB03")},
            {"file": "One Piece/IB-V5.txt", "webhook": os.getenv("IBV5")},
            {"file": "One Piece/IB-V6.txt", "webhook": os.getenv("IBV6")},
            {"file": "One Piece/OP-13.txt", "webhook": os.getenv("OP13")},
            {"file": "One Piece/OP-14.txt", "webhook": os.getenv("OP14")},
            {"file": "One Piece/OP-Other.txt", "webhook": os.getenv("OPOTHER")},
        ]
    }
]

CURRENT_FRANCHISE = None
CURRENT_WEBHOOK = None
CURRENT_ROLE_ID = None

FAILED_SITES = {}
FAILURE_COOLDOWN_MINUTES = 3

LAST_HOURLY_PING = None
LAST_DAILY_PING = None

HOURLY_STATS = {}
DAILY_STATS = {}
TOTAL_SCANS = 0
DAILY_SCANS = 0
HOURLY_FAILED_DETAILS = []
USE_MOBILE_HEADERS = False

JS_SKIP_CACHE = {}
JS_SKIP_MINUTES = 5

VERIFIED_OUT_CACHE = set()

JS_URL_PATTERNS = ['#/', 'dffullscreen', '?view=ajax', 'doofinder']
JS_PAGE_INDICATORS = ['enable javascript', 'javascript is required', 'doofinder',
                      'please enable javascript', 'browser does not support']

BLOCK_KEYWORDS = [
    "plush", "plushie", "figure", "toy",
    "mug", "cup", "glass",
    "poster", "art", "canvas",
    "postcard", "stationery",
    "clothing", "hoodie", "t-shirt", "shirt",
    "bag", "backpack",
    "keyring", "keychain",
    "playmat",
    "dice", "coin",
    "notebook", "binder",
    "squishmallow", "cushion", "pillow",
    "lamp", "light", "clock",
    "wallet", "purse",
    "lunchbox", "water bottle",
    "blanket", "towel",
    "hat", "cap", "beanie",
    "socks", "slippers",
    "puzzle", "jigsaw", "challenge",
    "funko", "pop!", "sleeves", "portfolio", "accessories", "event",
]

def is_tcg_product(name: str, url: str = "") -> bool:
    text = f"{name} {url}".lower()
    return not any(word in text for word in BLOCK_KEYWORDS)

STORE_UNAVAILABLE_MARKERS = [
    "enter using password", "password protected", "store closed",
    "temporarily closed", "temporarily unavailable", "site is unavailable",
    "maintenance mode", "under maintenance", "we'll be back soon",
    "coming back soon", "closed until", "currently unavailable",
    "are you the store owner", "admin login", "restricted access"
]

def is_store_unavailable(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(marker in t for marker in STORE_UNAVAILABLE_MARKERS)

# === MAIN FETCH FUNCTION (with all fixes) ===
def fetch_html_playwright(url: str, timeout_ms: int):
    if not PLAYWRIGHT_AVAILABLE:
        return 0, url, ""

    proxy_cfg = playwright_proxy_for_url(url)

    with sync_playwright() as p:
        launch_kwargs = {
            "headless": True,
            "args": [
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-infobars",
                "--window-size=1920,1080",
                "--disable-features=site-per-process",
                "--disable-web-security",
                "--ignore-certificate-errors",
                "--disable-quic",
            ]
        }
        if proxy_cfg:
            launch_kwargs["proxy"] = proxy_cfg

        context_kwargs = {
            "user_agent": random.choice(REAL_UAS),
            "viewport": {"width": 1920, "height": 1080},
            "java_script_enabled": True,
            "bypass_csp": True,
        }

        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(**context_kwargs)

        page = context.new_page()

        def route_filter(route):
            rt = route.request.resource_type
            if rt in ("image", "media", "font", "stylesheet", "other"):
                return route.abort()
            return route.continue_()

        page.route("**/*", route_filter)

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            status = resp.status if resp else 0
            html = page.content() or ""
            final_url = page.url
        except Exception as e:
            print(f"Playwright error on {url}: {e}")
            status, final_url, html = 0, url, ""

        browser.close()
        return status, final_url, html

# === MAIN LOOP ===
def main():
    global CURRENT_WEBHOOK, CURRENT_ROLE_ID

    print("🚀 Starting Store Monitor Bot...")
    print(f"   Time: {datetime.now(timezone.utc)}")
    print(f"   Production mode: {IS_PRODUCTION}")
    print(f"   Franchises: {', '.join(f['name'] for f in FRANCHISES)}")
    print(f"   Playwright available: {PLAYWRIGHT_AVAILABLE}")
    if PLAYWRIGHT_AVAILABLE:
        print(f"   Playwright domains: {', '.join(sorted(PLAYWRIGHT_DOMAINS))}")

    # ... (keep your existing DB init, sync_urls_to_db, load_ping_state, etc.)

    while True:
        global USE_MOBILE_HEADERS
        USE_MOBILE_HEADERS = not USE_MOBILE_HEADERS
        header_type = "📱 Mobile" if USE_MOBILE_HEADERS else "🖥️ Desktop"

        cycle_start = time.time()
        total_cycle_changes = 0
        total_stats = {'fetched': 0, 'failed': 0}

        # ... (keep your existing franchise loop, file loop, url loop)

        # In the url loop, use the updated fetch:
        headers = get_headers_for_url(url)
        timeout = get_timeout_for_url(url)

        # Fallback for known problem domains
        if any(domain in url for domain in ["johnlewis.com", "very.co.uk", "game.co.uk", "sportsdirect.com"]):
            print(f"[FALLBACK] Using requests + headers for {url}")
            status_code, final_url, html = fetch_html_requests(url, headers, timeout)
        else:
            status_code, final_url, html = fetch_html(url, headers=headers, timeout_s=timeout)

        # ... (rest of your check_direct_product logic remains the same)

        # Add jitter to main sleep
        base_delay = CHECK_INTERVAL
        jitter = random.uniform(-2, 3)
        time.sleep(max(1, base_delay + jitter))

        # ... (keep your ping code, stats, etc.)

if __name__ == "__main__":
    main()
