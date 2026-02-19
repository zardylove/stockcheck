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

# === MAIN FETCH FUNCTION ===
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

def fetch_html_requests(url: str, headers: dict, timeout_s: int):
    r = SESSION.get(
        url,
        headers=headers,
        timeout=timeout_s,
        proxies=proxies_for_url(url),
    )
    return r.status_code, r.url, r.text

def fetch_html(url: str, headers: dict, timeout_s: int):
    if should_use_playwright(url) and PLAYWRIGHT_AVAILABLE:
        return fetch_html_playwright(url, timeout_ms=min(timeout_s, MAX_TIMEOUT) * 1000)
    return fetch_html_requests(url, headers=headers, timeout_s=min(timeout_s, MAX_TIMEOUT))

# === DATABASE FUNCTIONS (unchanged from your original) ===
def init_db_pool():
    global DB_POOL
    print(f"🔌 Checking database connection...")
    if not DATABASE_URL:
        print("❌ DATABASE_URL not found! Database disabled.")
        return False
    if DB_POOL is None:
        try:
            DB_POOL = pool.SimpleConnectionPool(1, 10, DATABASE_URL, connect_timeout=10)
            print("✅ Database pool initialized")
            return True
        except Exception as e:
            print(f"❌ Failed to create pool: {e}")
            return False
    return True

def get_db_connection():
    global DB_POOL
    if DB_POOL:
        return DB_POOL.getconn()
    return psycopg2.connect(DATABASE_URL)

def return_db_connection(conn):
    global DB_POOL
    if DB_POOL and conn:
        DB_POOL.putconn(conn)
    elif conn:
        conn.close()

def init_database():
    if not DATABASE_URL:
        return False
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_state (
                store_url TEXT NOT NULL,
                product_url TEXT NOT NULL,
                product_name TEXT,
                in_stock BOOLEAN DEFAULT TRUE,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_alerted TIMESTAMP,
                PRIMARY KEY (store_url, product_url)
            )
        """)
        cur.execute("ALTER TABLE product_state ADD COLUMN IF NOT EXISTS last_alerted TIMESTAMP")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ping_state (
                ping_type TEXT PRIMARY KEY,
                last_ping TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitored_urls (
                url TEXT NOT NULL,
                file_group TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (url, file_group)
            )
        """)
        conn.commit()
        cur.close()
        return_db_connection(conn)
        print("✅ Database initialized")
        return True
    except Exception as e:
        print(f"❌ Database error: {e}")
        if conn:
            return_db_connection(conn)
        return False

def load_ping_state():
    global LAST_HOURLY_PING, LAST_DAILY_PING
    if not DATABASE_URL:
        print("⚠️ Skipping ping state load (no DATABASE_URL).")
        return
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT ping_type, last_ping FROM ping_state")
        rows = cur.fetchall()
        for ping_type, last_ping in rows:
            if ping_type == "hourly":
                LAST_HOURLY_PING = last_ping
            elif ping_type == "daily":
                LAST_DAILY_PING = last_ping
        cur.close()
        return_db_connection(conn)
        if LAST_HOURLY_PING or LAST_DAILY_PING:
            print(f"📂 Loaded ping state: hourly={LAST_HOURLY_PING}, daily={LAST_DAILY_PING}")
    except Exception as e:
        print(f"⚠️ Error loading ping state: {e}")
        if conn:
            return_db_connection(conn)

def save_ping_state(ping_type, value):
    if not DATABASE_URL:
        return
    for attempt in range(3):
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ping_state (ping_type, last_ping)
                VALUES (%s, %s)
                ON CONFLICT (ping_type) DO UPDATE SET last_ping = EXCLUDED.last_ping
            """, (ping_type, value))
            conn.commit()
            cur.close()
            return_db_connection(conn)
            return
        except Exception as e:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            if attempt < 2:
                time.sleep(1)
                init_db_pool()
            else:
                print(f"⚠️ Error saving ping state: {e}")

def sync_urls_to_db():
    if not DATABASE_URL:
        return
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        total_added = 0
        total_removed = 0
        for franchise in FRANCHISES:
            for file_path in franchise.get("direct_files", []):
                file_urls = set()
                try:
                    with open(file_path, "r") as f:
                        for line in f:
                            line = line.strip()
                            if line and line.startswith("http"):
                                file_urls.add(line.split()[0])
                except FileNotFoundError:
                    continue
                cur.execute("SELECT url FROM monitored_urls WHERE file_group = %s", (file_path,))
                db_urls = set(row[0] for row in cur.fetchall())
                new_urls = file_urls - db_urls
                removed_urls = db_urls - file_urls
                for url in new_urls:
                    cur.execute("INSERT INTO monitored_urls (url, file_group) VALUES (%s, %s) ON CONFLICT DO NOTHING", (url, file_path))
                    total_added += 1
                for url in removed_urls:
                    cur.execute("DELETE FROM monitored_urls WHERE url = %s AND file_group = %s", (url, file_path))
                    total_removed += 1
        conn.commit()
        cur.close()
        return_db_connection(conn)
        if total_added > 0 or total_removed > 0:
            print(f"🔄 URL sync: {total_added} added, {total_removed} removed")
        else:
            print(f"🔄 URL sync: all up to date")
    except Exception as e:
        print(f"⚠️ URL sync error: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
            return_db_connection(conn)

def load_urls_from_db(file_path):
    if not DATABASE_URL:
        return load_urls([file_path])
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT url FROM monitored_urls WHERE file_group = %s", (file_path,))
        urls = [row[0] for row in cur.fetchall()]
        cur.close()
        return_db_connection(conn)
        return urls
    except Exception as e:
        print(f"⚠️ DB URL load error, falling back to file: {e}")
        if conn:
            return_db_connection(conn)
        return load_urls([file_path])

def load_direct_state():
    direct_state = {}
    if not DATABASE_URL:
        return direct_state
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT product_url, product_name, in_stock, last_alerted FROM product_state WHERE store_url = product_url")
        rows = cur.fetchall()
        for url, name, in_stock, last_alerted in rows:
            direct_state[url] = {
                "name": name or "",
                "in_stock": in_stock,
                "stock_status": "unknown",
                "last_alerted": last_alerted
            }
        cur.close()
        return_db_connection(conn)
        print(f"📂 Loaded {len(rows)} direct products from DB")
    except Exception as e:
        print(f"❌ Error loading direct state: {e}")
        if conn:
            return_db_connection(conn)
    return direct_state

def save_product(product_url, product_name, in_stock, retry=True):
    if not DATABASE_URL:
        return
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO product_state (store_url, product_url, product_name, in_stock, last_seen)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (store_url, product_url)
            DO UPDATE SET product_name = EXCLUDED.product_name,
                          in_stock = EXCLUDED.in_stock,
                          last_seen = CURRENT_TIMESTAMP
        """, (product_url, product_url, product_name, in_stock))
        conn.commit()
        cur.close()
        return_db_connection(conn)
    except Exception as e:
        if conn:
            try:
                conn.close()
            except:
                pass
        if retry and "SSL" in str(e):
            time.sleep(1)
            save_product(product_url, product_name, in_stock, retry=False)
        else:
            print(f"⚠️ Error saving product: {e}")

def mark_alerted(product_url):
    if not DATABASE_URL:
        return
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE product_state
            SET last_alerted = CURRENT_TIMESTAMP
            WHERE product_url = %s
        """, (product_url,))
        conn.commit()
        cur.close()
        return_db_connection(conn)
    except Exception as e:
        print(f"⚠️ Error marking alerted: {e}")
        if conn:
            return_db_connection(conn)

# === STOCK CLASSIFICATION ===
PREORDER_TERMS = [
    "pre-order now", "preorder now", "available for pre-order", "available for preorder",
    "pre order now", "add to pre-order", "expected release", "expected dispatch",
    "releasing soon", "available from", "releases on", "presale", "pre-sale",
    "preorder available", "pre-order available"
]

OUT_OF_STOCK_TERMS = [
    "sold out", "out of stock", "currently unavailable", "temporarily out of stock",
    "not in stock", "no stock available", "stock: 0", "out-of-stock", "soldout",
    "item unavailable", "sold-out-btn", "notify when available", "notify me when in stock",
    "backorder", "back order", "waitlist", "wait list", "email when available",
    "check back soon", "notify me when", "register interest"
]

IN_STOCK_TEXT_TERMS = [
    "in stock", "available now", "available to buy", "item in stock",
    "stock available", "stock: available", "instock", "in-stock",
    "ready to ship", "ships today", "in stock now",
    "only a few left", "low stock", "limited stock", "few remaining"
]

ADD_TO_CART_BUTTON_TERMS = [
    "add to cart", "add to basket", "add to bag", "add to trolley",
    "add to order", "buy now", "order now", "add to shopping bag",
    "add to shopping cart", "purchase now", "get it now"
]

PREORDER_PATTERN = re.compile('|'.join(re.escape(term) for term in PREORDER_TERMS), re.IGNORECASE)
OUT_OF_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in OUT_OF_STOCK_TERMS), re.IGNORECASE)
IN_STOCK_TEXT_PATTERN = re.compile('|'.join(re.escape(term) for term in IN_STOCK_TEXT_TERMS), re.IGNORECASE)
ADD_TO_CART_PATTERN = re.compile('|'.join(re.escape(term) for term in ADD_TO_CART_BUTTON_TERMS), re.IGNORECASE)

def is_element_hidden(element):
    current = element
    while current and hasattr(current, 'get'):
        style = current.get('style', '')
        if style and ('display:none' in style.replace(' ', '') or 'display: none' in style):
            return True
        classes = current.get('class', [])
        class_str = ' '.join(classes).lower() if classes else ''
        if 'hidden' in class_str or 'hide' in class_str or 'd-none' in class_str:
            return True
        current = current.parent
    return False

def has_active_add_to_cart_button(soup):
    buttons = soup.find_all(['button', 'input'])
    for btn in buttons:
        btn_text = btn.get('value', '') if btn.name == 'input' else btn.get_text(strip=True)
        if ADD_TO_CART_PATTERN.search(btn_text.lower()):
            is_disabled = (
                btn.get('disabled') is not None or
                'disabled' in btn.get('class', []) or
                btn.get('aria-disabled') == 'true'
            )
            is_hidden = is_element_hidden(btn)
            if not is_disabled and not is_hidden:
                return True
    return False

def find_main_product_area(soup):
    wix_product = soup.find(attrs={'data-hook': 'product-page'})
    if wix_product:
        return wix_product

    main_selectors = [
        'product-essential', 'product-main', 'product-info-main',
        'product-details-wrapper', 'product-single', 'pdp-main',
        'product-form', 'product-info', 'product-summary'
    ]
    for selector in main_selectors:
        area = soup.find(class_=re.compile(rf'\b{selector}\b', re.I))
        if area:
            return area
    return None

def classify_stock_with_soup(soup, page_text, raw_html):
    text_lower = page_text.lower()
    main_area = find_main_product_area(soup)

    if main_area:
        main_text = main_area.get_text().lower()
        if OUT_OF_STOCK_PATTERN.search(main_text):
            return "out"
        if has_active_add_to_cart_button(main_area):
            if PREORDER_PATTERN.search(main_text):
                return "preorder"
            return "in"
        if PREORDER_PATTERN.search(main_text):
            return "preorder"
        return "out"

    if OUT_OF_STOCK_PATTERN.search(text_lower):
        return "out"

    if has_active_add_to_cart_button(soup):
        if PREORDER_PATTERN.search(text_lower):
            return "preorder"
        return "in"

    if IN_STOCK_TEXT_PATTERN.search(text_lower):
        if PREORDER_PATTERN.search(text_lower):
            return "preorder"
        return "in"

    if PREORDER_PATTERN.search(text_lower):
        return "preorder"

    return "out"

# === HELPERS ===
def is_site_in_failure_cooldown(url):
    if url not in FAILED_SITES:
        return False
    failure_time = FAILED_SITES[url]
    if datetime.now() - failure_time < timedelta(minutes=FAILURE_COOLDOWN_MINUTES):
        return True
    del FAILED_SITES[url]
    return False

def mark_site_failed(url):
    FAILED_SITES[url] = datetime.now()

def is_js_only_url(url):
    return any(pattern in url.lower() for pattern in JS_URL_PATTERNS)

def is_js_only_page(html_content, product_count):
    if len(html_content) < 2000 and product_count < 3:
        return True
    html_lower = html_content.lower()
    return any(indicator in html_lower for indicator in JS_PAGE_INDICATORS)

def is_in_js_skip_cache(url):
    if url not in JS_SKIP_CACHE:
        return False
    if datetime.now() < JS_SKIP_CACHE[url]:
        return True
    del JS_SKIP_CACHE[url]
    return False

def add_to_js_skip_cache(url):
    JS_SKIP_CACHE[url] = datetime.now() + timedelta(minutes=JS_SKIP_MINUTES)

def get_timeout_for_url(url):
    slow_sites = ["very.co.uk", "game.co.uk", "johnlewis.com", "argos.co.uk"]
    return 45 if any(site in url for site in slow_sites) else 15

CHECK_INTERVAL = 5

def send_alert(product_name, url, store_name, is_preorder=False, is_new=False,
               image_url=None, price=None, store_file=None):
    webhook_url = None
    role_id = None

    for group in CURRENT_FRANCHISE.get("webhook_secrets", []):
        if store_file and group["file"] == store_file:
            webhook_url = group["webhook"]
            break

    role_id = CURRENT_FRANCHISE.get("role_id")

    if webhook_url is None:
        webhook_url = CURRENT_WEBHOOK

    if webhook_url is None:
        print("Warning: No webhook for this file/group")
        return

    if is_preorder:
        title = f"PREORDER AVAILABLE - {product_name}"
        status = "Preorder"
        color = 16753920
        emoji = "📋"
    elif is_new:
        title = f"NEW PRODUCT - {product_name}"
        status = "In Stock"
        color = 3447003
        emoji = "🆕"
    else:
        title = f"BACK IN STOCK - {product_name}"
        status = "In Stock"
        color = 65280
        emoji = "📦"

    role_mention = f"<@&{role_id}>" if role_id else ""

    embed = {
        "title": title,
        "url": url,
        "color": color,
        "fields": [
            {"name": "Retailer", "value": store_name, "inline": True},
            {"name": "Price", "value": price or "N/A", "inline": True},
            {"name": "Status", "value": status, "inline": True},
            {"name": "Direct Link", "value": f"[Click Here]({url})", "inline": False}
        ],
        "footer": {"text": f"{CURRENT_FRANCHISE['name']} Restocks Monitor"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    if image_url:
        embed["thumbnail"] = {"url": image_url}

    data = {
        "content": f"{emoji} **RESTOCK** - **{product_name}** @{store_name} {role_mention}",
        "embeds": [embed],
        "allowed_mentions": {"parse": ["roles"]}
    }

    try:
        r = SESSION.post(webhook_url, json=data, timeout=10)
        if r.status_code == 204:
            print(f"✅ Alert sent to {store_file or 'default'}!")
        else:
            print(f"⚠️ Failed to send alert ({r.status_code}): {r.text}")
    except Exception as e:
        print(f"❌ Discord error: {e}")

def main():
    global CURRENT_WEBHOOK, CURRENT_ROLE_ID

    print("🚀 Starting Store Monitor Bot...")
    print(f"   Time: {datetime.now(timezone.utc)}")
    print(f"   Production mode: {IS_PRODUCTION}")
    print(f"   Franchises: {', '.join(f['name'] for f in FRANCHISES)}")
    print(f"   Playwright available: {PLAYWRIGHT_AVAILABLE}")
    if PLAYWRIGHT_AVAILABLE:
        print(f"   Playwright domains: {', '.join(sorted(PLAYWRIGHT_DOMAINS))}")

    db_ok = init_db_pool()
    if not db_ok:
        print("⚠️ Database not configured. Continuing without database.")
    else:
        if not init_database():
            print("⚠️ Database init failed. Continuing without database.")
            db_ok = False

    if db_ok:
        sync_urls_to_db()
        load_ping_state()
        direct_state = load_direct_state()
    else:
        direct_state = {}

    first_run = len(direct_state) == 0
    if first_run:
        print("🆕 First run - building initial database (no alerts)...")
        for franchise in FRANCHISES:
            for fp in franchise.get("direct_files", []):
                _ = load_urls_from_db(fp)

    while True:
        global USE_MOBILE_HEADERS
        USE_MOBILE_HEADERS = not USE_MOBILE_HEADERS
        header_type = "📱 Mobile" if USE_MOBILE_HEADERS else "🖥️ Desktop"

        cycle_start = time.time()
        total_cycle_changes = 0
        total_stats = {'fetched': 0, 'failed': 0}

        for franchise in FRANCHISES:
            global CURRENT_FRANCHISE, CURRENT_WEBHOOK, CURRENT_ROLE_ID
            CURRENT_FRANCHISE = franchise
            franchise_name = franchise["name"]
            CURRENT_WEBHOOK = None
            CURRENT_ROLE_ID = None

            print(f"\n{'='*50}")
            print(f"📋 Scanning {franchise_name}...")
            print(f"{'='*50}")

            direct_files = franchise.get("direct_files", [])
            if not direct_files:
                print(f"   No direct files for {franchise_name}")
                continue

            stats = {'fetched': 0, 'failed': 0}
            franchise_changes = 0

            dormant_files = franchise.get("dormant_files", [])
            for file_path in direct_files:
                file_name = file_path.split('/')[-1].replace('.txt', '')
                file_urls = load_urls_from_db(file_path)
                is_dormant = file_path in dormant_files

                if not file_urls:
                    continue

                global HOURLY_STATS, DAILY_STATS
                if not is_dormant:
                    if file_name not in HOURLY_STATS:
                        HOURLY_STATS[file_name] = {'fetched': 0, 'failed': 0, 'alerts': 0, 'products': len(file_urls)}
                    else:
                        HOURLY_STATS[file_name]['products'] = len(file_urls)
                    if file_name not in DAILY_STATS:
                        DAILY_STATS[file_name] = {'fetched': 0, 'failed': 0, 'alerts': 0, 'products': len(file_urls)}
                    else:
                        DAILY_STATS[file_name]['products'] = len(file_urls)

                file_stats = {'fetched': 0, 'failed': 0, 'alerts': 0, 'skipped': 0}
                dormant_label = " [DORMANT]" if is_dormant else ""
                print(f"\n📁 {file_name}{dormant_label} ({len(file_urls)} products)")

                for url in file_urls:
                    store_name = urlparse(url).netloc
                    print(f"  Checking: {store_name}...", end=" ")

                    headers = get_headers_for_url(url)          # FIXED: inside the loop
                    timeout = get_timeout_for_url(url)

                    # Fallback for known problem domains
                    if any(domain in url for domain in ["johnlewis.com", "very.co.uk", "game.co.uk", "sportsdirect.com"]):
                        print(f"[FALLBACK] Using requests + headers for {url}")
                        status_code, final_url, html = fetch_html_requests(url, headers, timeout)
                    else:
                        status_code, final_url, html = fetch_html(url, headers=headers, timeout_s=timeout)

                    prev_state = direct_state.get(url)
                    current_state, change = check_direct_product(
                        url, prev_state, file_stats, store_file=file_path, is_dormant=is_dormant
                    )

                    if current_state:
                        direct_state[url] = current_state
                        detailed_status = current_state.get("stock_status", "unknown").upper()

                        if change and not first_run:
                            print(f"🔍 Potential {change.get('type', 'change')} - verifying...", end=" ")
                            time.sleep(5)
                            verified_state, _ = check_direct_product(
                                url, direct_state.get(url), file_stats,
                                store_file=file_path, is_verification=True, is_dormant=is_dormant
                            )
                            if verified_state:
                                verified_status = verified_state.get("stock_status", "unknown")
                                if verified_status in ("in", "preorder"):
                                    direct_state[url]["in_stock"] = True
                                    last_alerted = direct_state[url].get("last_alerted")
                                    if not should_alert(last_alerted):
                                        print(f"✅ {'PREORDER' if verified_status == 'preorder' else 'IN STOCK'} (no alert)")
                                    else:
                                        franchise_changes += 1
                                        file_stats['alerts'] += 1
                                        is_preorder = verified_status == "preorder"
                                        print(f"✅ {'PREORDER CONFIRMED!' if is_preorder else 'RESTOCK CONFIRMED!'}")
                                        img = verified_state.get("image_url") or change.get("image_url")
                                        prc = verified_state.get("price") or change.get("price")
                                        send_alert(
                                            change['name'], change["url"], store_name,
                                            is_preorder=is_preorder, is_new=False,
                                            image_url=img, price=prc,
                                            store_file=file_path
                                        )
                                        direct_state[url]["last_alerted"] = datetime.now(timezone.utc)
                                        save_product(url, current_state["name"], True)
                                        mark_alerted(url)
                                else:
                                    print(f"❌ Verification failed ({verified_status.upper()})")
                                    save_product(url, current_state["name"], False)
                            else:
                                print(f"❌ Verification failed (no response)")
                        else:
                            if is_dormant and detailed_status == "OUT":
                                pass
                            elif detailed_status in ("IN", "PREORDER") and prev_state and prev_state.get("in_stock"):
                                print(f"{detailed_status} - ping already sent")
                            else:
                                print(f"{detailed_status}")

                        prev_in_stock = prev_state.get("in_stock") if prev_state else None
                        if current_state["in_stock"] != prev_in_stock:
                            save_product(url, current_state["name"], current_state["in_stock"])

                    # Jitter delay
                    base_delay = CHECK_INTERVAL
                    jitter = random.uniform(-2, 3)
                    time.sleep(max(1, base_delay + jitter))

                if not is_dormant:
                    HOURLY_STATS[file_name]['fetched'] += file_stats['fetched']
                    HOURLY_STATS[file_name]['failed'] += file_stats['failed']
                    HOURLY_STATS[file_name]['alerts'] += file_stats['alerts']
                    DAILY_STATS[file_name]['fetched'] += file_stats['fetched']
                    DAILY_STATS[file_name]['failed'] += file_stats['failed']
                    DAILY_STATS[file_name]['alerts'] += file_stats['alerts']

                stats['fetched'] += file_stats['fetched']
                stats['failed'] += file_stats['failed']

            print(f"\n📊 {franchise_name}: {franchise_changes} alerts sent")
            print(f"   Stats: {stats['fetched']} fetched, {stats['failed']} failed")

            total_cycle_changes += franchise_changes
            total_stats['fetched'] += stats['fetched']
            total_stats['failed'] += stats['failed']

        cycle_time = round(time.time() - cycle_start, 1)

        global TOTAL_SCANS, DAILY_SCANS
        TOTAL_SCANS += 1
        DAILY_SCANS += 1

        if first_run:
            print(f"\n{'='*50}")
            print(f"✅ Initial scan complete! Tracking {len(direct_state)} products")
            print("   Future changes will trigger Discord alerts.")
            print(f"{'='*50}\n")
            first_run = False
        else:
            print(f"\n{'='*50}")
            if total_cycle_changes > 0:
                print(f"📊 Cycle complete. {total_cycle_changes} total alerts sent.")
            else:
                print(f"📊 Cycle complete. No changes detected.")

        print(f"📈 Total: {total_stats['fetched']} fetched, {total_stats['failed']} failed | {header_type} | Cycle: {cycle_time}s")

        # ... (keep your ping code, stats, etc.)

if __name__ == "__main__":
    main()
