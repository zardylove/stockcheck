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

# === DATABASE CONNECTION POOL ===
DB_POOL = None
MAX_TIMEOUT = 60
IS_PRODUCTION = os.getenv("REPLIT_DEPLOYMENT") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

# === FRANCHISE CONFIGURATIONS ===
FRANCHISES = [
    {
        "name": "Pokemon",
        "role_id": os.getenv("POKEMON_ROLE"),
        "direct_files": [
            "Pokemon/Poke-30A.txt",
            "Pokemon/Poke-AH.txt",
            "Pokemon/Poke-DR.txt",
            "Pokemon/Poke-ME.txt",
            "Pokemon/Poke-PO.txt"
        ],
        "webhook_secrets": [
            {"file": "Pokemon/Poke-30A.txt", "webhook": os.getenv("POKE30A")},
            {"file": "Pokemon/Poke-AH.txt", "webhook": os.getenv("POKEAH")},
            {"file": "Pokemon/Poke-DR.txt", "webhook": os.getenv("POKEDR")},
            {"file": "Pokemon/Poke-ME.txt", "webhook": os.getenv("POKEME")},
            {"file": "Pokemon/Poke-PO.txt", "webhook": os.getenv("POKEPO")},
        ]
    },
    {
        "name": "One Piece",
        "role_id": os.getenv("ONEPIECE_ROLE"),
        "direct_files": [
            "One Piece/EB-02.txt",
            "One Piece/EB-03.txt",
            "One Piece/IB-V5.txt",
            "One Piece/IB-V6.txt",
            "One Piece/OP-13.txt",
            "One Piece/OP-14.txt"
        ],
        "webhook_secrets": [
            {"file": "One Piece/EB-02.txt", "webhook": os.getenv("EB02")},
            {"file": "One Piece/EB-03.txt", "webhook": os.getenv("EB03")},
            {"file": "One Piece/IB-V5.txt", "webhook": os.getenv("IBV5")},
            {"file": "One Piece/IB-V6.txt", "webhook": os.getenv("IBV6")},
            {"file": "One Piece/OP-13.txt", "webhook": os.getenv("OP13")},
            {"file": "One Piece/OP-14.txt", "webhook": os.getenv("OP14")},
        ]
    }
]

# Current franchise info for alerts
CURRENT_FRANCHISE = None

# Current webhook/role for the active file (set during scanning)
CURRENT_WEBHOOK = None
CURRENT_ROLE_ID = None

# === OPTIMIZATION: Reusable session for connection pooling ===
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
})

# === OPTIMIZATION: Failed site cache (skip for 3 minutes) ===
FAILED_SITES = {}  # {url: failure_time}
FAILURE_COOLDOWN_MINUTES = 3

# === HOURLY/DAILY PING TRACKING ===
LAST_HOURLY_PING = None  # Track last hour we sent a ping
LAST_DAILY_PING = None   # Track last day we sent a ping

# === STATS TRACKING FOR HOURLY/DAILY PINGS ===
HOURLY_STATS = {}  # {file_name: {'fetched': 0, 'failed': 0, 'alerts': 0}}
DAILY_STATS = {}   # {file_name: {'fetched': 0, 'failed': 0, 'alerts': 0}} - accumulates all day
TOTAL_SCANS = 0    # Total scan cycles completed (resets hourly)
DAILY_SCANS = 0    # Total scan cycles completed (resets daily)
HOURLY_FAILED_SITES = {}  # {site_domain: count} - tracks which sites failed this hour

# === OPTIMIZATION: JS/Dynamic page skip cache ===
JS_SKIP_CACHE = {}  # {url: skip_until_time}
JS_SKIP_MINUTES = 5

# === OPTIMIZATION: Verified-out product cache ===
VERIFIED_OUT_CACHE = set()

# URL patterns that indicate JavaScript-only pages
JS_URL_PATTERNS = ['#/', 'dffullscreen', '?view=ajax', 'doofinder']

# HTML content that indicates JS-only rendering
JS_PAGE_INDICATORS = ['enable javascript', 'javascript is required', 'doofinder', 
                      'please enable javascript', 'browser does not support']

# === PRODUCT TYPE FILTERING (block non-TCG items) ===
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
    for word in BLOCK_KEYWORDS:
        if word in text:
            return False
    return True

# === STORE UNAVAILABILITY DETECTION ===
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
    text = text.lower()
    return any(marker in text for marker in STORE_UNAVAILABLE_MARKERS)

# === SHOPIFY ANTIBOT LIST (for fallback only) ===
SHOPIFY_STORES_WITH_ANTIBOT = [
    "zingaentertainment.com", "themeeplerooms.co.uk", "jetcards.uk",
    "rockawaytoys.co.uk", "moon-whale.com", "redcardgames.com",
    "afkgaming.co.uk", "ajtoys.co.uk", "bremnertcg.co.uk",
    "coastiescollectibles.com", "dansolotcg.co.uk", "thedicedungeon.co.uk",
    "dragonvault.gg", "eclipsecards.com", "endocollects.co.uk",
    "gatheringgames.co.uk", "hammerheadtcg.co.uk", "nerdforged.co.uk",
    "peakycollectibles.co.uk", "safarizone.co.uk", "sweetsnthings.co.uk",
    "thistletavern.com", "thirstymeeples.co.uk", "titancards.co.uk",
    "toybarnhaus.co.uk", "travellingman.com", "westendgames.co.uk",
]

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

def normalize_product_url(url):
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        if not path:
            return None

        if '/collections/' in path and '/products/' in path:
            products_idx = path.find('/products/')
            path = path[products_idx:]

        normalized = urlunparse((parsed.scheme, parsed.netloc.lower(), path, '', '', ''))
        return normalized
    except:
        return url

CHECK_INTERVAL = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
}

MOBILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
}

PREORDER_TERMS = [
    "pre-order now", "preorder now", "available for pre-order", "available for preorder",
    "pre order now", "add to pre-order", "expected release", "expected dispatch",
    "releasing soon", "available from", "releases on", "presale", "pre-sale",
    "preorder available", "pre-order available"
]

OUT_OF_STOCK_TERMS = [
    "sold out", "out of stock", "unavailable", "notify when available",
    "currently unavailable", "temporarily out of stock", "not in stock",
    "no stock available", "stock: 0", "notify me when in stock", "out-of-stock",
    "soldout", "backorder", "back order", "waitlist", "wait list",
    "notify me", "email when available", "outofstock", "schema.org/outofstock",
    "check back soon", "sold-out-btn", "notify me when", "register interest",
    "item unavailable", "coming soon", "releases in", "will be in stock on"
]

IN_STOCK_TERMS = [
    "add to cart", "add to basket", "add to bag", "add to trolley",
    "add to order", "in stock", "available now", "available to buy",
    "buy now", "order now", "item in stock", "stock available",
    "stock: available", "instock", "in-stock", "add to shopping bag",
    "add to shopping cart", "purchase now", "get it now",
    "ready to ship", "ships today", "in stock now",
    "only a few left", "low stock", "limited stock", "few remaining"
]

PREORDER_PATTERN = re.compile('|'.join(re.escape(term) for term in PREORDER_TERMS), re.IGNORECASE)
OUT_OF_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in OUT_OF_STOCK_TERMS), re.IGNORECASE)
IN_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in IN_STOCK_TERMS), re.IGNORECASE)

def classify_stock(text):
    text_normalized = text.replace('\xa0', ' ').replace('\u00a0', ' ')
    text_normalized = ' '.join(text_normalized.split())

    text_lower = text_normalized.lower()

    preorder_match = PREORDER_PATTERN.search(text_lower)
    out_match = OUT_OF_STOCK_PATTERN.search(text_lower)
    in_match = IN_STOCK_PATTERN.search(text_lower)

    # OUT takes highest priority - if sold out, nothing else matters
    if out_match:
        return "out"
    if preorder_match:
        return "preorder"
    if in_match:
        match_text = in_match.group()
        match_pos = in_match.end()
        after_match = text_lower[match_pos:match_pos+10] if match_pos < len(text_lower) else ""
        if match_text == "in stock" and ("items" in after_match or "item" in after_match):
            return "out"
        return "in"

    return "out"

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

def is_verified_out(product_url):
    return product_url in VERIFIED_OUT_CACHE

def mark_verified_out(product_url):
    VERIFIED_OUT_CACHE.add(product_url)

def clear_verified_out(product_url):
    VERIFIED_OUT_CACHE.discard(product_url)

def load_urls(file_list):
    all_urls = set()
    for file_path in file_list:
        try:
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and line.startswith("http"):
                        all_urls.add(line.split()[0])
        except FileNotFoundError:
            print(f"⚠️ File not found: {file_path}")
    return list(all_urls)

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

ALERT_COOLDOWN_MINUTES = 0  # No cooldown - alert every change
VERIFY_COOLDOWN_MINUTES = 10
LAST_VERIFIED = {}

def should_verify(product_url):
    last = LAST_VERIFIED.get(product_url)
    return not last or datetime.now() - last >= timedelta(minutes=VERIFY_COOLDOWN_MINUTES)

def mark_verified_now(product_url):
    LAST_VERIFIED[product_url] = datetime.now()

def should_alert(last_alerted):
    return True  # Always alert on change (no cooldown)

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
        "content": f"{emoji} **RESTOCK** - {product_name} @{store_name} {role_mention}",
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

def confirm_product_stock(product_url):
    try:
        headers = get_headers_for_url(product_url)
        timeout = get_timeout_for_url(product_url)
        r = SESSION.get(product_url, headers=headers, timeout=min(timeout, MAX_TIMEOUT))

        if r.status_code != 200:
            return "unknown", None, None, None

        soup = BeautifulSoup(r.text, "html.parser")
        page_text = soup.get_text()
        raw_html = r.text

        product_name = None
        title_tag = soup.find('title')
        if title_tag:
            product_name = title_tag.get_text(strip=True)[:100]
        if not product_name:
            h1 = soup.find('h1')
            if h1:
                product_name = h1.get_text(strip=True)[:100]
        if not product_name:
            product_name = urlparse(product_url).path.split('/')[-1].replace('-', ' ').replace('.html', '')[:100]

        image_url = None
        img_selectors = [
            'img.product-featured-image', 'img.product-image', 'img.product__image',
            'meta[property="og:image"]', 'meta[name="og:image"]'
        ]
        for selector in img_selectors:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == 'meta':
                    image_url = elem.get('content')
                else:
                    image_url = elem.get('src') or elem.get('data-src')
                if image_url:
                    if image_url.startswith('//'):
                        image_url = 'https:' + image_url
                    elif image_url.startswith('/'):
                        parsed = urlparse(product_url)
                        image_url = f"{parsed.scheme}://{parsed.netloc}{image_url}"
                    break

        price = None
        price_selectors = ['.price', '.product-price', 'meta[property="product:price:amount"]']
        for selector in price_selectors:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == 'meta':
                    price = f"£{elem.get('content')}"
                else:
                    price_text = elem.get_text(strip=True)
                    match = re.search(r'£[\d,]+\.?\d*', price_text)
                    if match:
                        price = match.group()
                break

        if is_store_unavailable(page_text):
            return "unknown", product_name, image_url, price

        stock_status = classify_stock(page_text + " " + raw_html)

        return stock_status, product_name, image_url, price

    except Exception as e:
        print(f"❌ confirm_product_stock error: {e}")
        return "unknown", None, None, None

def get_headers_for_url(url):
    mobile_sites = [
        "very.co.uk", "freemans.com", "jdwilliams.co.uk", "jacamo.co.uk",
        "gameon.games", "hillscards.co.uk", "hmv.com", "game.co.uk",
        "johnlewis.com", "hamleys.com",
        "zingaentertainment.com", "themeeplerooms.co.uk", "jetcards.uk",
        "rockawaytoys.co.uk", "moon-whale.com", "redcardgames.com",
        "afkgaming.co.uk", "ajtoys.co.uk", "bremnertcg.co.uk",
        "coastiescollectibles.com", "dansolotcg.co.uk", "thedicedungeon.co.uk",
        "dragonvault.gg", "eclipsecards.com", "endocollects.co.uk",
        "gatheringgames.co.uk", "hammerheadtcg.co.uk", "nerdforged.co.uk",
        "peakycollectibles.co.uk", "safarizone.co.uk", "sweetsnthings.co.uk",
        "thistletavern.com", "thirstymeeples.co.uk", "titancards.co.uk",
        "toybarnhaus.co.uk", "travellingman.com", "westendgames.co.uk"
    ]
    if any(site in url.lower() for site in mobile_sites):
        return MOBILE_HEADERS
    return HEADERS

def get_timeout_for_url(url):
    slow_sites = ["very.co.uk", "game.co.uk", "johnlewis.com", "argos.co.uk"]
    if any(site in url for site in slow_sites):
        return 30
    return 15

def check_direct_product(url, previous_state, stats, store_file=None, is_verification=False):
    if is_site_in_failure_cooldown(url):
        print(f"⏭️ SKIPPED (failed recently)")
        if not is_verification:
            stats['skipped'] += 1
        return previous_state, None

    try:
        headers = get_headers_for_url(url)
        timeout = get_timeout_for_url(url)
        r = SESSION.get(url, headers=headers, timeout=min(timeout, MAX_TIMEOUT))

        if r.status_code != 200:
            domain = urlparse(url).netloc
            print(f"⚠️ Failed ({r.status_code})")
            mark_site_failed(url)
            if not is_verification:
                stats['failed'] += 1
                HOURLY_FAILED_SITES[domain] = HOURLY_FAILED_SITES.get(domain, 0) + 1
            return previous_state, None
        
        if not is_verification:
            stats['fetched'] += 1

        soup = BeautifulSoup(r.text, "html.parser")
        page_text = soup.get_text()
        raw_html = r.text

        product_name = None
        title_tag = soup.find('title')
        if title_tag:
            product_name = title_tag.get_text(strip=True)[:100]
        if not product_name:
            h1 = soup.find('h1')
            if h1:
                product_name = h1.get_text(strip=True)[:100]
        if not product_name:
            product_name = urlparse(url).path.split('/')[-1].replace('-', ' ').replace('.html', '')[:100]

        stock_status = classify_stock(page_text + " " + raw_html)
        is_available = stock_status in ("in", "preorder")

        current_state = {
            "name": product_name,
            "in_stock": is_available,
            "stock_status": stock_status,
            "last_alerted": previous_state.get("last_alerted") if previous_state else None
        }

        change = None
        if previous_state:
            was_available = previous_state.get("in_stock", False)
            if not was_available and is_available:
                last_alerted = previous_state.get("last_alerted")
                if should_alert(last_alerted):
                    change = {
                        "type": "preorder" if stock_status == "preorder" else "restock",
                        "name": product_name,
                        "url": url,
                        "store_file": store_file
                    }

        return current_state, change

    except requests.exceptions.Timeout:
        domain = urlparse(url).netloc
        print(f"⏱️ TIMEOUT")
        mark_site_failed(url)
        if not is_verification:
            stats['failed'] += 1
            HOURLY_FAILED_SITES[domain] = HOURLY_FAILED_SITES.get(domain, 0) + 1
        return previous_state, None
    except Exception as e:
        domain = urlparse(url).netloc
        print(f"❌ Error checking direct product {url}: {e}")
        mark_site_failed(url)
        if not is_verification:
            stats['failed'] += 1
            HOURLY_FAILED_SITES[domain] = HOURLY_FAILED_SITES.get(domain, 0) + 1
        return previous_state, None

def main():
    global CURRENT_WEBHOOK, CURRENT_ROLE_ID

    print("🚀 Starting Store Monitor Bot...")
    print(f"   Time: {datetime.now(timezone.utc)}")
    print(f"   Production mode: {IS_PRODUCTION}")
    print(f"   Franchises: {', '.join(f['name'] for f in FRANCHISES)}")

    if not init_db_pool():
        print("❌ Database pool failed. Exiting.")
        return
    if not init_database():
        print("❌ Database init failed. Exiting.")
        return
    load_ping_state()

    direct_state = load_direct_state()
    first_run = len(direct_state) == 0

    if first_run:
        print("🆕 First run - building initial database (no alerts)...")

    for franchise in FRANCHISES:
        direct_files = franchise.get("direct_files", [])
        webhook_groups = len(franchise.get("webhook_secrets", []))
        print(f"   {franchise['name']}: {len(direct_files)} direct files, {webhook_groups} webhook groups")

    while True:
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

            # Scan each file separately for clearer logging
            for file_path in direct_files:
                file_name = file_path.split('/')[-1].replace('.txt', '')
                file_urls = load_urls([file_path])
                
                if not file_urls:
                    continue
                
                # Initialize per-file stats tracking
                global HOURLY_STATS, DAILY_STATS
                if file_name not in HOURLY_STATS:
                    HOURLY_STATS[file_name] = {'fetched': 0, 'failed': 0, 'alerts': 0, 'products': len(file_urls)}
                else:
                    HOURLY_STATS[file_name]['products'] = len(file_urls)
                if file_name not in DAILY_STATS:
                    DAILY_STATS[file_name] = {'fetched': 0, 'failed': 0, 'alerts': 0, 'products': len(file_urls)}
                else:
                    DAILY_STATS[file_name]['products'] = len(file_urls)
                
                file_stats = {'fetched': 0, 'failed': 0, 'alerts': 0, 'skipped': 0}
                
                print(f"\n📁 {file_name} ({len(file_urls)} products)")
                
                for url in file_urls:
                    store_name = urlparse(url).netloc
                    print(f"  Checking: {store_name}...", end=" ")

                    prev_state = direct_state.get(url)
                    current_state, change = check_direct_product(url, prev_state, file_stats, store_file=file_path)

                    if current_state:
                        direct_state[url] = current_state
                        detailed_status = current_state.get("stock_status", "unknown").upper()

                        if change and not first_run:
                            print(f"🔍 Potential {change.get('type', 'change')} - verifying...", end=" ")
                            time.sleep(5)
                            verified_state, _ = check_direct_product(url, direct_state.get(url), file_stats, store_file=file_path, is_verification=True)
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
                                        send_alert(change['name'], change["url"], store_name,
                                                  is_preorder=is_preorder, is_new=False,
                                                  store_file=file_path)
                                        direct_state[url]["last_alerted"] = datetime.now(timezone.utc)
                                        save_product(url, current_state["name"], True)
                                        mark_alerted(url)
                                else:
                                    print(f"❌ Verification failed ({verified_status.upper()})")
                                    save_product(url, current_state["name"], False)
                            else:
                                print(f"❌ Verification failed (no response)")
                        else:
                            print(f"{detailed_status}")

                        prev_in_stock = prev_state.get("in_stock") if prev_state else None
                        if current_state["in_stock"] != prev_in_stock:
                            save_product(url, current_state["name"], current_state["in_stock"])

                    time.sleep(1)

                # Accumulate file stats into HOURLY_STATS and DAILY_STATS
                HOURLY_STATS[file_name]['fetched'] += file_stats['fetched']
                HOURLY_STATS[file_name]['failed'] += file_stats['failed']
                HOURLY_STATS[file_name]['alerts'] += file_stats['alerts']
                DAILY_STATS[file_name]['fetched'] += file_stats['fetched']
                DAILY_STATS[file_name]['failed'] += file_stats['failed']
                DAILY_STATS[file_name]['alerts'] += file_stats['alerts']
                
                # Also accumulate into franchise stats
                stats['fetched'] += file_stats['fetched']
                stats['failed'] += file_stats['failed']

            print(f"\n📊 {franchise_name}: {franchise_changes} alerts sent")
            print(f"   Stats: {stats['fetched']} fetched, {stats['failed']} failed")

            total_cycle_changes += franchise_changes
            total_stats['fetched'] += stats['fetched']
            total_stats['failed'] += stats['failed']

        cycle_time = round(time.time() - cycle_start, 1)
        
        # Increment total scan count
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

        print(f"📈 Total: {total_stats['fetched']} fetched, {total_stats['failed']} failed | Cycle: {cycle_time}s")

        # === HOURLY STATUS PING (exactly on the hour in London/UK time) ===
        global LAST_HOURLY_PING, LAST_DAILY_PING
        now_utc = datetime.now(timezone.utc)
        now_london = now_utc.astimezone(ZoneInfo("Europe/London"))
        current_hour = now_london.strftime("%Y-%m-%d %H")
        current_day = now_london.strftime("%Y-%m-%d")

        if HOURLY_WEBHOOK := os.getenv("HOURLYDATA"):
            # Only ping if we're in the first 15 minutes of the hour AND haven't pinged this hour
            if now_london.minute < 15 and LAST_HOURLY_PING != current_hour:
                prev_hour = now_london - timedelta(hours=1)
                time_range = f"{prev_hour.strftime('%H:%M')} - {now_london.strftime('%H:%M')}"
                
                # Build file breakdown
                file_breakdown = ""
                total_hourly_alerts = 0
                total_hourly_fetched = 0
                total_hourly_failed = 0
                for file_name, stats in sorted(HOURLY_STATS.items()):
                    file_breakdown += f"  • **{file_name}**: {stats['products']} products, {stats['fetched']} fetched, {stats['failed']} failed, {stats['alerts']} alerts\n"
                    total_hourly_alerts += stats['alerts']
                    total_hourly_fetched += stats['fetched']
                    total_hourly_failed += stats['failed']
                
                # Build failed sites breakdown
                failed_sites_text = ""
                if HOURLY_FAILED_SITES:
                    sorted_fails = sorted(HOURLY_FAILED_SITES.items(), key=lambda x: x[1], reverse=True)[:10]
                    failed_sites_text = "\n**Failed Sites (top 10)**\n"
                    for domain, count in sorted_fails:
                        failed_sites_text += f"  • {domain}: {count} fails\n"
                
                hourly_summary = (
                    f"🟢 **Hourly Bot Status** ({now_london.strftime('%d %B %Y %H:00 UK time')})\n"
                    f"**Period covered: {time_range}**\n\n"
                    f"**Overall Stats**\n"
                    f"• **Products tracked**: {len(direct_state)}\n"
                    f"• **Full cycle scans completed**: {TOTAL_SCANS}\n"
                    f"• **Total fetched**: {total_hourly_fetched}\n"
                    f"• **Total failed**: {total_hourly_failed}\n"
                    f"• **Alerts sent**: {total_hourly_alerts}\n\n"
                    f"**Per-File Breakdown**\n{file_breakdown}"
                    f"{failed_sites_text}\n"
                    f"• **Bot status**: ✅ Active"
                )
                try:
                    requests.post(HOURLY_WEBHOOK, json={"content": hourly_summary}, timeout=10)
                    print("📤 Sent hourly status ping")
                    LAST_HOURLY_PING = current_hour
                    save_ping_state("hourly", current_hour)
                    # Reset hourly stats after sending
                    HOURLY_STATS = {k: {'fetched': 0, 'failed': 0, 'alerts': 0, 'products': v['products']} for k, v in HOURLY_STATS.items()}
                    HOURLY_FAILED_SITES.clear()
                    TOTAL_SCANS = 0
                except Exception as e:
                    print(f"⚠️ Hourly ping failed: {e}")

        # === DAILY STATUS PING (at 8:00 AM UK time the following day) ===
        if DAILY_WEBHOOK := os.getenv("DAILYDATA"):
            # Trigger at 8:00–8:15 AM UK time AND haven't pinged today
            if now_london.hour == 8 and now_london.minute < 15 and LAST_DAILY_PING != current_day:
                yesterday = (now_london - timedelta(days=1)).strftime("%d %B %Y")
                
                # Build file breakdown for daily using DAILY_STATS (full day accumulation)
                daily_file_breakdown = ""
                daily_total_alerts = 0
                daily_total_fetched = 0
                daily_total_failed = 0
                for file_name, stats in sorted(DAILY_STATS.items()):
                    daily_file_breakdown += f"  • **{file_name}**: {stats['products']} products, {stats['fetched']} fetched, {stats['failed']} failed, {stats['alerts']} alerts\n"
                    daily_total_alerts += stats['alerts']
                    daily_total_fetched += stats['fetched']
                    daily_total_failed += stats['failed']
                
                daily_summary = (
                    f"📅 **Daily Bot Report – {yesterday}**\n\n"
                    f"**Overall Summary**\n"
                    f"• **Total products tracked**: {len(direct_state)}\n"
                    f"• **Full cycle scans completed**: {DAILY_SCANS}\n"
                    f"• **Total fetched**: {daily_total_fetched}\n"
                    f"• **Total failed**: {daily_total_failed}\n"
                    f"• **Total alerts sent**: {daily_total_alerts}\n\n"
                    f"**Per-File Breakdown**\n{daily_file_breakdown}\n"
                    f"• **Bot status**: ✅ Active\n"
                    f"• **Last full cycle**: {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
                )
                try:
                    requests.post(DAILY_WEBHOOK, json={"content": daily_summary}, timeout=10)
                    print("📤 Sent daily status ping (8 AM UK time)")
                    LAST_DAILY_PING = current_day
                    save_ping_state("daily", current_day)
                    # Reset daily stats after sending
                    DAILY_STATS = {k: {'fetched': 0, 'failed': 0, 'alerts': 0, 'products': v['products']} for k, v in DAILY_STATS.items()}
                    DAILY_SCANS = 0
                except Exception as e:
                    print(f"⚠️ Daily ping failed: {e}")

        print(f"⏱️ Next scan in {CHECK_INTERVAL} seconds...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()