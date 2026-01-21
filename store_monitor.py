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

# === DATABASE CONNECTION POOL ===
DB_POOL = None
MAX_TIMEOUT = 60  # Global max timeout cap
IS_PRODUCTION = os.getenv("REPLIT_DEPLOYMENT") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

# === FRANCHISE CONFIGURATIONS ===
FRANCHISES = [
    {
        "name": "Pokemon",
        "store_files": [],  # Empty - we are using direct files only
        "direct_files": [
            "Pokemon/Poke-30A.txt",
            "Pokemon/Poke-AH.txt",
            "Pokemon/Poke-DR.txt",
            "Pokemon/Poke-ME.txt",
            "Pokemon/Poke-PO.txt"
        ],
        "webhook_secrets": [
            {"file": "Pokemon/Poke-30A.txt", "webhook": os.getenv("POKE30A"), "role_id": os.getenv("POKE30A_ROLE")},
            {"file": "Pokemon/Poke-AH.txt", "webhook": os.getenv("POKEAH"), "role_id": os.getenv("POKEAH_ROLE")},
            {"file": "Pokemon/Poke-DR.txt", "webhook": os.getenv("POKEDR"), "role_id": os.getenv("POKEDR_ROLE")},
            {"file": "Pokemon/Poke-ME.txt", "webhook": os.getenv("POKEME"), "role_id": os.getenv("POKEME_ROLE")},
            {"file": "Pokemon/Poke-PO.txt", "webhook": os.getenv("POKEPO"), "role_id": os.getenv("POKEPO_ROLE")},
        ]
    },
    {
        "name": "One Piece",
        "store_files": [],  # Empty - we are using direct files only
        "direct_files": [
            "One Piece/EB-02.txt",
            "One Piece/EB-03.txt",
            "One Piece/IB-V5.txt",
            "One Piece/IB-V6.txt",
            "One Piece/OP-13.txt",
            "One Piece/OP-14.txt"
        ],
        "webhook_secrets": [
            {"file": "One Piece/EB-02.txt", "webhook": os.getenv("EB02"), "role_id": os.getenv("EB02_ROLE")},
            {"file": "One Piece/EB-03.txt", "webhook": os.getenv("EB03"), "role_id": os.getenv("EB03_ROLE")},
            {"file": "One Piece/IB-V5.txt", "webhook": os.getenv("IBV5"), "role_id": os.getenv("IBV5_ROLE")},
            {"file": "One Piece/IB-V6.txt", "webhook": os.getenv("IBV6"), "role_id": os.getenv("IBV6_ROLE")},
            {"file": "One Piece/OP-13.txt", "webhook": os.getenv("OP13"), "role_id": os.getenv("OP13_ROLE")},
            {"file": "One Piece/OP-14.txt", "webhook": os.getenv("OP14"), "role_id": os.getenv("OP14_ROLE")},
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

# === SHOPIFY ANTIBOT LIST ===
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
    print(f"   DATABASE_URL exists: {bool(DATABASE_URL)}")
    print(f"   REPLIT_DEPLOYMENT: {os.getenv('REPLIT_DEPLOYMENT')}")

    if not DATABASE_URL:
        print("❌ DATABASE_URL not found! Database features will be disabled.")
        return False

    if DB_POOL is None:
        try:
            DB_POOL = pool.SimpleConnectionPool(1, 10, DATABASE_URL, connect_timeout=10)
            print("✅ Database connection pool initialized")
            return True
        except Exception as e:
            print(f"❌ Failed to create connection pool: {e}")
            traceback.print_exc()
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
        cur.execute("""
            ALTER TABLE product_state ADD COLUMN IF NOT EXISTS last_alerted TIMESTAMP
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

def normalize_product_url(url):
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        if not path:
            return None

        # Strip Shopify collection path if present
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

# === STOCK CLASSIFICATION TERMS ===
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

    js_config_pattern = re.compile(r'\b\w+(?:Form|Button|Text|Label|Message)(?:SoldOut|OutOfStock|AddToCart|PreOrder|InStock)["\']?\s*[:=]\s*["\'][^"\']+["\']', re.IGNORECASE)
    text_normalized = js_config_pattern.sub('', text_normalized)

    js_object_pattern = re.compile(r'["\']?\w+["\']?\s*:\s*["\'](?:sold out|out of stock|add to cart|pre-order|in stock)["\']', re.IGNORECASE)
    text_normalized = js_object_pattern.sub('', text_normalized)

    text_lower = text_normalized.lower()

    authoritative_out_terms = [
        "schema.org/outofstock",
        '"availability":"outofstock"', '"availability": "outofstock"',
        '"availability":"out of stock"', '"availability": "out of stock"',
        '"stock_status":"outofstock"', '"stock_status": "outofstock"',
        "currently unavailable.",
    ]
    for term in authoritative_out_terms:
        if term in text_lower:
            return "out"

    out_match = OUT_OF_STOCK_PATTERN.search(text_lower)
    in_match = IN_STOCK_PATTERN.search(text_lower)
    preorder_match = PREORDER_PATTERN.search(text_lower)

    matches = []
    if out_match:
        matches.append(('out', out_match.start()))
    if in_match:
        matches.append(('in', in_match.start()))
    if preorder_match:
        matches.append(('preorder', preorder_match.start()))

    if not matches:
        return "unknown"

    matches.sort(key=lambda x: x[1])
    first_status = matches[0][0]

    if first_status == 'in' and out_match:
        if out_match.start() - in_match.start() < 500:
            return "out"

    return first_status

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
    """Load URLs from multiple text files and return combined unique list."""
    all_urls = set()
    for file_path in file_list:
        try:
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and line.startswith("http"):
                        url = line.split()[0]
                        all_urls.add(url)
        except FileNotFoundError:
            print(f"⚠️ Warning: File {file_path} not found - skipping")
    return list(all_urls)

def load_state():
    state = {}
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set! Cannot load state.")
        return state
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT store_url, product_url, product_name, in_stock, last_alerted FROM product_state WHERE store_url != product_url")
        rows = cur.fetchall()
        for store_url, product_url, product_name, in_stock, last_alerted in rows:
            if store_url not in state:
                state[store_url] = {}
            state[store_url][product_url] = {
                "name": product_name,
                "in_stock": in_stock,
                "last_alerted": last_alerted
            }
        cur.close()
        return_db_connection(conn)
        print(f"📂 Database: Loaded {len(rows)} products from {len(state)} stores")
    except Exception as e:
        print(f"❌ Error loading state from database: {e}")
        print("   Bot will treat this as first run - no alerts will be sent")
        if conn:
            return_db_connection(conn)
    return state

def load_direct_state():
    direct_state = {}
    if not DATABASE_URL:
        return direct_state
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT store_url, product_name, in_stock, last_alerted FROM product_state WHERE store_url = product_url")
        rows = cur.fetchall()
        for url, product_name, in_stock, last_alerted in rows:
            direct_state[url] = {
                "name": product_name or "",
                "in_stock": in_stock,
                "stock_status": "unknown",
                "last_alerted": last_alerted
            }
        cur.close()
        return_db_connection(conn)
        print(f"📂 Database: Loaded {len(rows)} direct products")
    except Exception as e:
        print(f"❌ Error loading direct state: {e}")
        if conn:
            return_db_connection(conn)
    return direct_state

def save_product(store_url, product_url, product_name, in_stock):
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
        """, (store_url, product_url, product_name, in_stock))
        conn.commit()
        cur.close()
        return_db_connection(conn)
        return True
    except Exception as e:
        print(f"⚠️ Error saving product: {e}")
        if conn:
            return_db_connection(conn)
        return False

def mark_alerted(store_url, product_url):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE product_state 
            SET last_alerted = CURRENT_TIMESTAMP
            WHERE store_url = %s AND product_url = %s
        """, (store_url, product_url))
        conn.commit()
        cur.close()
        return_db_connection(conn)
    except Exception as e:
        print(f"⚠️ Error marking alerted: {e}")
        if conn:
            return_db_connection(conn)

ALERT_COOLDOWN_MINUTES = 30
VERIFY_COOLDOWN_MINUTES = 10
LAST_VERIFIED = {}

def should_verify(product_url):
    last = LAST_VERIFIED.get(product_url)
    return not last or datetime.now() - last >= timedelta(minutes=VERIFY_COOLDOWN_MINUTES)

def mark_verified_now(product_url):
    LAST_VERIFIED[product_url] = datetime.now()

def should_alert(last_alerted):
    if last_alerted is None:
        return True

    if isinstance(last_alerted, str):
        try:
            last_alerted = datetime.fromisoformat(last_alerted)
        except:
            return True

    cooldown = timedelta(minutes=ALERT_COOLDOWN_MINUTES)
    time_since_alert = datetime.now() - last_alerted

    return time_since_alert >= cooldown

def is_url_initialized(store_url):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM product_state WHERE store_url = %s LIMIT 1
        """, (store_url,))
        exists = cur.fetchone() is not None
        cur.close()
        return_db_connection(conn)
        return exists
    except:
        if conn:
            return_db_connection(conn)
        return False

def mark_url_initialized(store_url):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO product_state (store_url, product_url, product_name, in_stock)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (store_url, store_url, "__init__", False))
        conn.commit()
        cur.close()
        return_db_connection(conn)
    except:
        if conn:
            return_db_connection(conn)

def should_silence_first_run(store_url):
    return not is_url_initialized(store_url)

def save_state(state):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        batch = []
        for store_url, products in state.items():
            for product_url, product_data in products.items():
                if isinstance(product_data, dict):
                    last_alerted = product_data.get("last_alerted")
                    batch.append((store_url, product_url, product_data.get("name"), product_data.get("in_stock", True), last_alerted))
                else:
                    batch.append((store_url, product_url, None, True, None))

        if batch:
            from psycopg2.extras import execute_values
            execute_values(cur, """
                INSERT INTO product_state (store_url, product_url, product_name, in_stock, last_seen, last_alerted)
                VALUES %s
                ON CONFLICT (store_url, product_url) 
                DO UPDATE SET product_name = EXCLUDED.product_name, 
                              in_stock = EXCLUDED.in_stock,
                              last_seen = CURRENT_TIMESTAMP,
                              last_alerted = COALESCE(EXCLUDED.last_alerted, product_state.last_alerted)
            """, batch, template="(%s, %s, %s, %s, CURRENT_TIMESTAMP, %s)")
            conn.commit()

        cur.close()
        return_db_connection(conn)
    except Exception as e:
        print(f"⚠️ Error saving state: {e}")
        if conn:
            return_db_connection(conn)

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

def send_alert(product_name, url, store_name, is_preorder=False, is_new=False, 
               image_url=None, price=None, store_file=None):
    webhook_url = None
    role_id = None

    # Find matching webhook/role for this store file
    for group in CURRENT_FRANCHISE.get("webhook_secrets", []):
        if store_file and group["file"] == store_file:
            webhook_url = group["webhook"]
            role_id = group["role_id"]
            break

    if webhook_url is None:
        webhook_url = CURRENT_WEBHOOK
        role_id = CURRENT_ROLE_ID

    if webhook_url is None:
        print("Warning: No webhook configured for this file/group")
        return

    # Determine alert type and colors
    if is_preorder:
        title = f"PREORDER AVAILABLE - {product_name}"
        status = "Preorder"
        color = 16753920  # Orange
        emoji = "📋"
    elif is_new:
        title = f"NEW PRODUCT - {product_name}"
        status = "In Stock"
        color = 3447003  # Blue
        emoji = "🆕"
    else:
        title = f"BACK IN STOCK - {product_name}"
        status = "In Stock"
        color = 65280  # Green
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

        image_url = None
        img_selectors = [
            'img.product-featured-image', 'img.product-image', 'img.product__image',
            'img[class*="product"]', 'img[class*="gallery"]', 'img[class*="main"]',
            '.product-image img', '.product-photo img', '.gallery img',
            '#product-image img', '.product-single__photo img',
            'meta[property="og:image"]', 'meta[name="og:image"]'
        ]
        for selector in img_selectors:
            try:
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
            except:
                continue

        price = None
        price_patterns = [
            r'£[\d,]+\.?\d*', r'\$[\d,]+\.?\d*', r'€[\d,]+\.?\d*'
        ]
        price_selectors = [
            '.price', '.product-price', '.current-price', '[class*="price"]',
            'meta[property="product:price:amount"]', 'meta[property="og:price:amount"]',
            '.money', 'span.money'
        ]
        for selector in price_selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    if elem.name == 'meta':
                        price_val = elem.get('content')
                        if price_val:
                            price = f"£{price_val}"
                            break
                    else:
                        price_text = elem.get_text(strip=True)
                        for pattern in price_patterns:
                            match = re.search(pattern, price_text)
                            if match:
                                price = match.group()
                                break
                        if price:
                            break
            except:
                continue

        if is_store_unavailable(page_text):
            print("⚠️ Store unavailable/maintenance")
            return "unknown", product_name, image_url, price

        raw_lower = raw_html.lower()

        deny_terms = ["captcha", "access denied", "cloudflare", "attention required", 
                      "verify you are human", "please wait", "checking your browser",
                      "ddos protection", "security check"]
        if any(t in raw_lower for t in deny_terms):
            print("⚠️ Anti-bot page detected")
            return "unknown", product_name, image_url, price

        is_real_product_page = False
        product_indicators = [
            'og:type" content="product',
            'og:product',
            'schema.org/product',
            '"@type":"product"',
            "'@type':'product'",
            'product:price',
            'itemprop="price"',
            'class="product-',
            'class="product_',
            'id="product-',
            'data-product-id',
            'add-to-cart',
            'addtocart',
            'product-form',
        ]
        for indicator in product_indicators:
            if indicator.lower() in raw_lower:
                is_real_product_page = True
                break

        if price:
            is_real_product_page = True

        stock_status = classify_stock(page_text + " " + raw_html)

        if not is_real_product_page and stock_status == "out":
            return "unknown", product_name, image_url, price

        return stock_status, product_name, image_url, price

    except Exception as e:
        print(f"❌ confirm_product_stock error: {e}")
        return "unknown", None, None, None

def get_product_card_text(link):
    card_text = ""
    for _ in range(5):
        parent = link.parent
        if parent is None:
            break
        card_text = parent.get_text().lower()
        if len(card_text) > 50:
            break
        link = parent

    if not card_text:
        card_text = link.get_text().lower()

    for sibling in link.find_next_siblings(limit=3):
        card_text += " " + sibling.get_text().lower()
    for sibling in link.find_previous_siblings(limit=3):
        card_text += " " + sibling.get_text().lower()

    classes = link.get('class', [])
    if isinstance(classes, list):
        card_text += " " + " ".join(classes).lower()

    return card_text

def find_product_container(soup):
    container_selectors = [
        '.productListing', '.product-listing', '.products-grid', '.product-grid',
        '.products', '#products', '.product-list', '#product-list',
        '.categoryProducts', '.category-products', '.listing-products',
        '[class*="product-list"]', '[class*="productList"]',
        '.collection-products', '.grid-products', '.product-items',
        '[data-section-type="collection"]', '.grid--uniform', '.collection-grid',
        '.product-grid-item', 'ul.products', 'div.grid-products', '[role="list"]',
        'main', '.main-content', '#main-content', '.content-main'
    ]

    for selector in container_selectors:
        try:
            container = soup.select_one(selector)
            if container:
                links = container.find_all('a', href=True)
                if len(links) >= 3:
                    return container
        except:
            continue

    return None

def extract_products(soup, base_url):
    products = {}

    container = find_product_container(soup)
    if container:
        product_links = container.find_all('a', href=True)
    else:
        product_links = soup.select('a[href*="/products/"], a[href*="/product/"]')
        if not product_links:
            product_links = soup.find_all('a', href=True)

    for link in product_links:
        href = link.get('href', '')
        full_url = urljoin(base_url, href)

        if not is_product_url(full_url, base_url):
            continue

        product_name = ""
        img = link.find('img')
        if img:
            product_name = img.get('alt', '') or img.get('title', '')

        if not product_name:
            product_name = link.get('title', '')

        if not product_name:
            title_elem = link.find(['h2', 'h3', 'h4', 'span', 'p'])
            if title_elem:
                product_name = title_elem.get_text(strip=True)

        if not product_name:
            product_name = link.get_text(strip=True)

        if not product_name or len(product_name) < 3:
            continue

        product_name = product_name[:100]

        if not is_tcg_product(product_name, full_url):
            continue

        card_text = get_product_card_text(link)
        category_state = classify_stock(card_text)

        is_in_stock = category_state == "preorder"

        normalized_url = normalize_product_url(full_url)
        if normalized_url and normalized_url not in products:
            products[normalized_url] = {
                "name": product_name,
                "in_stock": is_in_stock,
                "category_state": category_state
            }

    return products

def get_category_id_from_url(url):
    match = re.search(r'-c-(\d+(?:_\d+)*)', url)
    if match:
        return match.group(1)
    return None

def is_shopify_store(url):
    shopify_indicators = ['/collections/', '/products/', 'myshopify.com']
    return any(indicator in url.lower() for indicator in shopify_indicators) or \
           any(store in url.lower() for store in SHOPIFY_STORES_WITH_ANTIBOT)

def fetch_shopify_products_json(base_url):
    try:
        parsed = urlparse(base_url)

        if '?q=' in base_url or '&q=' in base_url or 'search' in base_url.lower():
            return None

        skip_handles = ['vendors', 'types', 'tags', 'all']
        for skip in skip_handles:
            if f'/collections/{skip}' in base_url.lower():
                return None

        collection_match = re.search(r'/collections/([^/?]+)', base_url)
        collection_handle = collection_match.group(1) if collection_match else None

        if collection_handle:
            products_url = f"{parsed.scheme}://{parsed.netloc}/collections/{collection_handle}/products.json?limit=250"
        else:
            products_url = f"{parsed.scheme}://{parsed.netloc}/products.json?limit=250"

        headers = get_headers_for_url(base_url)
        r = SESSION.get(products_url, headers=headers, timeout=15)

        if r.status_code != 200:
            return None

        data = r.json()
        products = {}

        for product in data.get('products', []):
            product_handle = product.get('handle', '')
            product_title = product.get('title', '')

            if not product_handle or not product_title:
                continue

            if not is_tcg_product(product_title, product_handle):
                continue

            if collection_handle:
                product_url = f"{parsed.scheme}://{parsed.netloc}/collections/{collection_handle}/products/{product_handle}"
            else:
                product_url = f"{parsed.scheme}://{parsed.netloc}/products/{product_handle}"
            normalized_url = normalize_product_url(product_url)

            if normalized_url:
                available = any(v.get('available', False) for v in product.get('variants', []))
                products[normalized_url] = {
                    "name": product_title[:100],
                    "in_stock": available,
                    "category_state": "in" if available else "out"
                }

        return products if products else None
    except Exception as e:
        return None

def fetch_shopify_sitemap_products(base_url):
    try:
        parsed = urlparse(base_url)
        sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap_products_1.xml"

        headers = get_headers_for_url(base_url)
        r = SESSION.get(sitemap_url, headers=headers, timeout=15)

        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, 'xml')
        products = {}

        for url_tag in soup.find_all('url'):
            loc = url_tag.find('loc')
            if not loc:
                continue

            product_url = loc.get_text(strip=True)
            if '/products/' not in product_url:
                continue

            image_title = url_tag.find('image:title')
            product_name = image_title.get_text(strip=True) if image_title else ""

            if not product_name:
                product_name = product_url.split('/products/')[-1].replace('-', ' ').title()

            if not is_tcg_product(product_name, product_url):
                continue

            normalized_url = normalize_product_url(product_url)
            if normalized_url and normalized_url not in products:
                products[normalized_url] = {
                    "name": product_name[:100],
                    "in_stock": False,
                    "category_state": "unknown"
                }

        return products if products else None
    except Exception as e:
        return None

def shopify_fallback_discovery(base_url, html_products_count):
    if not is_shopify_store(base_url):
        return None

    products_json = fetch_shopify_products_json(base_url)
    if products_json and len(products_json) > html_products_count:
        return products_json

    sitemap_products = fetch_shopify_sitemap_products(base_url)
    if sitemap_products and len(sitemap_products) > html_products_count:
        return sitemap_products

    return None

def is_product_url(url, base_url):
    if not url.startswith("http"):
        return False

    base_domain = urlparse(base_url).netloc
    url_domain = urlparse(url).netloc

    if base_domain != url_domain:
        return False

    base_category = get_category_id_from_url(base_url)
    if base_category:
        if re.search(r'-c-\d+', url):
            url_category = get_category_id_from_url(url)
            if url_category and url_category != base_category:
                if not url_category.startswith(base_category):
                    return False

    product_patterns = [
        r'/product[s]?/', r'/item/', r'/p/', r'/shop/',
        r'/collections/[^/]+/products/', r'/products/',
        r'-p-\d+', r'/dp/', r'/gp/product/',
        r'\.html$', r'/catalog/', r'/pokemon-'
    ]

    exclude_patterns = [
        r'/cart', r'/basket', r'/checkout', r'/account',
        r'/login', r'/register', r'/wishlist', r'/search',
        r'/page/', r'/category/', r'/collections/?$',
        r'/cdn/', r'/static/', r'\.js$', r'\.css$',
        r'\.jpg$', r'\.png$', r'\.gif$', r'/cdn-cgi/',
        r'-c-\d+(?:_\d+)*/?(?:\?|$)',
        r'/collections/[^/]+$', r'/collections/[^/]+\?'
    ]

    url_lower = url.lower()

    for pattern in exclude_patterns:
        if re.search(pattern, url_lower):
            return False

    for pattern in product_patterns:
        if re.search(pattern, url_lower):
            return True

    return False

def get_timeout_for_url(url):
    slow_sites = ["very.co.uk", "game.co.uk", "johnlewis.com", "argos.co.uk"]
    if any(site in url for site in slow_sites):
        return 30
    return 15

def check_direct_product(url, previous_state, stats, store_file=None):
    if is_site_in_failure_cooldown(url):
        print(f"⏭️ SKIPPED (failed recently)")
        stats['skipped'] += 1
        return previous_state, None

    try:
        headers = get_headers_for_url(url)
        timeout = get_timeout_for_url(url)
        r = SESSION.get(url, headers=headers, timeout=min(timeout, MAX_TIMEOUT))
        stats['fetched'] += 1

        if r.status_code != 200:
            print(f"⚠️ Failed ({r.status_code})")
            mark_site_failed(url)
            stats['failed'] += 1
            return previous_state, None

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

        if is_store_unavailable(page_text):
            print("⚠️ Store unavailable/maintenance")
            return {
                "name": product_name,
                "in_stock": False,
                "stock_status": "unknown",
                "last_alerted": previous_state.get("last_alerted") if previous_state else None
            }, None

        raw_lower = raw_html.lower()

        deny_terms = ["captcha", "access denied", "cloudflare", "attention required", 
                      "verify you are human", "please wait", "checking your browser",
                      "ddos protection", "security check"]
        if any(t in raw_lower for t in deny_terms):
            print("⚠️ Anti-bot page detected")
            return {
                "name": product_name,
                "in_stock": False,
                "stock_status": "unknown",
                "last_alerted": previous_state.get("last_alerted") if previous_state else None
            }, None

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
        print(f"⏱️ TIMEOUT")
        mark_site_failed(url)
        stats['failed'] += 1
        return previous_state, None
    except Exception as e:
        print(f"❌ Unexpected error during scan: {e}")
        traceback.print_exc()
        mark_site_failed(url)
        stats['failed'] += 1
        return previous_state, None

def check_store_page(url, previous_products, stats, store_file=None):
    if is_js_only_url(url):
        print(f"⏭️ SKIPPED (JS-only URL pattern)")
        stats['skipped'] += 1
        return previous_products, []

    if is_in_js_skip_cache(url):
        print(f"⏭️ SKIPPED (JS page, cached)")
        stats['skipped'] += 1
        return previous_products, []

    if is_site_in_failure_cooldown(url):
        print(f"⏭️ SKIPPED (failed recently)")
        stats['skipped'] += 1
        return previous_products, []

    try:
        headers = get_headers_for_url(url)
        timeout = get_timeout_for_url(url)
        r = SESSION.get(url, headers=headers, timeout=min(timeout, MAX_TIMEOUT))
        stats['fetched'] += 1

        if r.status_code != 200:
            print(f"⚠️ Failed (status {r.status_code})")
            mark_site_failed(url)
            stats['failed'] += 1
            return previous_products, []

        soup = BeautifulSoup(r.text, "html.parser")
        current_products = extract_products(soup, url)

        prev_count = len(previous_products)
        curr_count = len(current_products)

        if is_js_only_page(r.text, curr_count) and prev_count == 0:
            print(f"⏭️ JS-ONLY PAGE (adding to skip cache)")
            add_to_js_skip_cache(url)
            stats['skipped'] += 1
            return previous_products, []

        looks_blocked = (prev_count > 0 and curr_count < prev_count * 0.5) or curr_count == 0
        if looks_blocked and is_shopify_store(url):
            print(f"🔄 FALLBACK", end=" ")
            fallback_products = shopify_fallback_discovery(url, curr_count)
            if fallback_products:
                for purl, pinfo in fallback_products.items():
                    if purl not in current_products:
                        current_products[purl] = pinfo
                    elif pinfo.get("in_stock") and not current_products[purl].get("in_stock"):
                        current_products[purl]["in_stock"] = True
                        current_products[purl]["category_state"] = pinfo.get("category_state", "in")
                curr_count = len(current_products)
                print(f"({curr_count} products via API)", end=" ")

        changes = []

        for product_url, product_info in current_products.items():
            if product_url not in previous_products:
                changes.append({
                    "type": "new",
                    "name": product_info["name"],
                    "url": product_url,
                    "in_stock": product_info["in_stock"],
                    "store_file": store_file
                })
            else:
                prev_info = previous_products[product_url]
                current_products[product_url]["last_alerted"] = prev_info.get("last_alerted")

                category_state = product_info.get("category_state", "unknown")
                category_available = product_info.get("in_stock", False)
                prev_in_stock = prev_info.get("in_stock", False)

                current_products[product_url]["in_stock"] = prev_in_stock

                was_out = not prev_in_stock

                if is_verified_out(product_url):
                    continue

                if was_out and (category_available or category_state == "in"):
                    last_alerted = prev_info.get("last_alerted")
                    if should_alert(last_alerted):
                        changes.append({
                            "type": "restock",
                            "name": product_info["name"],
                            "url": product_url,
                            "store_file": store_file
                        })

        for prev_url in previous_products:
            if prev_url not in current_products:
                clear_verified_out(prev_url)

        return current_products, changes

    except requests.exceptions.Timeout:
        print(f"⏱️ TIMEOUT")
        mark_site_failed(url)
        stats['failed'] += 1
        return previous_products, []
    except Exception as e:
        print(f"❌ Unexpected error during scan: {e}")
        traceback.print_exc()
        mark_site_failed(url)
        stats['failed'] += 1
        return previous_products, []

def main():
    global CURRENT_WEBHOOK, CURRENT_ROLE_ID

    print("🚀 Starting Store Monitor Bot...")
    print(f"   Python version: {os.popen('python3 --version').read().strip()}")
    print(f"   Time: {datetime.now(timezone.utc)}")
    print(f"   Production mode: {IS_PRODUCTION}")
    print(f"   Franchises: {', '.join(f['name'] for f in FRANCHISES)}")
    print()

    max_retries = 3 if IS_PRODUCTION else 1
    db_ready = False

    for attempt in range(max_retries):
        if attempt > 0:
            print(f"   Retry {attempt + 1}/{max_retries} in 10 seconds...")
            time.sleep(10)

        if init_db_pool():
            if init_database():
                db_ready = True
                break
            else:
                print("❌ Database table initialization failed")
        else:
            print("❌ Database pool initialization failed")

    if not db_ready:
        print("❌ Failed to initialize database after retries. Exiting.")
        return

    state = load_state()
    direct_state = load_direct_state()
    first_run = len(state) == 0

    if first_run:
        print("🆕 First run - building initial product database...")
        print("   (No alerts will be sent on first run)\n")

    for franchise in FRANCHISES:
        store_files = franchise.get("store_files", [])
        direct_files = franchise.get("direct_files", [])
        webhook_groups = len(franchise.get("webhook_secrets", []))
        print(f"   {franchise['name']}: {len(store_files)} store files + {len(direct_files)} direct files, Webhook groups: {webhook_groups}")
    print()

    while True:
        cycle_start = time.time()
        total_cycle_changes = 0
        total_stats = {'fetched': 0, 'skipped': 0, 'failed': 0}

        for franchise in FRANCHISES:
            global CURRENT_WEBHOOK, CURRENT_ROLE_ID
            franchise_name = franchise["name"]
            CURRENT_WEBHOOK = None
            CURRENT_ROLE_ID = None

            print(f"\n{'='*50}")
            print(f"📋 Scanning {franchise_name}...")
            print(f"{'='*50}")

            store_files = franchise.get("store_files", [])
            direct_files = franchise.get("direct_files", [])

            urls = load_urls(store_files)
            direct_urls = [normalize_product_url(u) for u in load_urls(direct_files) if normalize_product_url(u)]

            if not urls and not direct_urls:
                print(f"   No URLs configured for {franchise_name}")
                continue

            stats = {'fetched': 0, 'skipped': 0, 'failed': 0}
            franchise_changes = 0

            if urls:
                print(f"\n🔍 Scanning {len(urls)} {franchise_name} store pages...")
                for url in urls:
                    store_name = urlparse(url).netloc
                    print(f"  Checking: {store_name}...", end=" ")

                    silence_alerts = should_silence_first_run(url)
                    if silence_alerts:
                        mark_url_initialized(url)
                        print(f"🆕 First scan, will scan next cycle")
                        time.sleep(random.uniform(1, 2))
                        continue

                    prev_products = state.get(url, {})
                    current_products, changes = check_store_page(url, prev_products, stats, store_file=url)

                    state[url] = current_products

                    if changes and not first_run and not silence_alerts:
                        print(f"Found {len(changes)} potential changes - verifying on product pages...")
                        for change in changes:
                            product_url = change["url"]
                            change_type = change["type"]
                            category_name = change["name"][:50]
                            store_file = change.get("store_file")

                            if is_verified_out(product_url):
                                print(f"    ⏭️ Skipping: {category_name}... (recently verified OUT)")
                                continue

                            if not should_verify(product_url):
                                print(f"    ⏭️ Skipping: {category_name}... (verify cooldown)")
                                continue

                            print(f"    🔍 Verifying: {category_name}...", end=" ")
                            confirmed_status, confirmed_name, image_url, price = confirm_product_stock(product_url)

                            if confirmed_status != "unknown":
                                mark_verified_now(product_url)

                            display_name = confirmed_name if confirmed_name else change["name"]

                            if product_url in state[url]:
                                if confirmed_status in ("in", "preorder"):
                                    state[url][product_url]["in_stock"] = True
                                    clear_verified_out(product_url)
                                elif confirmed_status == "out":
                                    state[url][product_url]["in_stock"] = False

                            if confirmed_status == "in":
                                clear_verified_out(product_url)
                                last_alerted = state[url].get(product_url, {}).get("last_alerted")
                                if not should_alert(last_alerted):
                                    print(f"✅ IN STOCK (cooldown active, no alert)")
                                else:
                                    franchise_changes += 1
                                    is_new_product = (change_type == "new")
                                    if is_new_product:
                                        print(f"✅ IN STOCK!")
                                    else:
                                        print(f"✅ RESTOCKED!")
                                    send_alert(display_name, product_url, store_name, 
                                              is_preorder=False, is_new=is_new_product,
                                              image_url=image_url, price=price,
                                              store_file=store_file)
                                    mark_alerted(url, product_url)
                                    if product_url in state[url]:
                                        state[url][product_url]["last_alerted"] = datetime.now(timezone.utc)

                            elif confirmed_status == "preorder":
                                clear_verified_out(product_url)
                                last_alerted = state[url].get(product_url, {}).get("last_alerted")
                                if not should_alert(last_alerted):
                                    print(f"📋 PREORDER (cooldown active, no alert)")
                                else:
                                    franchise_changes += 1
                                    print(f"📋 PREORDER!")
                                    send_alert(display_name, product_url, store_name,
                                              is_preorder=True, is_new=False,
                                              image_url=image_url, price=price,
                                              store_file=store_file)
                                    mark_alerted(url, product_url)
                                    if product_url in state[url]:
                                        state[url][product_url]["last_alerted"] = datetime.now(timezone.utc)

                            elif confirmed_status == "out":
                                mark_verified_out(product_url)
                                print(f"❌ OUT OF STOCK (no alert)")

                            else:
                                print(f"❓ UNKNOWN (no alert)")

                            time.sleep(1)
                    else:
                        print(f"OK ({len(current_products)} products)")

                    time.sleep(random.uniform(2, 4))

            if direct_urls:
                print(f"\n🎯 Checking {len(direct_urls)} {franchise_name} direct products...")
                for url in direct_urls:
                    store_name = urlparse(url).netloc
                    print(f"  Checking: {store_name}...", end=" ")

                    silence_alerts = should_silence_first_run(url)
                    prev_state = direct_state.get(url)
                    current_state, change = check_direct_product(url, prev_state, stats, store_file=url)

                    if silence_alerts:
                        mark_url_initialized(url)
                        if current_state:
                            current_state["last_alerted"] = datetime.now(timezone.utc)
                            direct_state[url] = current_state
                            save_product(url, url, current_state.get("name"), current_state.get("in_stock", False))
                        else:
                            direct_state[url] = {"name": "", "in_stock": False, "stock_status": "unknown", "last_alerted": datetime.now(timezone.utc)}
                            save_product(url, url, "", False)
                        print(f"🆕 First scan, alerts silenced")
                        continue

                    if current_state:
                        direct_state[url] = current_state
                        detailed_status = current_state.get("stock_status", "unknown").upper()

                        if change and not first_run and not silence_alerts:
                            print(f"🔍 Potential {change.get('type', 'change')} - verifying...", end=" ")
                            time.sleep(1)
                            verified_state, _ = check_direct_product(url, direct_state.get(url), stats, store_file=url)
                            if verified_state:
                                verified_status = verified_state.get("stock_status", "unknown")

                                if verified_status in ("in", "preorder"):
                                    direct_state[url]["in_stock"] = True
                                else:
                                    direct_state[url]["in_stock"] = False

                                if verified_status in ("in", "preorder"):
                                    last_alerted = direct_state.get(url, {}).get("last_alerted")
                                    if not should_alert(last_alerted):
                                        print(f"✅ IN STOCK (cooldown active, no alert)")
                                        direct_state[url]["in_stock"] = True
                                        save_product(url, url, current_state.get("name"), True)
                                    else:
                                        franchise_changes += 1
                                        change_type = change.get("type", "restock")
                                        is_preorder = (change_type == "preorder" or verified_status == "preorder")

                                        if is_preorder:
                                            print(f"✅ PREORDER CONFIRMED!")
                                        else:
                                            print(f"✅ RESTOCK CONFIRMED!")

                                        send_alert(change['name'], change["url"], store_name,
                                                  is_preorder=is_preorder, is_new=False,
                                                  image_url=None, price=None,
                                                  store_file=change.get("store_file"))
                                        direct_state[url]["last_alerted"] = datetime.now(timezone.utc)
                                        save_product(url, url, current_state.get("name"), True)
                                        mark_alerted(url, url)
                                else:
                                    print(f"❌ Verification failed ({verified_status.upper()})")
                                    save_product(url, url, current_state.get("name"), False)
                            else:
                                print(f"❌ Verification failed (no response)")
                        else:
                            print(f"{detailed_status}")

                        prev_in_stock = prev_state.get("in_stock") if prev_state else None
                        if current_state.get("in_stock") != prev_in_stock:
                            save_product(url, url, current_state.get("name"), current_state.get("in_stock"))

                    time.sleep(random.uniform(2, 4))

            print(f"\n📊 {franchise_name}: {franchise_changes} alerts sent")
            print(f"   Stats: {stats['fetched']} fetched, {stats['skipped']} skipped, {stats['failed']} failed")

            total_cycle_changes += franchise_changes
            total_stats['fetched'] += stats['fetched']
            total_stats['skipped'] += stats['skipped']
            total_stats['failed'] += stats['failed']

        save_state(state)

        cycle_time = round(time.time() - cycle_start, 1)

        if first_run:
            total_products = sum(len(products) for products in state.values())
            print(f"\n{'='*50}")
            print(f"✅ Initial scan complete! Tracking {total_products} products")
            print("   Future changes will trigger Discord alerts.")
            print(f"{'='*50}\n")
            first_run = False
        else:
            print(f"\n{'='*50}")
            if total_cycle_changes > 0:
                print(f"📊 Cycle complete. {total_cycle_changes} total alerts sent.")
            else:
                print(f"📊 Cycle complete. No changes detected.")

        print(f"📈 Total: {total_stats['fetched']} fetched, {total_stats['skipped']} skipped, {total_stats['failed']} failed | Cycle: {cycle_time}s")
        if FAILED_SITES:
            print(f"   ⏸️ {len(FAILED_SITES)} sites in failure cooldown")
        if JS_SKIP_CACHE:
            print(f"   🚫 {len(JS_SKIP_CACHE)} JS-only pages in skip cache")

        print(f"⏱️ Next scan in {CHECK_INTERVAL} seconds...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()