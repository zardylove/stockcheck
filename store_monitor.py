import requests
from bs4 import BeautifulSoup
import time
import os
import random
import json
import re
import psycopg2
from psycopg2 import pool
import traceback
from urllib.parse import urljoin, urlparse, urlunparse
from datetime import datetime, timedelta

# === DATABASE CONNECTION POOL ===
DB_POOL = None
MAX_TIMEOUT = 60  # Global max timeout cap
IS_PRODUCTION = os.getenv("REPLIT_DEPLOYMENT") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

# === FRANCHISE CONFIGURATIONS ===
FRANCHISES = [
    {
        "name": "Pokemon",
        "store_file": "PokeWebsites.txt",
        "direct_file": "PokeDirectProducts.txt",
        "webhook": os.getenv("POKESTOCK")
    },
    {
        "name": "One Piece",
        "store_file": "OPWebsites.txt",
        "direct_file": "OPDirectProducts.txt",
        "webhook": os.getenv("OPSTOCK")
    }
]

# Current webhook for the active franchise (set during scanning)
CURRENT_WEBHOOK = None

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
# When a product is verified as OUT, cache it so we don't re-verify every cycle
# Only clears when product is verified IN-STOCK or disappears and reappears
# Key: product_url, Value: True (no expiration)
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
    """Check if a product should be tracked (not blocked items like plushies, toys, etc.)"""
    text = f"{name} {url}".lower()
    
    # Block items matching ignore keywords
    for word in BLOCK_KEYWORDS:
        if word in text:
            return False
    
    # Allow everything else by default
    return True

# === STORE UNAVAILABILITY DETECTION (platform-agnostic) ===
STORE_UNAVAILABLE_MARKERS = [
    "enter using password",
    "password protected",
    "store closed",
    "temporarily closed",
    "temporarily unavailable",
    "site is unavailable",
    "maintenance mode",
    "under maintenance",
    "we'll be back soon",
    "coming back soon",
    "closed until",
    "currently unavailable",
    "are you the store owner",
    "admin login",
    "restricted access"
]

def is_store_unavailable(text: str) -> bool:
    """Check if page indicates store is unavailable/maintenance/password-protected."""
    if not text:
        return False
    text = text.lower()
    return any(marker in text for marker in STORE_UNAVAILABLE_MARKERS)

SHOPIFY_STORES_WITH_ANTIBOT = [
    "zingaentertainment.com",
    "themeeplerooms.co.uk",
    "jetcards.uk",
    "rockawaytoys.co.uk",
    "moon-whale.com",
    "redcardgames.com",
    "afkgaming.co.uk",
    "ajtoys.co.uk",
    "bremnertcg.co.uk",
    "coastiescollectibles.com",
    "dansolotcg.co.uk",
    "thedicedungeon.co.uk",
    "dragonvault.gg",
    "eclipsecards.com",
    "endocollects.co.uk",
    "gatheringgames.co.uk",
    "hammerheadtcg.co.uk",
    "nerdforged.co.uk",
    "peakycollectibles.co.uk",
    "safarizone.co.uk",
    "sweetsnthings.co.uk",
    "thistletavern.com",
    "thirstymeeples.co.uk",
    "titancards.co.uk",
    "toybarnhaus.co.uk",
    "travellingman.com",
    "westendgames.co.uk",
]

def init_db_pool():
    """Initialize database connection pool."""
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
    """Get database connection from pool."""
    global DB_POOL
    if DB_POOL:
        return DB_POOL.getconn()
    return psycopg2.connect(DATABASE_URL)

def return_db_connection(conn):
    """Return connection to pool."""
    global DB_POOL
    if DB_POOL and conn:
        DB_POOL.putconn(conn)
    elif conn:
        conn.close()

def init_database():
    """Initialize the database table for product state."""
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
    """Normalize URL by removing query strings, fragments, and standardizing format."""
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        if not path:
            return None
        normalized = urlunparse((parsed.scheme, parsed.netloc.lower(), path, '', '', ''))
        return normalized
    except:
        return url

CHECK_INTERVAL = 0

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

# === OPTIMIZATION: Precompiled regex for stock term matching ===
PREORDER_PATTERN = re.compile('|'.join(re.escape(term) for term in PREORDER_TERMS), re.IGNORECASE)
OUT_OF_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in OUT_OF_STOCK_TERMS), re.IGNORECASE)
IN_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in IN_STOCK_TERMS), re.IGNORECASE)

def classify_stock(text):
    """
    Classify stock status from page text.
    Returns: 'preorder', 'out', 'in', or 'unknown'
    
    Strategy:
    1. Check for authoritative schema.org/JSON data (highest priority)
    2. Then use position-based detection for visible content
    """
    # Normalize whitespace
    text_normalized = text.replace('\xa0', ' ').replace('\u00a0', ' ')
    text_normalized = ' '.join(text_normalized.split())
    text_lower = text_normalized.lower()
    
    # PRIORITY 1: Check for authoritative structured data (schema.org, JSON)
    # These are definitive indicators from structured product data
    authoritative_out_terms = [
        "schema.org/outofstock",
        '"availability":"outofstock"', '"availability": "outofstock"',
        '"availability":"out of stock"', '"availability": "out of stock"',
        '"stock_status":"outofstock"', '"stock_status": "outofstock"',
        "currently unavailable.",  # With period - specific phrase in JSON
    ]
    for term in authoritative_out_terms:
        if term in text_lower:
            return "out"
    
    # PRIORITY 2: Position-based detection for visible content
    out_match = OUT_OF_STOCK_PATTERN.search(text_lower)
    in_match = IN_STOCK_PATTERN.search(text_lower)
    preorder_match = PREORDER_PATTERN.search(text_lower)
    
    # Collect matches with their positions
    matches = []
    if out_match:
        matches.append(('out', out_match.start()))
    if in_match:
        matches.append(('in', in_match.start()))
    if preorder_match:
        matches.append(('preorder', preorder_match.start()))
    
    if not matches:
        return "unknown"
    
    # Sort by position - earliest match wins (main product comes first)
    matches.sort(key=lambda x: x[1])
    first_status = matches[0][0]
    
    # Special case: if first is "in" but "out" appears very close after (within 500 chars),
    # might be a "was in stock, now out" situation - prefer "out" in that case
    if first_status == 'in' and out_match:
        if out_match.start() - in_match.start() < 500:
            return "out"
    
    return first_status

def is_site_in_failure_cooldown(url):
    """Check if a site is in failure cooldown (failed recently)."""
    if url not in FAILED_SITES:
        return False
    failure_time = FAILED_SITES[url]
    if datetime.now() - failure_time < timedelta(minutes=FAILURE_COOLDOWN_MINUTES):
        return True
    del FAILED_SITES[url]
    return False

def mark_site_failed(url):
    """Mark a site as failed (will be skipped for cooldown period)."""
    FAILED_SITES[url] = datetime.now()

def is_js_only_url(url):
    """Check if URL pattern indicates JavaScript-only page."""
    return any(pattern in url.lower() for pattern in JS_URL_PATTERNS)

def is_js_only_page(html_content, product_count):
    """Check if page content indicates JavaScript-only rendering."""
    if len(html_content) < 2000 and product_count < 3:
        return True
    html_lower = html_content.lower()
    return any(indicator in html_lower for indicator in JS_PAGE_INDICATORS)

def is_in_js_skip_cache(url):
    """Check if URL is in JS skip cache."""
    if url not in JS_SKIP_CACHE:
        return False
    if datetime.now() < JS_SKIP_CACHE[url]:
        return True
    del JS_SKIP_CACHE[url]
    return False

def add_to_js_skip_cache(url):
    """Add URL to JS skip cache."""
    JS_SKIP_CACHE[url] = datetime.now() + timedelta(minutes=JS_SKIP_MINUTES)

def is_verified_out(product_url):
    """Check if product was verified as out-of-stock (no expiration)."""
    return product_url in VERIFIED_OUT_CACHE

def mark_verified_out(product_url):
    """Mark product as verified out-of-stock (persists until cleared)."""
    VERIFIED_OUT_CACHE.add(product_url)

def clear_verified_out(product_url):
    """Clear verified-out status when product becomes in-stock."""
    VERIFIED_OUT_CACHE.discard(product_url)

def load_urls(file_path="PokeWebsites.txt"):
    try:
        with open(file_path, "r") as f:
            urls = []
            for line in f:
                line = line.strip()
                if line and line.startswith("http"):
                    url = line.split()[0]
                    urls.append(url)
            return urls
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return []

def load_state():
    """Load product state from database."""
    state = {}
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set! Cannot load state.")
        return state
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT store_url, product_url, product_name, in_stock, last_alerted FROM product_state")
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

def save_product(store_url, product_url, product_name, in_stock):
    """Save a single product to the database."""
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
    """Update last_alerted timestamp for a product."""
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

def should_alert(last_alerted):
    """
    No time-based cooldown.
    Alert purely on OUT → IN transitions.
    """
    return True

def is_url_initialized(store_url):
    """Check if a store or direct product has been seen before."""
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
    """Mark a store/direct URL as initialized without alerting."""
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
    """Return True if this URL has never been scanned before."""
    return not is_url_initialized(store_url)

def save_state(state):
    """Save entire state to database using batch insert for speed."""
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

def send_alert(message, url, webhook=None):
    webhook_url = webhook or CURRENT_WEBHOOK
    if not webhook_url:
        print("Warning: Webhook not configured for this franchise")
        return
    
    data = {
        "content": f"🚨 **STOCK ALERT** 🚨\n{message}\n{url}"
    }
    try:
        r = requests.post(webhook_url, json=data, timeout=10)
        if r.status_code == 204:
            print(f"✅ Alert sent!")
        else:
            print(f"⚠️ Failed to send alert ({r.status_code})")
    except Exception as e:
        print(f"❌ Discord error: {e}")

def confirm_product_stock(product_url):
    """
    Fetch the actual product page to confirm stock status.
    Returns: ('in', 'preorder', 'out', 'unknown') and product name
    
    This eliminates false positives from category page detection.
    """
    try:
        headers = get_headers_for_url(product_url)
        timeout = get_timeout_for_url(product_url)
        r = SESSION.get(product_url, headers=headers, timeout=min(timeout, MAX_TIMEOUT))
        
        if r.status_code != 200:
            return "unknown", None
        
        soup = BeautifulSoup(r.text, "html.parser")
        page_text = soup.get_text()
        raw_html = r.text
        
        # Extract product name first (needed for return)
        product_name = None
        title_tag = soup.find('title')
        if title_tag:
            product_name = title_tag.get_text(strip=True)[:100]
        if not product_name:
            h1 = soup.find('h1')
            if h1:
                product_name = h1.get_text(strip=True)[:100]
        
        # Block alerts if store is unavailable / maintenance / password protected
        if is_store_unavailable(page_text):
            print("⚠️ Store unavailable/maintenance – treating as OUT")
            return "out", product_name
        
        # Classify stock status - check both visible text AND raw HTML for schema.org data
        stock_status = classify_stock(page_text + " " + raw_html)
        
        return stock_status, product_name
        
    except Exception as e:
        return "unknown", None

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
    """
    Extract products from a category page.
    Category scan is for discovery only: we classify stock but DO NOT send alerts here.
    Only preorders are marked as in_stock=True on category pages.
    All other states require product page confirmation.
    """
    products = {}
    
    container = find_product_container(soup)
    product_links = container.find_all('a', href=True) if container else soup.find_all('a', href=True)
    
    for link in product_links:
        href = link.get('href', '')
        full_url = urljoin(base_url, href)
        
        if not is_product_url(full_url, base_url):
            continue
        
        # Extract product name from various sources
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
        
        # Filter out non-TCG products (plushies, toys, etc.)
        if not is_tcg_product(product_name, full_url):
            continue
        
        card_text = get_product_card_text(link)
        
        # Classify stock conservatively on category page
        category_state = classify_stock(card_text)
        
        # CONSERVATIVE APPROACH:
        # - "in" on category page is UNCERTAIN (could be JS template text) → mark False
        # - "preorder" is reliable → mark True (always alert)
        # - "out" is reliable → mark False
        # - "unknown" → mark False
        if category_state == "preorder":
            is_in_stock = True  # Preorders always tracked as available
        else:
            is_in_stock = False  # Everything else requires product page confirmation
        
        normalized_url = normalize_product_url(full_url)
        if normalized_url and normalized_url not in products:
            products[normalized_url] = {
                "name": product_name,
                "in_stock": is_in_stock,
                "category_state": category_state  # Store category classification for reference
            }
    
    return products

def get_category_id_from_url(url):
    match = re.search(r'-c-(\d+(?:_\d+)*)', url)
    if match:
        return match.group(1)
    return None

# === SHOPIFY FALLBACK DISCOVERY ===
def is_shopify_store(url):
    """Check if URL is likely a Shopify store."""
    shopify_indicators = ['/collections/', '/products/', 'myshopify.com']
    return any(indicator in url.lower() for indicator in shopify_indicators) or \
           any(store in url.lower() for store in SHOPIFY_STORES_WITH_ANTIBOT)

def fetch_shopify_products_json(base_url):
    """Try to fetch products from Shopify products.json API (collection-specific if possible)."""
    try:
        parsed = urlparse(base_url)
        
        # Try collection-specific endpoint first if URL has /collections/
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
            
            # Include collection path in URL to match HTML-scraped URLs
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
    """Try to fetch products from Shopify sitemap."""
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
    """
    Fallback discovery for Shopify stores when collection HTML looks incomplete.
    Returns additional products found via products.json or sitemap.
    """
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
        r'-c-\d+(?:_\d+)*/?(?:\?|$)'
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
    """Get timeout for URL - reduced from 30 to 15 seconds, with slow site allowlist."""
    slow_sites = ["very.co.uk", "game.co.uk", "johnlewis.com", "argos.co.uk"]
    if any(site in url for site in slow_sites):
        return 30  # Reduced from 60
    return 15  # Reduced from 30

def check_direct_product(url, previous_state, stats):
    """Check a direct product URL for stock status using classify_stock."""
    # Check failure cooldown
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
        
        # Extract product name from page title or h1
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
        
        # Block alerts if store is unavailable / maintenance / password protected
        if is_store_unavailable(page_text):
            print("⚠️ Store unavailable/maintenance – treating as OUT")
            return {
                "name": product_name,
                "in_stock": False,
                "stock_status": "out",
                "last_alerted": previous_state.get("last_alerted") if previous_state else None
            }, None
        
        # Use classify_stock - check both visible text AND raw HTML for schema.org data
        stock_status = classify_stock(page_text + " " + raw_html)
        
        # Map stock_status to in_stock boolean (for DB compatibility)
        # 'in' or 'preorder' = available, 'out' or 'unknown' = not available
        is_available = stock_status in ("in", "preorder")
        
        current_state = {
            "name": product_name,
            "in_stock": is_available,
            "stock_status": stock_status,  # Store detailed status
            "last_alerted": previous_state.get("last_alerted") if previous_state else None
        }
        
        # Detect restock or preorder availability
        change = None
        if previous_state:
            was_available = previous_state.get("in_stock", False)
            prev_status = previous_state.get("stock_status", "out")
            
            # Alert if: was out/unknown, now in/preorder
            if not was_available and is_available:
                last_alerted = previous_state.get("last_alerted")
                if should_alert(last_alerted):
                    change = {
                        "type": "preorder" if stock_status == "preorder" else "restock",
                        "name": product_name,
                        "url": url
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

def check_store_page(url, previous_products, stats):
    # Check JS URL pattern skip
    if is_js_only_url(url):
        print(f"⏭️ SKIPPED (JS-only URL pattern)")
        stats['skipped'] += 1
        return previous_products, []
    
    # Check JS skip cache
    if is_in_js_skip_cache(url):
        print(f"⏭️ SKIPPED (JS page, cached)")
        stats['skipped'] += 1
        return previous_products, []
    
    # Check failure cooldown
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
        
        # Check if this is a JS-only page (empty response)
        if is_js_only_page(r.text, curr_count) and prev_count == 0:
            print(f"⏭️ JS-ONLY PAGE (adding to skip cache)")
            add_to_js_skip_cache(url)
            stats['skipped'] += 1
            return previous_products, []
        
        # === SHOPIFY FALLBACK DISCOVERY ===
        # If collection HTML looks incomplete/blocked, try products.json or sitemap
        looks_blocked = (prev_count > 0 and curr_count < prev_count * 0.5) or curr_count == 0
        if looks_blocked and is_shopify_store(url):
            print(f"🔄 FALLBACK", end=" ")
            fallback_products = shopify_fallback_discovery(url, curr_count)
            if fallback_products:
                # Merge fallback with HTML results (fallback takes precedence for stock status)
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
                # Check if this product was seen before in the database (returning product)
                # This prevents spam when products flicker due to anti-bot
                changes.append({
                    "type": "new",
                    "name": product_info["name"],
                    "url": product_url,
                    "in_stock": product_info["in_stock"]
                })
            else:
                prev_info = previous_products[product_url]
                current_products[product_url]["last_alerted"] = prev_info.get("last_alerted")
                
                category_state = product_info.get("category_state", "unknown")
                prev_in_stock = prev_info.get("in_stock", False)
                
                # Update in_stock based on category state:
                # - "out" on category page = genuinely out of stock, set False
                # - "in"/"preorder" = potentially in stock, preserve previous until verified
                # - "unknown" = preserve previous state
                if category_state == "out":
                    # Category page shows OUT - product genuinely went out of stock
                    current_products[product_url]["in_stock"] = False
                else:
                    # Category shows "in", "preorder", or "unknown" - preserve previous state
                    # Verification will update to True if confirmed in stock
                    current_products[product_url]["in_stock"] = prev_in_stock
                
                # Detect potential restocks:
                # 1. Product was out of stock, now shows preorder (in_stock=True from category)
                # 2. Product was out of stock, now shows "in" on category page (needs verification)
                was_out = not prev_in_stock
                
                # Case 1: Became preorder (category marked it in_stock=True)
                if was_out and product_info["in_stock"]:
                    last_alerted = prev_info.get("last_alerted")
                    if should_alert(last_alerted):
                        changes.append({
                            "type": "restock",
                            "name": product_info["name"],
                            "url": product_url
                        })
                # Case 2: Shows "in stock" on category page - needs verification
                elif was_out and category_state == "in":
                    last_alerted = prev_info.get("last_alerted")
                    if should_alert(last_alerted):
                        changes.append({
                            "type": "restock",
                            "name": product_info["name"],
                            "url": product_url
                        })
        
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
    global CURRENT_WEBHOOK
    
    print("🚀 Starting Store Monitor Bot...")
    print(f"   Python version: {os.popen('python3 --version').read().strip()}")
    print(f"   Time: {datetime.now()}")
    print(f"   Production mode: {IS_PRODUCTION}")
    print(f"   Franchises: {', '.join(f['name'] for f in FRANCHISES)}")
    print()
    
    # Initialize database connection pool with retry for production
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
    
    # Load state from database
    state = load_state()
    direct_state = load_state()
    first_run = len(state) == 0
    
    if first_run:
        print("🆕 First run - building initial product database...")
        print("   (No alerts will be sent on first run)\n")
    
    # Print franchise summary
    for franchise in FRANCHISES:
        store_urls = load_urls(franchise["store_file"])
        direct_urls = load_urls(franchise["direct_file"])
        webhook_status = "✅" if franchise["webhook"] else "❌"
        print(f"   {franchise['name']}: {len(store_urls)} stores, {len(direct_urls)} direct products, Webhook: {webhook_status}")
    print()
    
    while True:
        cycle_start = time.time()
        total_cycle_changes = 0
        total_stats = {'fetched': 0, 'skipped': 0, 'failed': 0}
        
        # === SCAN EACH FRANCHISE ===
        for franchise in FRANCHISES:
            franchise_name = franchise["name"]
            CURRENT_WEBHOOK = franchise["webhook"]
            
            print(f"\n{'='*50}")
            print(f"📋 Scanning {franchise_name}...")
            print(f"{'='*50}")
            
            # Load URLs for this franchise
            urls = load_urls(franchise["store_file"])
            direct_urls = [normalize_product_url(u) for u in load_urls(franchise["direct_file"]) if normalize_product_url(u)]
            
            if not urls and not direct_urls:
                print(f"   No URLs configured for {franchise_name}")
                continue
            
            stats = {'fetched': 0, 'skipped': 0, 'failed': 0}
            franchise_changes = 0
            
            # === SCAN STORE PAGES ===
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
                    current_products, changes = check_store_page(url, prev_products, stats)
                    
                    state[url] = current_products
                    
                    if changes and not first_run and not silence_alerts:
                        print(f"Found {len(changes)} potential changes - verifying on product pages...")
                        for change in changes:
                            product_url = change["url"]
                            change_type = change["type"]
                            category_name = change["name"][:50]
                            
                            # Skip if already verified as out-of-stock recently
                            if is_verified_out(product_url):
                                print(f"    ⏭️ Skipping: {category_name}... (recently verified OUT)")
                                continue
                            
                            print(f"    🔍 Verifying: {category_name}...", end=" ")
                            confirmed_status, confirmed_name = confirm_product_stock(product_url)
                            
                            display_name = confirmed_name if confirmed_name else change["name"]
                            
                            # Persist verified stock state
                            if product_url in state[url]:
                                if confirmed_status in ("in", "preorder"):
                                    state[url][product_url]["in_stock"] = True
                                    clear_verified_out(product_url)  # Clear out-of-stock cache
                            
                            if confirmed_status == "in":
                                franchise_changes += 1
                                clear_verified_out(product_url)
                                if change_type == "new":
                                    message = f"🆕 **NEW PRODUCT** at {store_name}\n**{display_name}**"
                                    print(f"✅ IN STOCK!")
                                else:
                                    message = f"📦 **BACK IN STOCK** at {store_name}\n**{display_name}**"
                                    print(f"✅ RESTOCKED!")
                                send_alert(message, product_url)
                                mark_alerted(url, product_url)
                                if product_url in state[url]:
                                    state[url][product_url]["last_alerted"] = datetime.now()
                                    
                            elif confirmed_status == "preorder":
                                franchise_changes += 1
                                clear_verified_out(product_url)
                                message = f"📋 **PREORDER AVAILABLE** at {store_name}\n**{display_name}**"
                                print(f"📋 PREORDER!")
                                send_alert(message, product_url)
                                mark_alerted(url, product_url)
                                if product_url in state[url]:
                                    state[url][product_url]["last_alerted"] = datetime.now()
                                    
                            elif confirmed_status == "out":
                                mark_verified_out(product_url)  # Cache as verified OUT
                                print(f"❌ OUT OF STOCK (no alert)")
                                    
                            else:
                                mark_verified_out(product_url)  # Also cache unknown to prevent spam
                                print(f"❓ UNKNOWN (no alert)")
                            
                            time.sleep(1)
                    else:
                        print(f"OK ({len(current_products)} products)")
                    
                    time.sleep(random.uniform(2, 4))
            
            # === SCAN DIRECT PRODUCTS ===
            if direct_urls:
                print(f"\n🎯 Checking {len(direct_urls)} {franchise_name} direct products...")
                for url in direct_urls:
                    store_name = urlparse(url).netloc
                    print(f"  Checking: {store_name}...", end=" ")
                    
                    silence_alerts = should_silence_first_run(url)
                    prev_state = direct_state.get(url)
                    current_state, change = check_direct_product(url, prev_state, stats)
                    
                    if silence_alerts:
                        mark_url_initialized(url)
                        if current_state:
                            current_state["last_alerted"] = datetime.now()
                            direct_state[url] = current_state
                        else:
                            direct_state[url] = {"name": "", "in_stock": False, "stock_status": "out", "last_alerted": datetime.now()}
                        print(f"🆕 First scan, alerts silenced")
                        continue
                    
                    if current_state:
                        direct_state[url] = current_state
                        detailed_status = current_state.get("stock_status", "unknown").upper()
                        
                        if change and not first_run and not silence_alerts:
                            print(f"🔍 Potential {change.get('type', 'change')} - verifying...", end=" ")
                            time.sleep(1)
                            verified_state, _ = check_direct_product(url, direct_state.get(url), stats)
                            if verified_state:
                                verified_status = verified_state.get("stock_status", "unknown")
                                
                                # Persist verified stock state
                                if verified_status in ("in", "preorder"):
                                    direct_state[url]["in_stock"] = True
                                else:
                                    direct_state[url]["in_stock"] = False
                                
                                if verified_status in ("in", "preorder"):
                                    franchise_changes += 1
                                    change_type = change.get("type", "restock")
                                    
                                    if change_type == "preorder" or verified_status == "preorder":
                                        message = f"📋 **PREORDER AVAILABLE** at {store_name}\n**{change['name']}**"
                                        print(f"✅ PREORDER CONFIRMED!")
                                    else:
                                        message = f"📦 **BACK IN STOCK** at {store_name}\n**{change['name']}**"
                                        print(f"✅ RESTOCK CONFIRMED!")
                                    
                                    send_alert(message, change["url"])
                                    direct_state[url]["last_alerted"] = datetime.now()
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
            
            # Franchise summary
            print(f"\n📊 {franchise_name}: {franchise_changes} alerts sent")
            print(f"   Stats: {stats['fetched']} fetched, {stats['skipped']} skipped, {stats['failed']} failed")
            
            total_cycle_changes += franchise_changes
            total_stats['fetched'] += stats['fetched']
            total_stats['skipped'] += stats['skipped']
            total_stats['failed'] += stats['failed']
        
        save_state(state)
        
        # === CYCLE SUMMARY ===
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
