import requests
from bs4 import BeautifulSoup
import time
import os
import random
import json
import re
import psycopg2
from urllib.parse import urljoin, urlparse, urlunparse
from datetime import datetime, timedelta

DISCORD_WEBHOOK = os.getenv("STOCK")
IS_PRODUCTION = os.getenv("REPLIT_DEPLOYMENT") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

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

# URL patterns that indicate JavaScript-only pages
JS_URL_PATTERNS = ['#/', 'dffullscreen', '?view=ajax', 'doofinder']

# HTML content that indicates JS-only rendering
JS_PAGE_INDICATORS = ['enable javascript', 'javascript is required', 'doofinder', 
                       'please enable javascript', 'browser does not support']

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

def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(DATABASE_URL)

def init_database():
    """Initialize the database table for product state."""
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
        conn.close()
        print("✅ Database initialized")
        return True
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

ALERT_COOLDOWN_HOURS = 8

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

OUT_OF_STOCK_TERMS = frozenset([
    "out of stock", "sold out", "unavailable", "notify when available",
    "currently unavailable", "temporarily out of stock", "pre-order",
    "coming soon", "not in stock", "no stock available", "stock: 0",
    "notify me when in stock", "out-of-stock", "soldout", "backorder",
    "back order", "waitlist", "wait list", "notify me", "email when available",
    "outofstock", "schema.org/outofstock", "check back soon", "sold-out-btn"
])

IN_STOCK_TERMS = frozenset([
    "add to cart", "add to basket", "add to bag", "add to trolley",
    "add to order", "in stock", "available", "available now",
    "available to buy", "buy now", "order now", "item in stock",
    "stock available", "stock: available", "instock", "in-stock",
    "add to shopping bag", "add to shopping cart", "purchase now",
    "shop now", "get it now", "ready to ship", "ships today",
    "in stock now", "hurry", "only a few left", "low stock",
    "limited stock", "few remaining"
])

# === OPTIMIZATION: Precompiled regex for stock term matching ===
OUT_OF_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in OUT_OF_STOCK_TERMS), re.IGNORECASE)
IN_STOCK_PATTERN = re.compile('|'.join(re.escape(term) for term in IN_STOCK_TERMS), re.IGNORECASE)

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

def load_urls(file_path="Websites.txt"):
    try:
        with open(file_path, "r") as f:
            return [line.strip() for line in f if line.strip() and line.startswith("http")]
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return []

def load_state():
    """Load product state from database."""
    state = {}
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set! Cannot load state.")
        return state
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
        conn.close()
        print(f"📂 Database: Loaded {len(rows)} products from {len(state)} stores")
    except Exception as e:
        print(f"❌ Error loading state from database: {e}")
        print("   Bot will treat this as first run - no alerts will be sent")
    return state

def save_product(store_url, product_url, product_name, in_stock):
    """Save a single product to the database."""
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
        conn.close()
        return True
    except Exception as e:
        print(f"⚠️ Error saving product: {e}")
        return False

def mark_alerted(store_url, product_url):
    """Update last_alerted timestamp for a product."""
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
        conn.close()
    except Exception as e:
        print(f"⚠️ Error marking alerted: {e}")

def should_alert(last_alerted):
    """Check if enough time has passed since last alert (8 hour cooldown)."""
    if last_alerted is None:
        return True
    now = datetime.now()
    cooldown = timedelta(hours=ALERT_COOLDOWN_HOURS)
    return (now - last_alerted) > cooldown

def get_last_alerted_from_db(product_url):
    """Check database for last_alerted timestamp of a product (for anti-flicker)."""
    if not DATABASE_URL:
        return None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT last_alerted FROM product_state 
            WHERE product_url = %s AND last_alerted IS NOT NULL
            LIMIT 1
        """, (product_url,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        return None

def save_state(state):
    """Save entire state to database using batch insert for speed."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        batch = []
        for store_url, products in state.items():
            for product_url, product_data in products.items():
                if isinstance(product_data, dict):
                    batch.append((store_url, product_url, product_data.get("name"), product_data.get("in_stock", True)))
                else:
                    batch.append((store_url, product_url, None, True))
        
        if batch:
            from psycopg2.extras import execute_values
            execute_values(cur, """
                INSERT INTO product_state (store_url, product_url, product_name, in_stock, last_seen)
                VALUES %s
                ON CONFLICT (store_url, product_url) 
                DO UPDATE SET product_name = EXCLUDED.product_name, 
                              in_stock = EXCLUDED.in_stock,
                              last_seen = CURRENT_TIMESTAMP
            """, batch, template="(%s, %s, %s, %s, CURRENT_TIMESTAMP)")
            conn.commit()
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Error saving state: {e}")

def get_headers_for_url(url):
    mobile_sites = [
        "very.co.uk", "freemans.com", "jdwilliams.co.uk", "jacamo.co.uk",
        "gameon.games", "hillscards.co.uk", "hmv.com", "game.co.uk",
        "johnlewis.com", "hamleys.com"
    ]
    if any(site in url for site in mobile_sites):
        return MOBILE_HEADERS
    return HEADERS

def send_alert(message, url):
    if not IS_PRODUCTION:
        print(f"📋 [DEV MODE - no Discord ping]")
        return
    
    if not DISCORD_WEBHOOK:
        print("Warning: STOCK webhook not configured")
        return
    
    data = {
        "content": f"🚨 **STOCK ALERT** 🚨\n{message}\n{url}"
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=data, timeout=10)
        if r.status_code == 204:
            print(f"✅ Alert sent!")
        else:
            print(f"⚠️ Failed to send alert ({r.status_code})")
    except Exception as e:
        print(f"❌ Discord error: {e}")

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
    products = {}
    
    container = find_product_container(soup)
    if container:
        product_links = container.find_all('a', href=True)
    else:
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
            title_elem = link.find(['h2', 'h3', 'h4', 'span', 'p'])
            if title_elem:
                product_name = title_elem.get_text(strip=True)
        
        if not product_name:
            product_name = link.get_text(strip=True)
        
        if not product_name or len(product_name) < 3:
            continue
        
        product_name = product_name[:100]
        
        card_text = get_product_card_text(link)
        
        # Use precompiled regex patterns for faster matching
        has_out_of_stock = bool(OUT_OF_STOCK_PATTERN.search(card_text))
        has_in_stock = bool(IN_STOCK_PATTERN.search(card_text))
        
        if has_out_of_stock:
            is_in_stock = False
        elif has_in_stock:
            is_in_stock = True
        else:
            is_in_stock = True
        
        normalized_url = normalize_product_url(full_url)
        if normalized_url and normalized_url not in products:
            products[normalized_url] = {
                "name": product_name,
                "in_stock": is_in_stock
            }
    
    return products

def get_category_id_from_url(url):
    match = re.search(r'-c-(\d+(?:_\d+)*)', url)
    if match:
        return match.group(1)
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
    """Check a direct product URL for stock status."""
    # Check failure cooldown
    if is_site_in_failure_cooldown(url):
        print(f"⏭️ SKIPPED (failed recently)")
        stats['skipped'] += 1
        return previous_state, None
    
    try:
        headers = get_headers_for_url(url)
        timeout = get_timeout_for_url(url)
        r = SESSION.get(url, headers=headers, timeout=timeout)
        stats['fetched'] += 1
        
        if r.status_code != 200:
            print(f"⚠️ Failed ({r.status_code})")
            mark_site_failed(url)
            stats['failed'] += 1
            return previous_state, None
        
        soup = BeautifulSoup(r.text, "html.parser")
        page_text = soup.get_text().lower()
        
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
        
        # Check stock status using precompiled regex patterns
        has_out_of_stock = bool(OUT_OF_STOCK_PATTERN.search(page_text))
        has_in_stock = bool(IN_STOCK_PATTERN.search(page_text))
        
        # For direct products: if "add to basket/cart" is found, it's in stock
        # This takes priority over other out-of-stock text that might be on the page
        if has_in_stock:
            is_in_stock = True
        elif has_out_of_stock:
            is_in_stock = False
        else:
            is_in_stock = True  # Assume in stock if unclear
        
        current_state = {
            "name": product_name,
            "in_stock": is_in_stock,
            "last_alerted": previous_state.get("last_alerted") if previous_state else None
        }
        
        # Detect restock (was out of stock, now in stock)
        change = None
        if previous_state:
            was_in_stock = previous_state.get("in_stock", False)
            if not was_in_stock and is_in_stock:
                last_alerted = previous_state.get("last_alerted")
                if should_alert(last_alerted):
                    change = {
                        "type": "restock",
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
        print(f"❌ Error: {e}")
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
        r = SESSION.get(url, headers=headers, timeout=timeout)
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
        
        is_shopify_antibot = any(store in url for store in SHOPIFY_STORES_WITH_ANTIBOT)
        
        if is_shopify_antibot and prev_count > 0 and curr_count < prev_count:
            print(f"⚠️ BLOCKED ({curr_count} vs {prev_count} cached, Shopify anti-bot)")
            return previous_products, []
        
        if prev_count >= 5 and curr_count < prev_count * 0.3:
            print(f"⚠️ BLOCKED ({curr_count} products vs {prev_count} cached, keeping cache)")
            return previous_products, []
        
        changes = []
        
        for product_url, product_info in current_products.items():
            if product_url not in previous_products:
                # Check if this product was seen before in the database (returning product)
                # This prevents spam when products flicker due to anti-bot
                db_last_alerted = get_last_alerted_from_db(product_url)
                if db_last_alerted and not should_alert(db_last_alerted):
                    # Product was alerted recently, skip (anti-flicker protection)
                    current_products[product_url]["last_alerted"] = db_last_alerted
                    continue
                changes.append({
                    "type": "new",
                    "name": product_info["name"],
                    "url": product_url,
                    "in_stock": product_info["in_stock"]
                })
            else:
                prev_info = previous_products[product_url]
                current_products[product_url]["last_alerted"] = prev_info.get("last_alerted")
                if not prev_info.get("in_stock", False) and product_info["in_stock"]:
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
        print(f"❌ Error: {e}")
        mark_site_failed(url)
        stats['failed'] += 1
        return previous_products, []

def main():
    print("🚀 Starting Store Monitor Bot...")
    
    if not init_database():
        print("❌ Failed to initialize database. Exiting.")
        return
    
    print(f"📋 Loading URLs from Websites.txt...")
    
    urls = load_urls()
    if not urls:
        print("No URLs to check. Add URLs to Websites.txt")
        return
    
    direct_urls = load_urls("DirectProducts.txt")
    
    print(f"Found {len(urls)} store pages to monitor")
    if direct_urls:
        print(f"Found {len(direct_urls)} direct products to monitor")
    print(f"Check interval: {CHECK_INTERVAL} seconds\n")
    
    state = load_state()
    direct_state = {}  # Track direct product states
    first_run = len(state) == 0
    
    if first_run:
        print("🆕 First run - building initial product database...")
        print("   (No alerts will be sent on first run)\n")
    
    while True:
        # === OPTIMIZATION: Cycle stats tracking ===
        stats = {'fetched': 0, 'skipped': 0, 'failed': 0}
        cycle_start = time.time()
        
        print(f"🔍 Scanning {len(urls)} store pages...")
        total_changes = 0
        
        for url in urls:
            store_name = urlparse(url).netloc
            print(f"  Checking: {store_name}...", end=" ")
            
            prev_products = state.get(url, {})
            current_products, changes = check_store_page(url, prev_products, stats)
            
            state[url] = current_products
            
            if changes and not first_run:
                print(f"Found {len(changes)} changes!")
                for change in changes:
                    total_changes += 1
                    if change["type"] == "new":
                        if change.get("in_stock", False):
                            message = f"🆕 **NEW PRODUCT** at {store_name}\n**{change['name']}**"
                            print(f"    🆕 NEW: {change['name'][:50]}")
                            send_alert(message, change["url"])
                            mark_alerted(url, change["url"])
                        else:
                            print(f"    ⏸️ NEW (out of stock, no alert): {change['name'][:50]}")
                    elif change["type"] == "restock":
                        message = f"📦 **BACK IN STOCK** at {store_name}\n**{change['name']}**"
                        print(f"    📦 RESTOCK: {change['name'][:50]}")
                        send_alert(message, change["url"])
                        mark_alerted(url, change["url"])
                    time.sleep(1)
            else:
                print(f"OK ({len(current_products)} products)")
            
            time.sleep(random.uniform(2, 4))
        
        # Check direct product URLs
        if direct_urls:
            print(f"\n🎯 Checking {len(direct_urls)} direct products...")
            for url in direct_urls:
                store_name = urlparse(url).netloc
                print(f"  Checking: {store_name}...", end=" ")
                
                prev_state = direct_state.get(url)
                current_state, change = check_direct_product(url, prev_state, stats)
                
                if current_state:
                    direct_state[url] = current_state
                    stock_status = "IN STOCK" if current_state.get("in_stock") else "OUT OF STOCK"
                    
                    if change and not first_run:
                        total_changes += 1
                        message = f"📦 **BACK IN STOCK** at {store_name}\n**{change['name']}**"
                        print(f"🔔 RESTOCK!")
                        send_alert(message, change["url"])
                        # Update last_alerted in direct_state
                        direct_state[url]["last_alerted"] = datetime.now()
                        # Also save to database
                        save_product(url, url, current_state.get("name"), current_state.get("in_stock"))
                        mark_alerted(url, url)
                    else:
                        print(f"{stock_status}")
                    
                    # Save state to database
                    save_product(url, url, current_state.get("name"), current_state.get("in_stock"))
                
                time.sleep(random.uniform(2, 4))
        
        save_state(state)
        
        # === OPTIMIZATION: Cycle stats summary ===
        cycle_time = round(time.time() - cycle_start, 1)
        
        if first_run:
            total_products = sum(len(products) for products in state.values())
            print(f"\n✅ Initial scan complete! Tracking {total_products} products across {len(urls)} stores")
            print("   Future changes will trigger Discord alerts.\n")
            first_run = False
        else:
            if total_changes > 0:
                print(f"\n📊 Scan complete. {total_changes} changes detected and alerted.")
            else:
                print(f"\n📊 Scan complete. No changes detected.")
        
        # Print cycle stats
        print(f"📈 Stats: {stats['fetched']} fetched, {stats['skipped']} skipped, {stats['failed']} failed | Cycle: {cycle_time}s")
        if FAILED_SITES:
            print(f"   ⏸️ {len(FAILED_SITES)} sites in failure cooldown")
        if JS_SKIP_CACHE:
            print(f"   🚫 {len(JS_SKIP_CACHE)} JS-only pages in skip cache")
        
        print(f"⏱️ Next scan in {CHECK_INTERVAL} seconds...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
