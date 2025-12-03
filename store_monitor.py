import requests
from bs4 import BeautifulSoup
import time
import os
import random
import json
import re
from urllib.parse import urljoin, urlparse

DISCORD_WEBHOOK = os.getenv("STOCK")
CHECK_INTERVAL = 10
STATE_FILE = "product_state.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
}

MOBILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
}

OUT_OF_STOCK_TERMS = [
    "out of stock", "sold out", "unavailable", "notify when available",
    "currently unavailable", "temporarily out of stock", "pre-order",
    "coming soon", "not in stock", "no stock available", "stock: 0",
    "notify me when in stock", "out-of-stock", "soldout", "backorder",
    "back order", "waitlist", "wait list", "notify me", "email when available"
]

IN_STOCK_TERMS = [
    "add to cart", "add to basket", "add to bag", "add to trolley",
    "add to order", "in stock", "available", "available now",
    "available to buy", "buy now", "order now", "item in stock",
    "stock available", "stock: available", "instock", "in-stock",
    "add to shopping bag", "add to shopping cart", "purchase now",
    "shop now", "get it now", "ready to ship", "ships today",
    "in stock now", "hurry", "only a few left", "low stock",
    "limited stock", "few remaining"
]

def load_urls(file_path="Websites.txt"):
    try:
        with open(file_path, "r") as f:
            return [line.strip() for line in f if line.strip() and line.startswith("http")]
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return []

def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def get_headers_for_url(url):
    if "very.co.uk" in url or "freemans.com" in url or "jdwilliams.co.uk" in url or "jacamo.co.uk" in url:
        return MOBILE_HEADERS
    return HEADERS

def send_alert(message, url):
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
        
        has_out_of_stock = any(term in card_text for term in OUT_OF_STOCK_TERMS)
        has_in_stock = any(term in card_text for term in IN_STOCK_TERMS)
        
        if has_out_of_stock:
            is_in_stock = False
        elif has_in_stock:
            is_in_stock = True
        else:
            is_in_stock = True
        
        if full_url not in products:
            products[full_url] = {
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

def check_store_page(url, previous_products):
    try:
        headers = get_headers_for_url(url)
        r = requests.get(url, headers=headers, timeout=30)
        
        if r.status_code != 200:
            print(f"⚠️ Failed to fetch {url} (status {r.status_code})")
            return previous_products, []
        
        soup = BeautifulSoup(r.text, "html.parser")
        current_products = extract_products(soup, url)
        
        changes = []
        
        for product_url, product_info in current_products.items():
            if product_url not in previous_products:
                changes.append({
                    "type": "new",
                    "name": product_info["name"],
                    "url": product_url,
                    "in_stock": product_info["in_stock"]
                })
            else:
                prev_info = previous_products[product_url]
                if not prev_info.get("in_stock", False) and product_info["in_stock"]:
                    changes.append({
                        "type": "restock",
                        "name": product_info["name"],
                        "url": product_url
                    })
        
        return current_products, changes
        
    except Exception as e:
        print(f"❌ Error checking {url}: {e}")
        return previous_products, []

def main():
    print("🚀 Starting Store Monitor Bot...")
    print(f"📋 Loading URLs from Websites.txt...")
    
    urls = load_urls()
    if not urls:
        print("No URLs to check. Add URLs to Websites.txt")
        return
    
    print(f"Found {len(urls)} store pages to monitor")
    print(f"Check interval: {CHECK_INTERVAL} seconds\n")
    
    state = load_state()
    first_run = len(state) == 0
    
    if first_run:
        print("🆕 First run - building initial product database...")
        print("   (No alerts will be sent on first run)\n")
    
    while True:
        print(f"🔍 Scanning {len(urls)} store pages...")
        total_changes = 0
        
        for url in urls:
            store_name = urlparse(url).netloc
            print(f"  Checking: {store_name}...", end=" ")
            
            prev_products = state.get(url, {})
            current_products, changes = check_store_page(url, prev_products)
            
            state[url] = current_products
            
            if changes and not first_run:
                print(f"Found {len(changes)} changes!")
                for change in changes:
                    total_changes += 1
                    if change["type"] == "new":
                        stock_status = "✅ In Stock" if change["in_stock"] else "❌ Out of Stock"
                        message = f"🆕 **NEW PRODUCT** at {store_name}\n**{change['name']}**\nStatus: {stock_status}"
                        print(f"    🆕 NEW: {change['name'][:50]}")
                        send_alert(message, change["url"])
                    elif change["type"] == "restock":
                        message = f"📦 **BACK IN STOCK** at {store_name}\n**{change['name']}**"
                        print(f"    📦 RESTOCK: {change['name'][:50]}")
                        send_alert(message, change["url"])
                    time.sleep(1)
            else:
                print(f"OK ({len(current_products)} products)")
            
            time.sleep(random.uniform(2, 4))
        
        save_state(state)
        
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
        
        print(f"⏱️ Next scan in {CHECK_INTERVAL} seconds...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
