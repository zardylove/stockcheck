# Overview

This is a Python-based stock monitoring system that tracks Pokemon Trading Card Game (TCG) product availability across multiple UK retailers. The application scrapes store category pages to detect new products and stock changes, sending real-time Discord notifications.

## Main Bot: store_monitor.py

The primary bot (`store_monitor.py`) monitors 91 UK Pokemon TCG store pages from `Websites.txt`:
- Detects **new products** added to store pages
- Detects **restocks** (products changing from out-of-stock to in-stock)
- Sends Discord alerts via the `STOCK` webhook
- Stores product state in **PostgreSQL database** (persists across republishes)
- Runs continuously with no delay between scan cycles
- **TCG-only filtering**: Filters out plushies, toys, figures, and non-card products
- Discord alerts work in both development and production modes

### Performance Optimizations (Dec 2024)
- **Session reuse**: Single `requests.Session()` for connection pooling
- **Request timeout**: 15 seconds (30s for slow sites), max 60s cap
- **Failed site cache**: Skip sites for 3 minutes after failure
- **JS page detection**: Skip JavaScript-only pages for 5 minutes
- **Precompiled regex**: Stock term matching uses compiled patterns
- **Cycle stats**: Logs fetched/skipped/failed counts and cycle time
- **Database connection pool**: Reuses connections instead of opening/closing for each query
- **Traceback logging**: Full stack traces on errors for easier debugging

## Deployment

Configured for Reserved VM deployment ($20/month with Replit Core credits):
- Run command: `python3 store_monitor.py`
- Runs 24/7 on always-on cloud server

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Core Design Pattern

**Store Page Monitoring**: The main bot (`store_monitor.py`) monitors entire store category pages rather than individual product URLs:
- Extracts all product links from each store page
- Compares against previous scan to detect changes
- Alerts on new products AND restocks

**URL Files**:
- `Websites.txt` - Store category pages (48 UK retailers)
- `DirectProducts.txt` - Individual product URLs for direct stock monitoring
- Legacy: `151bb.txt`, `prisbb.txt`, etc. - Specific product URLs

**Adding New URLs**: Simply edit the text files - one URL per line. The bot will pick them up on the next scan.

## Direct Product Monitoring

For stores that don't have proper category pages (e.g., Saturn Magic uses JavaScript-based search), you can add direct product URLs to `DirectProducts.txt`. The bot will:
- Check each product page directly for stock status
- Alert when a product changes from out-of-stock to in-stock
- Uses different detection priority: "Add to Basket" takes precedence over other text on the page

## Web Scraping Strategy

**Dual User-Agent Approach**: The system uses both desktop and mobile browser user-agents, with site-specific selection logic.

**Implementation**: 
- Default: Desktop Chrome user-agent for most sites
- Fallback: Mobile Safari user-agent for stricter sites (very.co.uk, freemans.com, etc.)

**Rationale**: Some e-commerce sites have different bot protection or rendering logic based on the client type. Mobile user-agents often face less aggressive blocking.

## Stock Detection Logic (Dec 2024 Update)

**Two-Stage Verification**: To eliminate false positives, the bot now uses a two-stage approach:
1. **Category page scan**: Detects potential new products or restocks
2. **Product page confirmation**: Before alerting, fetches the actual product page to verify stock status

**classify_stock() Function**: Returns one of four states:
- `preorder` - Product available for pre-order (ALERTS)
- `in` - Product in stock (ALERTS)
- `out` - Product out of stock (NO ALERT)
- `unknown` - Cannot determine status (NO ALERT - conservative approach)

**Detection Priority** (checked in order):
1. If PREORDER term found (and no OUT term) → `preorder`
2. If OUT_OF_STOCK term found → `out`
3. If IN_STOCK term found → `in`
4. If none found → `unknown`

**PREORDER_TERMS** (alerts sent - valuable products):
- pre-order, preorder, pre order, expected
- releasing, available from, releases on, preorder now

**OUT_OF_STOCK_TERMS** (no alert):
- sold out, out of stock, unavailable, notify when available
- temporarily out of stock, not in stock, check back soon
- soldout, backorder, notify me, email when available

**IN_STOCK_TERMS** (alert after confirmation):
- add to cart, add to basket, add to bag, buy now
- in stock, available now, order now, ready to ship

**Key Improvement**: Unlike before, the bot no longer assumes "in stock" when uncertain. It now requires positive confirmation from the product page before sending any alert.

**Rationale**: Eliminates false positives from JavaScript template code (like `productFormAddToCart: "Add to cart"`) that appears on pages even when products are sold out.

## State Management

**PostgreSQL Database Persistence** (store_monitor.py): Uses PostgreSQL database to track which products have been seen before to avoid duplicate alerts.

**Implementation**:
- Table: `product_state` with store_url, product_url, product_name, in_stock, first_seen, last_seen
- Tracks product URLs and stock status
- Persists across republishes and restarts (solves the duplicate alert problem)

**Rationale**: Prevents alert fatigue by only notifying on genuine stock changes. Database storage ensures state persists across deployments unlike file-based storage which resets on republish.

## Notification System

**Discord Webhook Integration**: All alerts are sent via Discord webhooks rather than email, SMS, or other channels.

**Message Format**:
```
🚨 **STOCK ALERT** 🚨
Product may be in stock:
[URL]
```

**Rationale**: Discord webhooks are:
- Free and unlimited
- Real-time delivery
- Easy to integrate (single HTTP POST)
- Mobile notification support
- No authentication complexity

## Configuration Management

**Environment Variables for Secrets**: Discord webhook URLs are stored in environment variables rather than hardcoded.

**Per-Script Webhooks**:
- `_151_BB` - Pokemon 151 Booster Bundle
- `PRIS_BB` - Prismatic Evolutions Booster Bundle
- `PRIS_SB` - Prismatic Evolutions Surprise Box
- `PRIS_SPC` - Prismatic Evolutions Super Premium Collection
- `STOCK` - General store monitoring

**Rationale**: Allows different Discord channels for different product types, enabling users to subscribe to specific product categories.

## Monitoring Intervals

**Fixed Polling Intervals**: Scripts use time.sleep() with hardcoded intervals (10-120 seconds).

**Current Implementation**:
- Product-specific monitors: 10 seconds
- Store-wide monitors: 120 seconds

**Rationale**: Balance between:
- Rapid detection (shorter intervals)
- Avoiding rate limiting/IP bans (longer intervals)
- Resource consumption

**Alternative Considered**: Dynamic intervals based on time of day or historical stock patterns - not implemented for simplicity.

## URL Management

**Text File-Based URL Lists**: Each monitor reads URLs from a corresponding .txt file.

**Format**: One URL per line, whitespace trimmed

**Rationale**: 
- Non-technical users can edit URLs
- Version control friendly
- No database setup required
- Easy bulk updates

# External Dependencies

## Third-Party Libraries

**requests** - HTTP client for web scraping
- Purpose: Making HTTP requests to retailer websites
- Usage: Fetching product pages with custom headers

**BeautifulSoup (bs4)** - HTML parser
- Purpose: Extracting text content from HTML pages
- Usage: Searching for stock availability phrases in page content

**Standard Library Dependencies**:
- `time` - Sleep intervals between checks
- `os` - Environment variable access
- `random` - Potential randomization (imported but usage unclear in provided code)
- `json` - State file persistence
- `re` - Regular expression matching (store_monitor.py)
- `urllib.parse` - URL manipulation (store_monitor.py)

## External Services

**Discord Webhooks**
- Purpose: Real-time stock notifications
- Integration: HTTP POST with JSON payload
- Authentication: Webhook URL acts as credential
- Expected Response: 204 No Content on success

## Target Websites

The application monitors 50+ UK retailers including:
- Major retailers: Argos, John Lewis, Game, HMV
- Specialist card shops: Magic Madhouse, Chaos Cards, Total Cards
- Online marketplaces: Very.co.uk, Freemans, JD Williams
- Independent shops: Various TCG and toy stores

**Note**: No official APIs are used; all data is scraped from public HTML pages.

## Infrastructure

**Deployment Environment**: Designed for Replit deployment
- Expects environment variables to be set in Replit Secrets
- No database required
- File system used for state persistence
- Continuous execution model (infinite loop with sleep)

**Resource Requirements**:
- Minimal: Simple HTTP requests and text parsing
- No GPU, heavy compute, or large memory needs
- Network-bound rather than CPU-bound