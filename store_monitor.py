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


# =========================================================
#  PLAYWRIGHT (optional but supported)
# =========================================================
PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False


# =========================================================
#  DECODO PROXY SUPPORT (requests + optional Playwright proxy)
#  Put domains here ONLY if they need proxying (418/403/429 etc).
# =========================================================
DECODO_BLOCKED_DOMAINS = {
    "thetoyplanet.co.uk",
    # If John Lewis blocks your IP, add it here:
    "johnlewis.com",
    # Add more as you find them blocked, e.g.:
    # "very.co.uk",
    # "argos.co.uk",
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
    """
    Requests proxies dict for domains in DECODO_BLOCKED_DOMAINS.
    Returns None if domain is not in list or creds missing.
    """
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
    return {"http": proxy, "https": proxy}


def playwright_proxy_for_url(url: str):
    """
    Playwright proxy settings for domains in DECODO_BLOCKED_DOMAINS.
    Playwright expects server/username/password, not a URL with creds.
    """
    host = _host_for_url(url)
    if not host or host not in DECODO_BLOCKED_DOMAINS:
        return None

    user = os.getenv("DECODO_USER")
    pw = os.getenv("DECODO_PASS")
    proxy_host = os.getenv("DECODO_HOST", "proxy.decodo.com")
    proxy_port = os.getenv("DECODO_PORT", "10000")

    if not user or not pw:
        return None

    return {
        "server": f"http://{proxy_host}:{proxy_port}",
        "username": user,
        "password": pw,
    }


# =========================================================
#  PLAYWRIGHT DOMAINS (JS-heavy / bot-protected)
# =========================================================
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


# =========================================================
#  DATABASE CONNECTION POOL
# =========================================================
DB_POOL = None
MAX_TIMEOUT = 60
IS_PRODUCTION = os.getenv("REPLIT_DEPLOYMENT") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")


# =========================================================
#  FRANCHISE CONFIGURATIONS
# =========================================================
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


# =========================================================
#  SESSION (requests) + retries/backoff
# =========================================================
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET", "POST"])
)
adapter = HTTPAdapter(max_retries=retry_strategy)

SESSION = requests.Session()
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)


# =========================================================
#  HEADERS: desktop/mobile (toggle each cycle) + realistic UAs
# =========================================================
REAL_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64
