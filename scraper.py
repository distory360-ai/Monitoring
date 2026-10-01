
from __future__ import annotations

import asyncio
import csv
import datetime as dt
import gzip
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import feedparser
import httpx
import lxml.html
import trafilatura
from dateutil import parser as dateparser
from textblob import TextBlob


# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
DB_PATH = os.getenv("MP_DB_PATH", "data/mediapulse.db")
CSV_EXPORT = os.getenv("MP_CSV", "daily_news.csv")
MENTIONS_EXPORT = os.getenv("MP_MENTIONS_CSV", "data/brand_mentions.csv")
EXPORT_DAYS = int(os.getenv("MP_EXPORT_DAYS", "30"))
LOOKBACK_HOURS = int(os.getenv("MP_LOOKBACK_HOURS", "48"))
MAX_URLS_PER_SOURCE = int(os.getenv("MP_MAX_URLS_PER_SOURCE", "150"))
MAX_FETCH = int(os.getenv("MP_MAX_FETCH", "6000"))
# Time budget for one run (minutes). The crawl stops starting new fetches when it runs out,
# saves everything, and queues the unfetched URLs for the next run — so a run can never be
# killed by the CI job limit before it saves. Discovery gets its own, smaller budget.
TIME_BUDGET_MIN = float(os.getenv("MP_TIME_BUDGET_MIN", "100"))
DISCOVERY_BUDGET_MIN = float(os.getenv("MP_DISCOVERY_BUDGET_MIN", "15"))
SOURCE_DISCOVERY_TIMEOUT = float(os.getenv("MP_SOURCE_DISCOVERY_TIMEOUT", "240"))   # seconds per outlet
MAX_PER_HOST = int(os.getenv("MP_MAX_PER_HOST", "60"))          # article fetches per site per run
PENDING_MAX_AGE_DAYS = 3
MAX_SITEMAPS = int(os.getenv("MP_MAX_SITEMAPS", "6"))
CONCURRENCY = int(os.getenv("MP_CONCURRENCY", "24"))
DOMAIN_DELAY = float(os.getenv("MP_DOMAIN_DELAY", "2.0"))
REQUEST_TIMEOUT = float(os.getenv("MP_TIMEOUT", "20"))
ENABLE_GDELT = os.getenv("MP_ENABLE_GDELT", "1") == "1"
# news.google.com's robots.txt disallows /rss/search for crawlers, and this crawler obeys robots.txt,
# so Google News returns nothing. Off by default to keep the log clean.
ENABLE_GNEWS = os.getenv("MP_ENABLE_GNEWS", "0") == "1"
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")          # v3 crashed here: name was never defined

GDELT_DELAY = 10.0         # GDELT throttles shared CI IPs hard; ≥10 s between requests
GDELT_BATCH = 12           # terms OR'd per GDELT query (fewer, larger queries = fewer 429s)
GDELT_MAX_429 = 3          # consecutive rate-limit replies before GDELT is skipped for this run
MIN_TEXT_CHARS = 250       # below this, extraction probably hit a non-article page
COMMIT_EVERY = 50          # articles saved between database commits

BOT_TOKEN = "MediaPulseBot"
USER_AGENT = os.getenv(
    "MP_USER_AGENT",
    f"{BOT_TOKEN}/4.0 (+https://mediapulse.africa/bot; media monitoring)",
)

UTC = dt.timezone.utc
EPOCH = dt.datetime(1970, 1, 1, tzinfo=UTC)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
os.makedirs("data", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("data/pipeline.log"), logging.StreamHandler()],
)
log = logging.getLogger("mediapulse")
logging.getLogger("httpx").setLevel(logging.WARNING)
for _noisy in ("trafilatura", "htmldate", "courlan", "justext", "readability"):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)      # "discarding data" etc. — counted in stats instead



# ════════════════════════════════════════════════════════════════════════════
# PART 1 — CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════
# MediaPulse Africa — registries (brands, topics, sources).
#
# Edit this file, not pipeline.py, to add clients, brands or outlets.
#
# BRANDS
#   aliases : strings to match (word-boundary). Short ALL-CAPS aliases (<=5 chars,
#             e.g. "KCB", "UBA", "MTN") are matched case-sensitively so "Cuba" or
#             "tuba" never count as UBA.
#   context : optional. For ambiguous names ("Shell", "Bolt", "Glo", "Orange"),
#             at least one context word must appear in the article, or the hit
#             is discarded.
#   country : ISO-2 home market, or "PAN" for pan-African / multinational.
#   Aliases can be in any language or script — "Banque mondiale", "Benki ya Dunia",
#   "ኢትዮ ቴሌኮም", "اتصالات المغرب". Ge'ez and Arabic-script aliases are matched even with
#   prefixes attached (የኢትዮ ቴሌኮም, والبنك الدولي) and across spelling variants.
#   An English name will NOT find an article written in Amharic or Arabic script —
#   add the native-script spelling as an alias for brands that matter there.
#
# NOTE: this registry only controls which brands get per-mention rows, alerts
# and Google News / GDELT discovery on every run. It does NOT limit what can be
# searched: every article is full-text indexed, so any brand can be queried with
# `python ingest.py "Any Brand"` — and promoted to tracking with --track, which
# backfills its mentions over everything already collected.

# ─────────────────────────────────────────────────────────────
# TOPIC / CLIENT MONITORING (topic trackers, unchanged intent,
# typos fixed: "FFoundation", "Seconadry")
# ─────────────────────────────────────────────────────────────
MONITORING_TARGETS = {
    "Mastercard Foundation Africa Secondary Education": [
        "Mastercard Foundation Scholars",
        "Mastercard Foundation Scholars Program",
        "Mastercard Foundation Scholars Programme",
        "Mastercard Foundation CITL",
        "Centre for Innovative Teaching and Learning",
        "Mastercard Foundation Transitions",
        "Mastercard Foundation Secondary Education",
        "Mastercard Foundation",
    ],
    "Africa Fintech": [
        "Africa fintech", "African fintech", "mobile money", "digital payments",
        "Flutterwave", "Paystack", "OPay", "Chipper Cash", "Moniepoint",
    ],
    "Africa Tech & AI": [
        "Africa artificial intelligence", "African AI", "Africa AI",
        "Africa machine learning", "Africa tech startup", "African startup",
        "African developer",
    ],
    "Africa Health": [
        "Africa health", "malaria", "HIV", "maternal health", "vaccination",
        "KEMSA", "Africa CDC", "health system",
    ],
    "Africa Education": [
        "Africa education", "African university", "scholarship", "EdTech",
        "TVET", "youth employment",
    ],
    "Africa Climate": [
        "climate change", "flooding", "drought", "renewable energy",
        "solar power", "green economy", "carbon credits",
    ],
    "Africa Development Finance": [
        "African Development Bank", "AfDB", "World Bank", "IMF",
        "development aid", "foreign direct investment", "Afreximbank",
    ],
    "Kenya Media": [
        "Nation Media Group", "Standard Group", "Royal Media Services",
        "Kenya journalism", "press freedom", "Media Council of Kenya",
    ],
    "Africa PR & Communications": [
        "public relations", "crisis communications", "brand reputation",
        "corporate communications",
    ],
    "Kenya Politics": [
        "William Ruto", "Kenya parliament", "National Assembly", "Kenya cabinet",
        "National Treasury", "IEBC", "Finance Bill",
    ],
    "East Africa Economy": [
        "East African Community", "EAC", "Kenya economy", "Uganda economy",
        "Tanzania economy", "Rwanda economy", "Central Bank of Kenya",
    ],
}

# ─────────────────────────────────────────────────────────────
# BRAND REGISTRY
# ─────────────────────────────────────────────────────────────
BRANDS = {
    # ── KENYA ────────────────────────────────────────────────
    "Safaricom": {"country": "KE", "sector": "Telecom",
                  "aliases": ["Safaricom", "M-Pesa", "M-PESA", "MPesa", "Mpesa", "Fuliza", "ሳፋሪኮም"]},
    "Equity Group": {"country": "KE", "sector": "Banking",
                     "aliases": ["Equity Bank", "Equity Group", "Equity Group Holdings", "Equitel"]},
    "KCB Group": {"country": "KE", "sector": "Banking",
                  "aliases": ["KCB", "KCB Group", "KCB Bank", "Kenya Commercial Bank"]},
    "Co-operative Bank": {"country": "KE", "sector": "Banking",
                          "aliases": ["Co-operative Bank", "Co-op Bank", "Cooperative Bank of Kenya"]},
    "NCBA Group": {"country": "KE", "sector": "Banking",
                   "aliases": ["NCBA", "NCBA Bank", "NCBA Group"]},
    "I&M Group": {"country": "KE", "sector": "Banking",
                  "aliases": ["I&M Bank", "I&M Group"]},
    "Kenya Airways": {"country": "KE", "sector": "Aviation",
                      "aliases": ["Kenya Airways", "KQ"],
                      "context": ["airline", "flight", "flights", "aviation", "Kenya Airways", "JKIA"]},
    "Kenya Power": {"country": "KE", "sector": "Energy",
                    "aliases": ["Kenya Power", "KPLC"]},
    "KenGen": {"country": "KE", "sector": "Energy", "aliases": ["KenGen"]},
    "EABL": {"country": "KE", "sector": "FMCG",
             "aliases": ["EABL", "East African Breweries", "Kenya Breweries"]},
    "Bidco Africa": {"country": "KE", "sector": "FMCG", "aliases": ["Bidco", "Bidco Africa"]},
    "Brookside Dairy": {"country": "KE", "sector": "FMCG", "aliases": ["Brookside Dairy", "Brookside"],
                        "context": ["milk", "dairy", "farmers", "Brookside Dairy"]},
    "Britam": {"country": "KE", "sector": "Insurance", "aliases": ["Britam"]},
    "Jubilee Insurance": {"country": "KE", "sector": "Insurance",
                          "aliases": ["Jubilee Insurance", "Jubilee Holdings", "Jubilee Health Insurance"]},
    "ICEA LION": {"country": "KE", "sector": "Insurance", "aliases": ["ICEA LION", "ICEA Lion"]},
    "Nation Media Group": {"country": "KE", "sector": "Media", "aliases": ["Nation Media Group", "NMG"]},
    "Standard Group": {"country": "KE", "sector": "Media", "aliases": ["Standard Group"],
                       "context": ["Kenya", "Nairobi", "newspaper", "KTN", "media"]},
    "Royal Media Services": {"country": "KE", "sector": "Media",
                             "aliases": ["Royal Media Services", "Citizen TV"]},
    "Naivas": {"country": "KE", "sector": "Retail", "aliases": ["Naivas"]},
    "Quickmart": {"country": "KE", "sector": "Retail", "aliases": ["Quickmart"]},
    "Twiga Foods": {"country": "KE", "sector": "AgriTech", "aliases": ["Twiga Foods", "Twiga"],
                    "context": ["food", "retailers", "startup", "Twiga Foods", "distribution"]},
    "M-KOPA": {"country": "KE", "sector": "Fintech", "aliases": ["M-KOPA", "M-Kopa"]},
    "Telkom Kenya": {"country": "KE", "sector": "Telecom", "aliases": ["Telkom Kenya"]},

    # ── NIGERIA ──────────────────────────────────────────────
    "Dangote Group": {"country": "NG", "sector": "Industrial",
                      "aliases": ["Dangote", "Dangote Group", "Dangote Cement", "Dangote Refinery"]},
    "BUA Group": {"country": "NG", "sector": "Industrial", "aliases": ["BUA Group", "BUA Cement", "BUA Foods"]},
    "Globacom": {"country": "NG", "sector": "Telecom", "aliases": ["Globacom", "Glo"],
                 "context": ["telecom", "network", "subscribers", "mobile", "Globacom", "data"]},
    "Access Bank": {"country": "NG", "sector": "Banking",
                    "aliases": ["Access Bank", "Access Holdings"]},
    "GTCO": {"country": "NG", "sector": "Banking",
             "aliases": ["GTBank", "GTCO", "Guaranty Trust Bank", "Guaranty Trust Holding"]},
    "Zenith Bank": {"country": "NG", "sector": "Banking", "aliases": ["Zenith Bank"]},
    "First Bank of Nigeria": {"country": "NG", "sector": "Banking",
                              "aliases": ["First Bank of Nigeria", "FirstBank", "FBN Holdings", "FirstHoldCo"]},
    "UBA": {"country": "NG", "sector": "Banking", "aliases": ["UBA", "United Bank for Africa"]},
    "Flutterwave": {"country": "NG", "sector": "Fintech", "aliases": ["Flutterwave"]},
    "Paystack": {"country": "NG", "sector": "Fintech", "aliases": ["Paystack"]},
    "Moniepoint": {"country": "NG", "sector": "Fintech", "aliases": ["Moniepoint"]},
    "OPay": {"country": "NG", "sector": "Fintech", "aliases": ["OPay"]},
    "Interswitch": {"country": "NG", "sector": "Fintech", "aliases": ["Interswitch", "Verve"],
                    "context": ["payment", "payments", "card", "Interswitch", "fintech"]},
    "Kuda": {"country": "NG", "sector": "Fintech", "aliases": ["Kuda Bank", "Kuda"],
             "context": ["bank", "fintech", "customers", "Kuda Bank", "app"]},
    "Seplat Energy": {"country": "NG", "sector": "Energy", "aliases": ["Seplat", "Seplat Energy"]},
    "NNPC": {"country": "NG", "sector": "Energy", "aliases": ["NNPC", "NNPC Limited"]},
    "Nestle Nigeria": {"country": "NG", "sector": "FMCG", "aliases": ["Nestle Nigeria", "Nestlé Nigeria"]},

    # ── SOUTH AFRICA ─────────────────────────────────────────
    "Standard Bank": {"country": "ZA", "sector": "Banking",
                      "aliases": ["Standard Bank", "Stanbic", "Stanbic IBTC", "Stanbic Bank"]},
    "FirstRand": {"country": "ZA", "sector": "Banking",
                  "aliases": ["FirstRand", "FNB", "First National Bank"]},
    "Absa": {"country": "ZA", "sector": "Banking", "aliases": ["Absa", "Absa Group", "Absa Bank"]},
    "Nedbank": {"country": "ZA", "sector": "Banking", "aliases": ["Nedbank"]},
    "Capitec": {"country": "ZA", "sector": "Banking", "aliases": ["Capitec"]},
    "Old Mutual": {"country": "ZA", "sector": "Insurance", "aliases": ["Old Mutual"]},
    "Sanlam": {"country": "ZA", "sector": "Insurance", "aliases": ["Sanlam"]},
    "Discovery": {"country": "ZA", "sector": "Insurance",
                  "aliases": ["Discovery Limited", "Discovery Health", "Discovery Bank", "Discovery Vitality"]},
    "Vodacom": {"country": "ZA", "sector": "Telecom", "aliases": ["Vodacom", "M-Pesa Tanzania"]},
    "Telkom SA": {"country": "ZA", "sector": "Telecom", "aliases": ["Telkom SA", "Telkom"],
                  "context": ["South Africa", "SA", "JSE", "Telkom SA", "Openserve"]},
    "Naspers": {"country": "ZA", "sector": "Tech", "aliases": ["Naspers", "Prosus"]},
    "MultiChoice": {"country": "ZA", "sector": "Media",
                    "aliases": ["MultiChoice", "DStv", "GOtv", "Showmax"]},
    "Shoprite": {"country": "ZA", "sector": "Retail", "aliases": ["Shoprite", "Checkers"],
                 "context": ["retail", "retailer", "store", "stores", "supermarket", "Shoprite"]},
    "Pick n Pay": {"country": "ZA", "sector": "Retail", "aliases": ["Pick n Pay"]},
    "Woolworths SA": {"country": "ZA", "sector": "Retail", "aliases": ["Woolworths Holdings", "Woolworths"],
                      "context": ["South Africa", "Johannesburg", "Cape Town", "JSE", "rand"]},
    "Sasol": {"country": "ZA", "sector": "Energy", "aliases": ["Sasol"]},
    "Eskom": {"country": "ZA", "sector": "Energy", "aliases": ["Eskom"]},
    "Transnet": {"country": "ZA", "sector": "Logistics", "aliases": ["Transnet"]},
    "Bidcorp": {"country": "ZA", "sector": "FMCG", "aliases": ["Bidcorp", "Bid Corporation"]},
    "Takealot": {"country": "ZA", "sector": "E-commerce", "aliases": ["Takealot"]},

    # ── UGANDA ───────────────────────────────────────────────
    "Stanbic Uganda": {"country": "UG", "sector": "Banking",
                       "aliases": ["Stanbic Bank Uganda", "Stanbic Uganda", "Stanbic Uganda Holdings"]},
    "Centenary Bank": {"country": "UG", "sector": "Banking",
                       "aliases": ["Centenary Bank", "Centenary Rural Development Bank"]},
    "dfcu Bank": {"country": "UG", "sector": "Banking", "aliases": ["dfcu Bank", "dfcu Group", "DFCU"]},
    "Housing Finance Bank": {"country": "UG", "sector": "Banking", "aliases": ["Housing Finance Bank"]},
    "PostBank Uganda": {"country": "UG", "sector": "Banking", "aliases": ["PostBank Uganda", "Postbank Uganda"]},
    "Bank of Uganda": {"country": "UG", "sector": "Regulator", "aliases": ["Bank of Uganda"]},
    "MTN Uganda": {"country": "UG", "sector": "Telecom", "aliases": ["MTN Uganda", "MTN MoMo Uganda"]},
    "Airtel Uganda": {"country": "UG", "sector": "Telecom", "aliases": ["Airtel Uganda", "Airtel Money Uganda"]},
    "Lycamobile Uganda": {"country": "UG", "sector": "Telecom", "aliases": ["Lycamobile Uganda"]},
    "Umeme": {"country": "UG", "sector": "Energy", "aliases": ["Umeme"]},
    "UEDCL": {"country": "UG", "sector": "Energy",
              "aliases": ["UEDCL", "Uganda Electricity Distribution Company"]},
    "NWSC": {"country": "UG", "sector": "Utilities",
             "aliases": ["NWSC", "National Water and Sewerage Corporation"]},
    "Uganda Airlines": {"country": "UG", "sector": "Aviation", "aliases": ["Uganda Airlines"]},
    "Nile Breweries": {"country": "UG", "sector": "FMCG", "aliases": ["Nile Breweries"]},
    "Uganda Breweries": {"country": "UG", "sector": "FMCG", "aliases": ["Uganda Breweries", "UBL"],
                         "context": ["beer", "brewer", "brewery", "Uganda Breweries", "Diageo", "spirits"]},
    "Mukwano Group": {"country": "UG", "sector": "FMCG", "aliases": ["Mukwano", "Mukwano Group"]},
    "Kakira Sugar": {"country": "UG", "sector": "FMCG", "aliases": ["Kakira Sugar", "Kakira Sugar Works"]},
    "Hima Cement": {"country": "UG", "sector": "Industrial", "aliases": ["Hima Cement"]},
    "Tororo Cement": {"country": "UG", "sector": "Industrial", "aliases": ["Tororo Cement"]},
    "Roofings Group": {"country": "UG", "sector": "Industrial", "aliases": ["Roofings Group", "Roofings Limited"]},
    "SafeBoda": {"country": "UG", "sector": "Mobility", "aliases": ["SafeBoda"]},
    "Uganda Revenue Authority": {"country": "UG", "sector": "Government",
                                 "aliases": ["Uganda Revenue Authority", "URA"],
                                 "context": ["tax", "taxes", "revenue", "Uganda Revenue Authority", "customs"]},

    # ── EAST / NORTH / WEST AFRICA (non-KE/NG/ZA) ────────────
    "Ethiopian Airlines": {"country": "ET", "sector": "Aviation",
                           "aliases": ["Ethiopian Airlines", "ኢትዮጵያ አየር መንገድ"]},
    "Commercial Bank of Ethiopia": {"country": "ET", "sector": "Banking",
                                    "aliases": ["Commercial Bank of Ethiopia", "CBE", "ኢትዮጵያ ንግድ ባንክ"],
                                    "context": ["Ethiopia", "Ethiopian", "birr", "Addis", "ኢትዮጵያ", "ብር"]},
    "Ethio Telecom": {"country": "ET", "sector": "Telecom", "aliases": ["Ethio Telecom", "Telebirr", "ኢትዮ ቴሌኮም", "ቴሌብር"]},
    "RwandAir": {"country": "RW", "sector": "Aviation", "aliases": ["RwandAir"]},
    "MTN Group": {"country": "PAN", "sector": "Telecom",
                  "aliases": ["MTN", "MTN Group", "MTN Nigeria", "MTN Ghana", "MTN Uganda", "MoMo"],
                  "context": ["MTN", "telecom", "mobile money", "subscribers", "network"]},
    "Airtel Africa": {"country": "PAN", "sector": "Telecom", "aliases": ["Airtel", "Airtel Africa", "Airtel Money"]},
    "Orange Africa": {"country": "PAN", "sector": "Telecom",
                      "aliases": ["Orange Money", "Orange Middle East and Africa", "Orange Côte d'Ivoire",
                                  "Orange Sénégal", "Orange Cameroun", "Sonatel"]},
    "Wave": {"country": "SN", "sector": "Fintech", "aliases": ["Wave Mobile Money", "Wave"],
             "context": ["mobile money", "fintech", "Senegal", "Sénégal", "transfer", "paiement"]},
    "Maroc Telecom": {"country": "MA", "sector": "Telecom", "aliases": ["Maroc Telecom", "Itissalat Al-Maghrib", "اتصالات المغرب"]},
    "Attijariwafa Bank": {"country": "MA", "sector": "Banking", "aliases": ["Attijariwafa", "Attijariwafa Bank", "التجاري وفا بنك"]},
    "OCP Group": {"country": "MA", "sector": "Mining", "aliases": ["OCP Group", "OCP", "المكتب الشريف للفوسفاط"],
                  "context": ["phosphate", "fertilizer", "fertiliser", "Morocco", "Maroc", "الفوسفاط", "المغرب"]},
    "CIB Egypt": {"country": "EG", "sector": "Banking",
                  "aliases": ["Commercial International Bank", "CIB Egypt", "البنك التجاري الدولي"]},
    "Fawry": {"country": "EG", "sector": "Fintech", "aliases": ["Fawry"]},
    "Jumia": {"country": "PAN", "sector": "E-commerce", "aliases": ["Jumia"]},
    "Ecobank": {"country": "PAN", "sector": "Banking", "aliases": ["Ecobank"]},
    "Afreximbank": {"country": "PAN", "sector": "DFI", "aliases": ["Afreximbank"]},
    "African Development Bank": {"country": "PAN", "sector": "DFI",
                                 "aliases": ["African Development Bank", "AfDB", "Banque africaine de développement",
                                             "Banco Africano de Desenvolvimento", "البنك الأفريقي للتنمية"]},
    "Chipper Cash": {"country": "PAN", "sector": "Fintech", "aliases": ["Chipper Cash"]},
    "Sendwave": {"country": "PAN", "sector": "Fintech", "aliases": ["Sendwave"]},
    "Andela": {"country": "PAN", "sector": "Tech", "aliases": ["Andela"]},
    "Sun King": {"country": "PAN", "sector": "Energy", "aliases": ["Sun King", "Greenlight Planet"]},
    "Tullow Oil": {"country": "PAN", "sector": "Energy", "aliases": ["Tullow Oil", "Tullow"]},

    # ── MULTINATIONALS (Africa operations) ───────────────────
    "TotalEnergies": {"country": "PAN", "sector": "Energy", "aliases": ["TotalEnergies"]},
    "Shell": {"country": "PAN", "sector": "Energy", "aliases": ["Shell"],
              "context": ["oil", "gas", "petroleum", "Niger Delta", "refinery", "crude"]},
    "Unilever": {"country": "PAN", "sector": "FMCG", "aliases": ["Unilever"]},
    "Diageo": {"country": "PAN", "sector": "FMCG", "aliases": ["Diageo", "Guinness Nigeria"]},
    "Heineken": {"country": "PAN", "sector": "FMCG", "aliases": ["Heineken", "Nigerian Breweries"]},
    "Coca-Cola": {"country": "PAN", "sector": "FMCG",
                  "aliases": ["Coca-Cola", "Coca-Cola Beverages Africa", "CCBA"]},
    "Google": {"country": "PAN", "sector": "Tech", "aliases": ["Google"]},
    "Microsoft": {"country": "PAN", "sector": "Tech", "aliases": ["Microsoft"]},
    "Meta": {"country": "PAN", "sector": "Tech", "aliases": ["Meta Platforms", "Facebook", "WhatsApp", "Instagram"]},
    "Amazon": {"country": "PAN", "sector": "Tech", "aliases": ["Amazon Web Services", "AWS", "Amazon"],
               "context": ["cloud", "AWS", "e-commerce", "Amazon Web Services", "data centre", "data center"]},
    "Uber": {"country": "PAN", "sector": "Mobility", "aliases": ["Uber"]},
    "Bolt": {"country": "PAN", "sector": "Mobility", "aliases": ["Bolt"],
             "context": ["ride-hailing", "ride hailing", "drivers", "taxi", "app", "Bolt Food"]},

    # ── FOUNDATIONS / DEVELOPMENT ────────────────────────────
    "Mastercard Foundation": {"country": "PAN", "sector": "Philanthropy",
                              "aliases": ["Mastercard Foundation", "Fondation Mastercard", "Fundação Mastercard"]},
    "Gates Foundation": {"country": "PAN", "sector": "Philanthropy",
                         "aliases": ["Gates Foundation", "Bill & Melinda Gates Foundation"]},
    "Rockefeller Foundation": {"country": "PAN", "sector": "Philanthropy", "aliases": ["Rockefeller Foundation"]},
    "Ford Foundation": {"country": "PAN", "sector": "Philanthropy", "aliases": ["Ford Foundation"]},
    "African Wildlife Foundation": {"country": "PAN", "sector": "Philanthropy",
                                    "aliases": ["African Wildlife Foundation", "AWF"]},
    "Science for Africa Foundation": {"country": "PAN", "sector": "Philanthropy",
                                      "aliases": ["Science for Africa Foundation"]},
    "World Bank": {"country": "PAN", "sector": "DFI",
                   "aliases": ["World Bank", "Banque mondiale", "Banco Mundial", "Benki ya Dunia", "Bankin Duniya",
                               "البنك الدولي", "ዓለም ባንክ", "አለም ባንክ"]},
    "IMF": {"country": "PAN", "sector": "DFI",
            "aliases": ["IMF", "International Monetary Fund", "FMI", "Fonds monétaire international",
                        "Fundo Monetário Internacional", "صندوق النقد الدولي"]},
    "GIZ": {"country": "PAN", "sector": "Development", "aliases": ["GIZ"]},
    "FCDO": {"country": "PAN", "sector": "Development", "aliases": ["FCDO"]},
}

# ─────────────────────────────────────────────────────────────
# SOURCES — the crawler only needs the homepage. It discovers
# sitemaps (via robots.txt) and RSS/Atom feeds automatically.
#   feeds  : optional; add one when auto-discovery misses it.
#   path   : a URL with a path (e.g. bbc.com/swahili) restricts the
#            crawl to that section.
#   tier   : how much weight a mention here deserves in reports.
#            national     — major national newspaper / broadcaster / agency
#            trade        — business, sector or specialist title
#            digital      — smaller online-only outlet or blog
#            syndication  — republishes press releases / other outlets' copy
#            international— non-African outlet (Africa section only)
#   crawl  : False = don't crawl; articles from this domain that arrive
#            via GDELT / Google News / NewsAPI are still tagged with it.
# Articles from domains not listed here get tier "unlisted".
# ─────────────────────────────────────────────────────────────
SOURCES = [
    # ── Kenya ────────────────────────────────────────────────
    {"name": "Nation Africa", "url": "https://nation.africa/kenya", "country": "KE", "lang": "en",
     "tier": "national", "feeds": ["https://nation.africa/rss"]},
    {"name": "Business Daily Africa", "url": "https://www.businessdailyafrica.com", "country": "KE", "lang": "en",
     "tier": "national", "feeds": ["https://www.businessdailyafrica.com/rss"]},
    {"name": "The Standard", "url": "https://www.standardmedia.co.ke", "country": "KE", "lang": "en",
     "tier": "national", "feeds": ["https://www.standardmedia.co.ke/rss"]},
    {"name": "The Star Kenya", "url": "https://www.the-star.co.ke", "country": "KE", "lang": "en",
     "tier": "national", "feeds": ["https://www.the-star.co.ke/rss"]},
    {"name": "Capital FM Kenya", "url": "https://www.capitalfm.co.ke", "country": "KE", "lang": "en",
     "tier": "national", "feeds": ["https://www.capitalfm.co.ke/news/feed/"]},
    {"name": "Citizen Digital", "url": "https://www.citizen.digital", "country": "KE", "lang": "en", "tier": "national"},
    {"name": "Kenyans.co.ke", "url": "https://www.kenyans.co.ke", "country": "KE", "lang": "en",
     "tier": "digital", "feeds": ["https://www.kenyans.co.ke/rss.xml"]},
    {"name": "Tuko", "url": "https://www.tuko.co.ke", "country": "KE", "lang": "en", "tier": "digital"},
    {"name": "KBC", "url": "https://www.kbc.co.ke", "country": "KE", "lang": "en", "tier": "national"},
    {"name": "Kenya Wallstreet", "url": "https://kenyawallstreet.com", "country": "KE", "lang": "en",
     "tier": "trade", "feeds": ["https://kenyawallstreet.com/feed/"]},
    {"name": "The EastAfrican", "url": "https://www.theeastafrican.co.ke", "country": "KE", "lang": "en", "tier": "national"},
    {"name": "Taifa Leo", "url": "https://taifaleo.nation.co.ke", "country": "KE", "lang": "sw", "tier": "national"},
    {"name": "BBC Swahili", "url": "https://www.bbc.com/swahili", "country": "PAN", "lang": "sw", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/swahili/rss.xml"]},

    # ── Uganda ───────────────────────────────────────────────
    {"name": "Daily Monitor", "url": "https://www.monitor.co.ug", "country": "UG", "lang": "en", "tier": "national"},
    {"name": "New Vision", "url": "https://www.newvision.co.ug", "country": "UG", "lang": "en", "tier": "national"},
    {"name": "Nile Post", "url": "https://nilepost.co.ug", "country": "UG", "lang": "en", "tier": "national"},
    {"name": "NTV Uganda", "url": "https://ntv.co.ug", "country": "UG", "lang": "en", "tier": "national"},
    {"name": "The Independent Uganda", "url": "https://www.independent.co.ug", "country": "UG", "lang": "en",
     "tier": "national"},
    {"name": "PML Daily", "url": "https://pmldaily.com", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "Pulse Uganda", "url": "https://www.pulse.ug", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "SoftPower News", "url": "https://softpower.ug", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "Business Focus", "url": "https://businessfocus.co.ug", "country": "UG", "lang": "en", "tier": "trade"},
    {"name": "The Standard Uganda", "url": "https://thestandard.co.ug", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "UG Standard", "url": "https://www.ugstandard.com", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "Ugnews Line", "url": "https://www.ugnewsline.com", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "Let Out News", "url": "https://letoutnews.com", "country": "UG", "lang": "en", "tier": "digital"},

    # ── Tanzania / Rwanda / Ethiopia ─────────────────────────
    {"name": "The Citizen TZ", "url": "https://www.thecitizen.co.tz", "country": "TZ", "lang": "en", "tier": "national"},
    {"name": "Mwananchi", "url": "https://www.mwananchi.co.tz", "country": "TZ", "lang": "sw", "tier": "national"},
    {"name": "Daily News TZ", "url": "https://dailynews.co.tz", "country": "TZ", "lang": "en", "tier": "national"},
    {"name": "The New Times", "url": "https://www.newtimes.co.rw", "country": "RW", "lang": "en", "tier": "national"},
    {"name": "Addis Standard", "url": "https://addisstandard.com", "country": "ET", "lang": "en", "tier": "national"},
    {"name": "Ethio Negari", "url": "https://ethionegari.com", "country": "ET", "lang": "en", "tier": "digital"},

    # ── Nigeria ──────────────────────────────────────────────
    {"name": "Punch", "url": "https://punchng.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Premium Times", "url": "https://www.premiumtimesng.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Vanguard", "url": "https://www.vanguardngr.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "The Guardian Nigeria", "url": "https://guardian.ng", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Daily Trust", "url": "https://dailytrust.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "BusinessDay NG", "url": "https://businessday.ng", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "TheCable", "url": "https://www.thecable.ng", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Channels TV", "url": "https://www.channelstv.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Nairametrics", "url": "https://nairametrics.com", "country": "NG", "lang": "en", "tier": "trade"},
    {"name": "Realnews Magazine", "url": "https://realnewsmagazine.net", "country": "NG", "lang": "en", "tier": "trade"},
    {"name": "Ndokwa Reporters", "url": "https://www.ndokwareporters.com", "country": "NG", "lang": "en",
     "tier": "digital"},
    {"name": "World Top News Ng", "url": "https://worldtopnewsng.com", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "iReport247News", "url": "https://ireport247news.com", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Newsjaunts", "url": "https://newsjaunts.com", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "News Wings", "url": "https://newswings.com.ng", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Celebrities Arena", "url": "https://celebritiesarena.com", "country": "NG", "lang": "en", "tier": "digital"},

    # ── Ghana / Liberia ──────────────────────────────────────
    {"name": "GhanaWeb", "url": "https://www.ghanaweb.com", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "Graphic Online", "url": "https://www.graphic.com.gh", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "MyJoyOnline", "url": "https://www.myjoyonline.com", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "Citi Newsroom", "url": "https://citinewsroom.com", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "Peace FM Online", "url": "https://www.peacefmonline.com", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "DailyGuide Network", "url": "https://dailyguidenetwork.com", "country": "GH", "lang": "en",
     "tier": "national"},
    # GhanaWebbers republishes GhanaWeb copy — kept so its mentions are flagged as duplicates, not new coverage
    {"name": "GhanaWebbers", "url": "https://www.ghanawebbers.com", "country": "GH", "lang": "en",
     "tier": "syndication"},
    {"name": "The New Dawn Liberia", "url": "https://www.thenewdawnliberia.com", "country": "LR", "lang": "en",
     "tier": "national"},

    # ── Southern Africa ──────────────────────────────────────
    {"name": "News24", "url": "https://www.news24.com", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "Daily Maverick", "url": "https://www.dailymaverick.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "IOL", "url": "https://iol.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "Moneyweb", "url": "https://www.moneyweb.co.za", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "TechCentral", "url": "https://techcentral.co.za", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "MyBroadband", "url": "https://mybroadband.co.za", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "The African Business Journal", "url": "https://www.tabj.co.za", "country": "ZA", "lang": "en",
     "tier": "trade"},
    {"name": "Financial Insight Zambia", "url": "https://financialinsight.africa", "country": "ZM", "lang": "en",
     "tier": "trade"},
    {"name": "Zed Gossip", "url": "https://zedgossip.net", "country": "ZM", "lang": "en", "tier": "digital"},
    {"name": "The Projects Magazine", "url": "https://theprojectsbw.com", "country": "BW", "lang": "en", "tier": "trade"},

    # ── North Africa ─────────────────────────────────────────
    {"name": "Morocco World News", "url": "https://www.moroccoworldnews.com", "country": "MA", "lang": "en",
     "tier": "national"},
    {"name": "Hespress English", "url": "https://en.hespress.com", "country": "MA", "lang": "en", "tier": "national"},
    {"name": "Egypt Independent", "url": "https://www.egyptindependent.com", "country": "EG", "lang": "en",
     "tier": "national"},
    {"name": "Daily News Egypt", "url": "https://www.dailynewsegypt.com", "country": "EG", "lang": "en", "tier": "national"},

    # ── Francophone ──────────────────────────────────────────
    {"name": "Seneweb", "url": "https://www.seneweb.com", "country": "SN", "lang": "fr", "tier": "national"},
    {"name": "Abidjan.net", "url": "https://news.abidjan.net", "country": "CI", "lang": "fr", "tier": "national"},
    {"name": "World Canal Info", "url": "https://worldcanalinfo.com", "country": "CI", "lang": "fr", "tier": "digital"},
    {"name": "Savoir News", "url": "https://savoirnews.tg", "country": "TG", "lang": "fr", "tier": "national"},
    {"name": "L'Économiste du Togo", "url": "https://leconomistedutogo.tg", "country": "TG", "lang": "fr",
     "tier": "trade"},
    {"name": "Le Kiosque de l'Economie", "url": "https://kiosqeco.com", "country": "PAN", "lang": "fr", "tier": "trade"},
    {"name": "PAPOT.NET", "url": "https://www.papot.net", "country": "PAN", "lang": "fr", "tier": "digital"},
    {"name": "RFI Afrique", "url": "https://www.rfi.fr/fr/afrique", "country": "PAN", "lang": "fr",
     "tier": "international"},

    # ── Pan-African business, tech & specialist ──────────────
    {"name": "TechCabal", "url": "https://techcabal.com", "country": "PAN", "lang": "en",
     "tier": "trade", "feeds": ["https://techcabal.com/feed/"]},
    {"name": "TechPoint Africa", "url": "https://techpoint.africa", "country": "PAN", "lang": "en",
     "tier": "trade", "feeds": ["https://techpoint.africa/feed/"]},
    {"name": "Disrupt Africa", "url": "https://disruptafrica.com", "country": "PAN", "lang": "en",
     "tier": "trade", "feeds": ["https://disruptafrica.com/feed/"]},
    {"name": "Africanews", "url": "https://www.africanews.com", "country": "PAN", "lang": "en", "tier": "national"},
    {"name": "The Africa Report", "url": "https://www.theafricareport.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "Zawya", "url": "https://www.zawya.com/en", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "University World News", "url": "https://www.universityworldnews.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "African Travel Times", "url": "https://africantraveltimes.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "Space Watch Africa", "url": "https://spacewatchafrica.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "African Newspage", "url": "https://www.africannewspage.net", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "Discover Africa News", "url": "https://discoverafricanews.com", "country": "PAN", "lang": "en",
     "tier": "digital"},
    {"name": "Africa Bulletin", "url": "https://africabulletin.com", "country": "PAN", "lang": "en", "tier": "digital"},
    {"name": "African Peace Magazine", "url": "https://www.africanpeacemagazine.com", "country": "PAN", "lang": "en",
     "tier": "digital"},
    # APO Group's release wire: the original of most press releases that small sites republish
    {"name": "Africa Newsroom (APO)", "url": "https://www.africa-newsroom.com", "country": "PAN", "lang": "en",
     "tier": "syndication"},

    # ── African-language outlets ─────────────────────────────
    # BBC World Service African-language services (one host, separate sections)
    {"name": "BBC Hausa", "url": "https://www.bbc.com/hausa", "country": "NG", "lang": "ha", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/hausa/rss.xml"]},
    {"name": "BBC Yoruba", "url": "https://www.bbc.com/yoruba", "country": "NG", "lang": "yo", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/yoruba/rss.xml"]},
    {"name": "BBC Igbo", "url": "https://www.bbc.com/igbo", "country": "NG", "lang": "ig", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/igbo/rss.xml"]},
    {"name": "BBC Pidgin", "url": "https://www.bbc.com/pidgin", "country": "NG", "lang": "pcm", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/pidgin/rss.xml"]},
    {"name": "BBC Amharic", "url": "https://www.bbc.com/amharic", "country": "ET", "lang": "am", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/amharic/rss.xml"]},
    {"name": "BBC Afaan Oromoo", "url": "https://www.bbc.com/afaanoromoo", "country": "ET", "lang": "om",
     "tier": "international", "feeds": ["https://feeds.bbci.co.uk/afaanoromoo/rss.xml"]},
    {"name": "BBC Tigrinya", "url": "https://www.bbc.com/tigrinya", "country": "ET", "lang": "ti", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/tigrinya/rss.xml"]},
    {"name": "BBC Somali", "url": "https://www.bbc.com/somali", "country": "SO", "lang": "so", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/somali/rss.xml"]},
    {"name": "BBC Gahuza", "url": "https://www.bbc.com/gahuza", "country": "RW", "lang": "rw", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/gahuza/rss.xml"]},
    {"name": "BBC Afrique", "url": "https://www.bbc.com/afrique", "country": "PAN", "lang": "fr", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/afrique/rss.xml"]},
    {"name": "BBC Arabic", "url": "https://www.bbc.com/arabic", "country": "PAN", "lang": "ar", "tier": "international",
     "feeds": ["https://feeds.bbci.co.uk/arabic/rss.xml"]},
    {"name": "DW Kiswahili", "url": "https://www.dw.com/sw", "country": "PAN", "lang": "sw", "tier": "international"},
    # Local-language press
    {"name": "Aminiya", "url": "https://aminiya.ng", "country": "NG", "lang": "ha", "tier": "national"},
    {"name": "Addis Admass", "url": "https://www.addisadmassnews.com", "country": "ET", "lang": "am", "tier": "national"},
    {"name": "Hiiraan Online", "url": "https://www.hiiraan.com", "country": "SO", "lang": "so", "tier": "national"},
    {"name": "Kigali Today", "url": "https://www.kigalitoday.com", "country": "RW", "lang": "rw", "tier": "national"},
    {"name": "Bukedde", "url": "https://www.bukedde.co.ug", "country": "UG", "lang": "lg", "tier": "national"},
    {"name": "Isolezwe", "url": "https://isolezwe.co.za", "country": "ZA", "lang": "zu", "tier": "national"},
    {"name": "Maroela Media", "url": "https://maroelamedia.co.za", "country": "ZA", "lang": "af", "tier": "national"},
    {"name": "Kwayedza", "url": "https://www.kwayedza.co.zw", "country": "ZW", "lang": "sn", "tier": "national"},
    {"name": "Hespress (Arabic)", "url": "https://www.hespress.com", "country": "MA", "lang": "ar", "tier": "national"},
    {"name": "Youm7", "url": "https://www.youm7.com", "country": "EG", "lang": "ar", "tier": "national"},
    {"name": "Jornal de Angola", "url": "https://www.jornaldeangola.ao", "country": "AO", "lang": "pt", "tier": "national"},
    {"name": "Novo Jornal", "url": "https://novojornal.co.ao", "country": "AO", "lang": "pt", "tier": "national"},
    {"name": "O País", "url": "https://opais.co.mz", "country": "MZ", "lang": "pt", "tier": "national"},
    {"name": "Jornal Notícias", "url": "https://jornalnoticias.co.mz", "country": "MZ", "lang": "pt", "tier": "national"},

    # ══ ADDED OCT 2026 — wider coverage ══════════════════════════════════════
    # ── Kenya ────────────────────────────────────────────────
    {"name": "People Daily", "url": "https://www.pd.co.ke", "country": "KE", "lang": "en", "tier": "national"},
    {"name": "Kenya News Agency", "url": "https://www.kenyanews.go.ke", "country": "KE", "lang": "en", "tier": "national"},
    {"name": "Business Today Kenya", "url": "https://businesstoday.co.ke", "country": "KE", "lang": "en", "tier": "trade"},
    {"name": "Techweez", "url": "https://techweez.com", "country": "KE", "lang": "en", "tier": "trade"},
    {"name": "TechTrendsKE", "url": "https://techtrendske.co.ke", "country": "KE", "lang": "en", "tier": "trade"},
    {"name": "The Elephant", "url": "https://www.theelephant.info", "country": "KE", "lang": "en", "tier": "trade"},
    # ── Uganda / Tanzania / Rwanda ──────────────────────────
    {"name": "The Observer Uganda", "url": "https://observer.ug", "country": "UG", "lang": "en", "tier": "national"},
    {"name": "ChimpReports", "url": "https://chimpreports.com", "country": "UG", "lang": "en", "tier": "digital"},
    {"name": "IPP Media (The Guardian TZ)", "url": "https://www.ippmedia.com", "country": "TZ", "lang": "en",
     "tier": "national"},
    {"name": "The Chanzo", "url": "https://thechanzo.com", "country": "TZ", "lang": "en", "tier": "digital"},
    {"name": "KT Press", "url": "https://www.ktpress.rw", "country": "RW", "lang": "en", "tier": "digital"},
    {"name": "Taarifa Rwanda", "url": "https://taarifa.rw", "country": "RW", "lang": "en", "tier": "digital"},
    {"name": "IGIHE", "url": "https://igihe.com", "country": "RW", "lang": "rw", "tier": "national"},
    # ── Ethiopia / Horn / Sudans ────────────────────────────
    {"name": "The Reporter Ethiopia", "url": "https://www.thereporterethiopia.com", "country": "ET", "lang": "en",
     "tier": "national"},
    {"name": "Addis Fortune", "url": "https://addisfortune.news", "country": "ET", "lang": "en", "tier": "trade"},
    {"name": "Capital Ethiopia", "url": "https://www.capitalethiopia.com", "country": "ET", "lang": "en", "tier": "trade"},
    {"name": "Fana Broadcasting", "url": "https://www.fanabc.com", "country": "ET", "lang": "en", "tier": "national"},
    {"name": "Ethiopian News Agency", "url": "https://www.ena.et", "country": "ET", "lang": "en", "tier": "national"},
    {"name": "Garowe Online", "url": "https://www.garoweonline.com", "country": "SO", "lang": "en", "tier": "national"},
    {"name": "Horseed Media", "url": "https://horseedmedia.net", "country": "SO", "lang": "en", "tier": "digital"},
    {"name": "Radio Tamazuj", "url": "https://www.radiotamazuj.org", "country": "SS", "lang": "en", "tier": "national"},
    {"name": "Eye Radio", "url": "https://www.eyeradio.org", "country": "SS", "lang": "en", "tier": "national"},
    {"name": "Sudan Tribune", "url": "https://sudantribune.com", "country": "SD", "lang": "en", "tier": "national"},
    # ── Nigeria ──────────────────────────────────────────────
    {"name": "The Nation Nigeria", "url": "https://thenationonlineng.net", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "ThisDay", "url": "https://www.thisdaylive.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Leadership", "url": "https://leadership.ng", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Nigerian Tribune", "url": "https://tribuneonlineng.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "The Sun Nigeria", "url": "https://sunnewsonline.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Independent Nigeria", "url": "https://independent.ng", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Blueprint", "url": "https://blueprint.ng", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "P.M. News", "url": "https://pmnewsnigeria.com", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Daily Post Nigeria", "url": "https://dailypost.ng", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Sahara Reporters", "url": "https://saharareporters.com", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Ripples Nigeria", "url": "https://www.ripplesnigeria.com", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Peoples Gazette", "url": "https://gazettengr.com", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "HumAngle", "url": "https://humanglemedia.com", "country": "NG", "lang": "en", "tier": "trade"},
    {"name": "Legit.ng", "url": "https://www.legit.ng", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Pulse Nigeria", "url": "https://www.pulse.ng", "country": "NG", "lang": "en", "tier": "digital"},
    {"name": "Arise News", "url": "https://www.arise.tv", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "TVC News", "url": "https://www.tvcnews.tv", "country": "NG", "lang": "en", "tier": "national"},
    {"name": "Business Post Nigeria", "url": "https://businesspost.ng", "country": "NG", "lang": "en", "tier": "trade"},
    {"name": "Techeconomy", "url": "https://techeconomy.ng", "country": "NG", "lang": "en", "tier": "trade"},
    {"name": "Brand Communicator", "url": "https://brandcom.ng", "country": "NG", "lang": "en", "tier": "trade"},
    {"name": "Marketing Edge", "url": "https://www.marketingedge.com.ng", "country": "NG", "lang": "en", "tier": "trade"},
    # ── Ghana / Liberia / Sierra Leone / Gambia ─────────────
    {"name": "Modern Ghana", "url": "https://www.modernghana.com", "country": "GH", "lang": "en", "tier": "digital"},
    {"name": "Ghana News Agency", "url": "https://gna.org.gh", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "3News", "url": "https://3news.com", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "Adom Online", "url": "https://www.adomonline.com", "country": "GH", "lang": "en", "tier": "national"},
    {"name": "Business & Financial Times", "url": "https://thebftonline.com", "country": "GH", "lang": "en",
     "tier": "trade"},
    {"name": "Pulse Ghana", "url": "https://www.pulse.com.gh", "country": "GH", "lang": "en", "tier": "digital"},
    {"name": "Yen.com.gh", "url": "https://yen.com.gh", "country": "GH", "lang": "en", "tier": "digital"},
    {"name": "FrontPage Africa", "url": "https://frontpageafricaonline.com", "country": "LR", "lang": "en",
     "tier": "national"},
    {"name": "Liberian Observer", "url": "https://www.liberianobserver.com", "country": "LR", "lang": "en",
     "tier": "national"},
    {"name": "Awoko", "url": "https://awokonewspaper.sl", "country": "SL", "lang": "en", "tier": "national"},
    {"name": "Sierraloaded", "url": "https://www.sierraloaded.sl", "country": "SL", "lang": "en", "tier": "digital"},
    {"name": "The Point (Gambia)", "url": "https://thepoint.gm", "country": "GM", "lang": "en", "tier": "national"},
    {"name": "Foroyaa", "url": "https://foroyaa.net", "country": "GM", "lang": "en", "tier": "national"},
    # ── Francophone West & Central Africa ───────────────────
    {"name": "Dakaractu", "url": "https://www.dakaractu.com", "country": "SN", "lang": "fr", "tier": "digital"},
    {"name": "Le Soleil (Sénégal)", "url": "https://lesoleil.sn", "country": "SN", "lang": "fr", "tier": "national"},
    {"name": "APS Sénégal", "url": "https://aps.sn", "country": "SN", "lang": "fr", "tier": "national"},
    {"name": "Senego", "url": "https://senego.com", "country": "SN", "lang": "fr", "tier": "digital"},
    {"name": "Fraternité Matin", "url": "https://www.fratmat.info", "country": "CI", "lang": "fr", "tier": "national"},
    {"name": "Koaci", "url": "https://www.koaci.com", "country": "CI", "lang": "fr", "tier": "digital"},
    {"name": "Linfodrome", "url": "https://www.linfodrome.com", "country": "CI", "lang": "fr", "tier": "digital"},
    {"name": "LeFaso.net", "url": "https://lefaso.net", "country": "BF", "lang": "fr", "tier": "national"},
    {"name": "Burkina24", "url": "https://burkina24.com", "country": "BF", "lang": "fr", "tier": "digital"},
    {"name": "Maliweb", "url": "https://www.maliweb.net", "country": "ML", "lang": "fr", "tier": "digital"},
    {"name": "ActuNiger", "url": "https://www.actuniger.com", "country": "NE", "lang": "fr", "tier": "digital"},
    {"name": "La Nouvelle Tribune (Bénin)", "url": "https://lanouvelletribune.info", "country": "BJ", "lang": "fr",
     "tier": "digital"},
    {"name": "Banouto", "url": "https://www.banouto.bj", "country": "BJ", "lang": "fr", "tier": "digital"},
    {"name": "Republic of Togo", "url": "https://www.republicoftogo.com", "country": "TG", "lang": "fr",
     "tier": "national"},
    {"name": "Togo First", "url": "https://www.togofirst.com", "country": "TG", "lang": "fr", "tier": "trade"},
    {"name": "Guinéenews", "url": "https://guineenews.org", "country": "GN", "lang": "fr", "tier": "digital"},
    {"name": "Journal du Cameroun", "url": "https://www.journalducameroun.com", "country": "CM", "lang": "fr",
     "tier": "digital"},
    {"name": "Cameroon Tribune", "url": "https://www.cameroon-tribune.cm", "country": "CM", "lang": "fr",
     "tier": "national"},
    {"name": "Actu Cameroun", "url": "https://actucameroun.com", "country": "CM", "lang": "fr", "tier": "digital"},
    {"name": "Investir au Cameroun", "url": "https://www.investiraucameroun.com", "country": "CM", "lang": "fr",
     "tier": "trade"},
    {"name": "Mimi Mefo Info", "url": "https://mimimefoinfos.com", "country": "CM", "lang": "en", "tier": "digital"},
    {"name": "Actualite.cd", "url": "https://actualite.cd", "country": "CD", "lang": "fr", "tier": "national"},
    {"name": "Radio Okapi", "url": "https://www.radiookapi.net", "country": "CD", "lang": "fr", "tier": "national"},
    {"name": "7sur7.cd", "url": "https://7sur7.cd", "country": "CD", "lang": "fr", "tier": "digital"},
    {"name": "Zoom Eco", "url": "https://zoom-eco.net", "country": "CD", "lang": "fr", "tier": "trade"},
    # ── Indian Ocean ─────────────────────────────────────────
    {"name": "L'Express de Madagascar", "url": "https://lexpress.mg", "country": "MG", "lang": "fr", "tier": "national"},
    {"name": "Midi Madagasikara", "url": "https://midi-madagasikara.mg", "country": "MG", "lang": "fr",
     "tier": "national"},
    {"name": "L'Express Maurice", "url": "https://lexpress.mu", "country": "MU", "lang": "fr", "tier": "national"},
    {"name": "Defimedia", "url": "https://defimedia.info", "country": "MU", "lang": "fr", "tier": "national"},
    # ── Southern Africa ──────────────────────────────────────
    {"name": "TimesLIVE", "url": "https://www.timeslive.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "BusinessLIVE", "url": "https://www.businesslive.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "Mail & Guardian", "url": "https://mg.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "The Citizen (SA)", "url": "https://www.citizen.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "SABC News", "url": "https://www.sabcnews.com", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "eNCA", "url": "https://www.enca.com", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "SowetanLIVE", "url": "https://www.sowetanlive.co.za", "country": "ZA", "lang": "en", "tier": "national"},
    {"name": "The South African", "url": "https://www.thesouthafrican.com", "country": "ZA", "lang": "en",
     "tier": "digital"},
    {"name": "BusinessTech", "url": "https://businesstech.co.za", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "ITWeb", "url": "https://www.itweb.co.za", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "Bizcommunity", "url": "https://www.bizcommunity.com", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "Ventureburn", "url": "https://ventureburn.com", "country": "ZA", "lang": "en", "tier": "trade"},
    {"name": "Lusaka Times", "url": "https://www.lusakatimes.com", "country": "ZM", "lang": "en", "tier": "digital"},
    {"name": "Zambia Daily Mail", "url": "https://www.daily-mail.co.zm", "country": "ZM", "lang": "en", "tier": "national"},
    {"name": "News Diggers", "url": "https://diggers.news", "country": "ZM", "lang": "en", "tier": "national"},
    {"name": "Zambia Monitor", "url": "https://www.zambiamonitor.com", "country": "ZM", "lang": "en", "tier": "trade"},
    {"name": "Mwebantu", "url": "https://www.mwebantu.com", "country": "ZM", "lang": "en", "tier": "digital"},
    {"name": "The Herald (Zimbabwe)", "url": "https://www.herald.co.zw", "country": "ZW", "lang": "en", "tier": "national"},
    {"name": "The Chronicle (Zimbabwe)", "url": "https://www.chronicle.co.zw", "country": "ZW", "lang": "en",
     "tier": "national"},
    {"name": "NewsDay Zimbabwe", "url": "https://www.newsday.co.zw", "country": "ZW", "lang": "en", "tier": "national"},
    {"name": "ZimLive", "url": "https://www.zimlive.com", "country": "ZW", "lang": "en", "tier": "digital"},
    {"name": "NewZimbabwe", "url": "https://www.newzimbabwe.com", "country": "ZW", "lang": "en", "tier": "digital"},
    {"name": "Bulawayo24", "url": "https://bulawayo24.com", "country": "ZW", "lang": "en", "tier": "digital"},
    {"name": "Nyasa Times", "url": "https://www.nyasatimes.com", "country": "MW", "lang": "en", "tier": "digital"},
    {"name": "Malawi24", "url": "https://malawi24.com", "country": "MW", "lang": "en", "tier": "digital"},
    {"name": "The Nation (Malawi)", "url": "https://mwnation.com", "country": "MW", "lang": "en", "tier": "national"},
    {"name": "Times 360 Malawi", "url": "https://times.mw", "country": "MW", "lang": "en", "tier": "national"},
    {"name": "Club of Mozambique", "url": "https://clubofmozambique.com", "country": "MZ", "lang": "en", "tier": "trade"},
    {"name": "ANGOP", "url": "https://www.angop.ao", "country": "AO", "lang": "pt", "tier": "national"},
    {"name": "Expansão", "url": "https://expansao.co.ao", "country": "AO", "lang": "pt", "tier": "trade"},
    {"name": "The Namibian", "url": "https://www.namibian.com.na", "country": "NA", "lang": "en", "tier": "national"},
    {"name": "New Era (Namibia)", "url": "https://neweralive.na", "country": "NA", "lang": "en", "tier": "national"},
    {"name": "Namibian Sun", "url": "https://www.namibiansun.com", "country": "NA", "lang": "en", "tier": "national"},
    {"name": "Mmegi", "url": "https://www.mmegi.bw", "country": "BW", "lang": "en", "tier": "national"},
    {"name": "Sunday Standard (Botswana)", "url": "https://www.sundaystandard.info", "country": "BW", "lang": "en",
     "tier": "national"},
    {"name": "Lesotho Times", "url": "https://lestimes.com", "country": "LS", "lang": "en", "tier": "national"},
    {"name": "Times of Eswatini", "url": "https://times.co.sz", "country": "SZ", "lang": "en", "tier": "national"},
    # ── North Africa ─────────────────────────────────────────
    {"name": "Ahram Online", "url": "https://english.ahram.org.eg", "country": "EG", "lang": "en", "tier": "national"},
    {"name": "Egypt Today", "url": "https://www.egypttoday.com", "country": "EG", "lang": "en", "tier": "national"},
    {"name": "Mada Masr", "url": "https://www.madamasr.com", "country": "EG", "lang": "en", "tier": "national"},
    {"name": "Enterprise", "url": "https://enterprise.press", "country": "EG", "lang": "en", "tier": "trade"},
    {"name": "Al-Masry Al-Youm", "url": "https://www.almasryalyoum.com", "country": "EG", "lang": "ar",
     "tier": "national"},
    {"name": "Le360", "url": "https://fr.le360.ma", "country": "MA", "lang": "fr", "tier": "national"},
    {"name": "Médias24", "url": "https://medias24.com", "country": "MA", "lang": "fr", "tier": "trade"},
    {"name": "TelQuel", "url": "https://telquel.ma", "country": "MA", "lang": "fr", "tier": "national"},
    {"name": "Le Matin (Maroc)", "url": "https://lematin.ma", "country": "MA", "lang": "fr", "tier": "national"},
    {"name": "TSA Algérie", "url": "https://www.tsa-algerie.com", "country": "DZ", "lang": "fr", "tier": "national"},
    {"name": "APS Algérie", "url": "https://www.aps.dz", "country": "DZ", "lang": "fr", "tier": "national"},
    {"name": "Kapitalis", "url": "https://kapitalis.com", "country": "TN", "lang": "fr", "tier": "national"},
    {"name": "Business News Tunisie", "url": "https://www.businessnews.com.tn", "country": "TN", "lang": "fr",
     "tier": "trade"},
    {"name": "La Presse de Tunisie", "url": "https://lapresse.tn", "country": "TN", "lang": "fr", "tier": "national"},
    {"name": "Libya Observer", "url": "https://libyaobserver.ly", "country": "LY", "lang": "en", "tier": "national"},
    {"name": "Libya Herald", "url": "https://libyaherald.com", "country": "LY", "lang": "en", "tier": "national"},
    # ── Pan-African business, tech, marketing ───────────────
    {"name": "Business Insider Africa", "url": "https://africa.businessinsider.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "CNBC Africa", "url": "https://www.cnbcafrica.com", "country": "PAN", "lang": "en", "tier": "national"},
    {"name": "African Business", "url": "https://african.business", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "How We Made It In Africa", "url": "https://www.howwemadeitinafrica.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "Further Africa", "url": "https://furtherafrica.com", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "IT News Africa", "url": "https://www.itnewsafrica.com", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "TechAfrica News", "url": "https://techafricanews.com", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "Connecting Africa", "url": "https://www.connectingafrica.com", "country": "PAN", "lang": "en",
     "tier": "trade"},
    {"name": "Benjamindada", "url": "https://www.benjamindada.com", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "Africa Check", "url": "https://africacheck.org", "country": "PAN", "lang": "en", "tier": "trade"},
    {"name": "APA News", "url": "https://apanews.net", "country": "PAN", "lang": "en", "tier": "national"},
    {"name": "VOA Africa", "url": "https://www.voaafrica.com", "country": "PAN", "lang": "en", "tier": "international"},
    {"name": "VOA Afrique", "url": "https://www.voaafrique.com", "country": "PAN", "lang": "fr", "tier": "international"},
    {"name": "BBC News Africa", "url": "https://www.bbc.com/news/world/africa", "country": "PAN", "lang": "en",
     "tier": "international", "feeds": ["https://feeds.bbci.co.uk/news/world/africa/rss.xml"]},
    {"name": "The Conversation Africa", "url": "https://theconversation.com/africa", "country": "PAN", "lang": "en",
     "tier": "trade", "feeds": ["https://theconversation.com/africa/articles.atom"]},
    {"name": "Le Monde Afrique", "url": "https://www.lemonde.fr/afrique", "country": "PAN", "lang": "fr",
     "tier": "international", "feeds": ["https://www.lemonde.fr/afrique/rss_full.xml"]},
    {"name": "Jeune Afrique", "url": "https://www.jeuneafrique.com", "country": "PAN", "lang": "fr", "tier": "national"},
    {"name": "Financial Afrik", "url": "https://www.financialafrik.com", "country": "PAN", "lang": "fr",
     "tier": "trade"},
    {"name": "Agence Ecofin", "url": "https://www.agenceecofin.com", "country": "PAN", "lang": "fr", "tier": "trade"},
    {"name": "Sika Finance", "url": "https://www.sikafinance.com", "country": "PAN", "lang": "fr", "tier": "trade"},
    # allAfrica republishes hundreds of African papers: useful for reach, tagged so copies aren't counted as new coverage
    {"name": "allAfrica", "url": "https://allafrica.com", "country": "PAN", "lang": "en", "tier": "syndication"},

    # ── International ────────────────────────────────────────
    {"name": "The Guardian (Africa)", "url": "https://www.theguardian.com/world/africa", "country": "GB", "lang": "en",
     "tier": "international",
     "feeds": ["https://www.theguardian.com/world/africa/rss"]},
    # Aggregators / republishers: not crawled (huge, mostly irrelevant), but tagged when GDELT or
    # Google News surfaces one of their pages.
    {"name": "Yahoo! News UK", "url": "https://uk.news.yahoo.com", "country": "GB", "lang": "en",
     "tier": "syndication", "crawl": False},
    {"name": "AOL UK", "url": "https://www.aol.co.uk", "country": "GB", "lang": "en", "tier": "syndication",
     "crawl": False},
    {"name": "inkl", "url": "https://www.inkl.com", "country": "GB", "lang": "en", "tier": "syndication", "crawl": False},
    {"name": "Europe Says", "url": "https://www.europesays.com", "country": "EU", "lang": "en", "tier": "syndication",
     "crawl": False},
]

# Google News editions queried with country-specific brand names: (gl, language)
GOOGLE_NEWS_EDITIONS = [
    ("KE", "en"), ("NG", "en"), ("ZA", "en"), ("GH", "en"),
    ("UG", "en"), ("TZ", "en"), ("ET", "en"), ("RW", "en"),
    ("SN", "fr"), ("CI", "fr"), ("MA", "fr"), ("EG", "en"),
]

# ─────────────────────────────────────────────────────────────
# Topic-category words in African languages (added to pipeline.CATEGORY_RULES).
# Drafted for coverage, not verified by native speakers — have each list reviewed.
# Ge'ez / Arabic-script words are matched as substrings after normalisation.
# ─────────────────────────────────────────────────────────────
CATEGORY_WORDS_AFRICAN = {
    "AI & Tech": ["fasaha", "tiknoolajiyada", "tegnologie", "kunsmatige intelligensie", "tecnologia",
                  "inteligência artificial", "ቴክኖሎጂ", "الذكاء الاصطناعي", "تكنولوجيا"],
    "Health": ["lafiya", "asibiti", "ilera", "caafimaad", "isbitaal", "gesondheid", "hospitaal", "saúde",
               "ጤና", "ሆስፒታል", "صحة", "مستشفى"],
    "Politics": ["zabe", "gwamnati", "majalisa", "idibo", "ijoba", "doorasho", "dowladda", "baarlamaanka",
                 "amatora", "guverinoma", "verkiesing", "regering", "eleições", "governo", "parlamento",
                 "ምርጫ", "መንግሥት", "ፓርላማ", "انتخابات", "حكومة", "برلمان"],
    "Business": ["tattalin arziki", "kasuwanci", "dhaqaalaha", "ganacsi", "ekonomie", "besigheid", "economia",
                 "investimento", "mercado", "ኢኮኖሚ", "ኢንቨስትመንት", "اقتصاد", "استثمار"],
    "Climate & Environment": ["ambaliya", "abaar", "fatahaad", "droogte", "klimaat", "seca", "cheias", "clima",
                              "ድርቅ", "ጎርፍ", "جفاف", "فيضانات", "مناخ"],
    "Education": ["ilimi", "makaranta", "ile-iwe", "waxbarasho", "onderwys", "skool", "educação", "escola",
                  "universidade", "ትምህርት", "تعليم", "جامعة"],
    "Agriculture": ["noma", "manoma", "beeraha", "landbou", "agricultura", "agricultores", "ግብርና", "زراعة"],
    "Security & Conflict": ["tsaro", "amniga", "dagaal", "veiligheid", "segurança", "conflito", "ataque",
                            "ፀጥታ", "ግጭት", "الأمن", "هجوم", "صراع"],
}


# ════════════════════════════════════════════════════════════════════════════
# PART 2 — LANGUAGES
# ════════════════════════════════════════════════════════════════════════════
# MediaPulse Africa — African-language support shared by pipeline.py and search.py.
#
# What this handles:
#   * Language codes: the detector (py3langid) recognises Swahili, Hausa, Yoruba, Igbo,
#     Nigerian Pidgin, Amharic, Somali, Oromo, Kinyarwanda, Luganda, Shona, Zulu, Xhosa,
#     Afrikaans, Sesotho, Sepedi, Lingala, Gikuyu, Malagasy, Fulfulde, Kabyle, Arabic
#     (incl. Egyptian/Moroccan), Portuguese and French. For languages it can't recognise
#     (Tigrinya, Wolof, Twi, Ewe, Kirundi, Chichewa, Dholuo, ...) the source's declared
#     language or the page's <html lang> is trusted instead.
#   * Scripts: Ge'ez (Amharic, Tigrinya) and Arabic script attach prefixes to words
#     (የ-/በ-/ለ- ; و-/ب-/ل-), so brand names inside them can't be matched on word
#     boundaries, and both scripts have interchangeable spellings. Text and aliases in
#     these scripts are normalised and matched as substrings.
#   * Optional models for sentiment and organisation detection beyond English (see
#     MP_SENTIMENT_MODEL / MP_NER_MODEL below). Without them, non-English articles are
#     stored, indexed, searched and brand-matched, but sentiment is "Not scored".



LANGUAGE_NAMES = {
    "en": "English", "fr": "French", "pt": "Portuguese", "ar": "Arabic", "sw": "Swahili", "ha": "Hausa",
    "yo": "Yoruba", "ig": "Igbo", "pcm": "Nigerian Pidgin", "am": "Amharic", "ti": "Tigrinya", "om": "Oromo",
    "so": "Somali", "rw": "Kinyarwanda", "rn": "Kirundi", "lg": "Luganda", "sn": "Shona", "nd": "Ndebele",
    "zu": "Zulu", "xh": "Xhosa", "af": "Afrikaans", "st": "Sesotho", "nso": "Sepedi", "tn": "Setswana",
    "ln": "Lingala", "kik": "Gikuyu", "luo": "Dholuo", "mg": "Malagasy", "wo": "Wolof", "ff": "Fulfulde",
    "fuv": "Fulfulde", "tw": "Twi", "ak": "Akan", "ee": "Ewe", "ny": "Chichewa", "bm": "Bambara",
    "kab": "Kabyle", "ber": "Amazigh", "und": "Unknown",
}

# Languages py3langid has no model for — trust the page / source declaration for these.
UNDETECTABLE = {"ti", "wo", "luo", "tw", "ak", "ee", "ny", "bm", "rn", "nd", "tn", "ts", "ve", "ss",
                "kg", "ff", "ber", "tzm", "lua", "sg", "ki", "mer", "kam", "guz", "luy"}

# Detector variants folded into the code reports use
_LANG_ALIAS = {"arz": "ar", "ary": "ar", "apc": "ar", "ki": "kik", "fuv": "ff", "gug": "gn",
               "iw": "he", "in": "id"}

_ETHIOPIC = re.compile(r"[ሀ-፿ᎀ-᎟ⶀ-⷟]")
_ARABIC = re.compile(r"[؀-ۿݐ-ݿࢠ-ࣿ]")
_TASHKEEL = re.compile(r"[ؐ-ًؚ-ٰٟۖ-ۭـ]")


def normalize_lang(code: Optional[str]) -> str:
    """'en-US' -> 'en', 'arz' -> 'ar', '' -> 'und'."""
    if not code:
        return "und"
    code = code.strip().replace("_", "-").split("-")[0].lower()
    return _LANG_ALIAS.get(code, code) or "und"


def is_nonlatin(s: str) -> bool:
    return bool(_ETHIOPIC.search(s) or _ARABIC.search(s))


def _ethiopic_table() -> dict:
    """Amharic homophone letters written interchangeably: ሐ/ኀ→ሀ, ሠ→ሰ, ዐ→አ, ፀ→ጸ (all vowel orders)."""
    table = {}
    for src, dst, n in ((0x1210, 0x1200, 8), (0x1280, 0x1200, 8), (0x1220, 0x1230, 8),
                        (0x12D0, 0x12A0, 7), (0x1340, 0x1338, 7)):
        for i in range(n):
            table[src + i] = dst + i
    return table


_ETH_TABLE = _ethiopic_table()
_AR_TABLE = str.maketrans({"أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا", "ة": "ه", "ى": "ي", "ؤ": "و", "ئ": "ي"})


def normalize_script(text: str) -> str:
    """Spelling-variant normalisation for Ge'ez and Arabic script; Latin text is returned unchanged
    (lower-cased elsewhere). Length can change, so match and window on the normalised string."""
    if not text or not is_nonlatin(text):
        return text
    text = _TASHKEEL.sub("", text)
    return text.translate(_AR_TABLE).translate(_ETH_TABLE)


_HTML_LANG = re.compile(r"<html[^>]*?\blang\s*=\s*[\"']?([A-Za-z]{2,3}(?:[-_][A-Za-z0-9]+)?)", re.I)


def min_article_chars(text: str, latin_min: int) -> int:
    """A Ge'ez character is a whole syllable and Arabic omits short vowels, so the same article
    is far shorter in characters. Scale the 'is this a real article' threshold by script."""
    sample = text[:600]
    if _ETHIOPIC.search(sample):
        return int(latin_min * 0.45)
    if _ARABIC.search(sample):
        return int(latin_min * 0.7)
    return latin_min


def page_lang(html: str) -> str:
    m = _HTML_LANG.search(html[:3000] if html else "")
    return normalize_lang(m.group(1)) if m else ""


def choose_language(detected: str, hint: str) -> str:
    """Detector result unless the declared language is one the detector can't know."""
    hint = normalize_lang(hint) if hint else ""
    if hint in UNDETECTABLE:
        return hint
    if detected and detected != "und":
        return normalize_lang(detected)
    return hint or "und"


# ─────────────────────────────────────────────
# OPTIONAL MODELS (Hugging Face transformers)
#   MP_SENTIMENT_MODEL  text-classification model id or local path, e.g. a model fine-tuned
#                       on AfriSenti (Hausa, Yoruba, Igbo, Amharic, Swahili, Kinyarwanda, ...)
#   MP_SENTIMENT_LABELS optional mapping when labels are opaque: "LABEL_0=Negative,LABEL_1=Neutral,LABEL_2=Positive"
#   MP_SENTIMENT_ALL    "1" = use the model for English too (default: TextBlob for English)
#   MP_NER_MODEL        token-classification model id or path, e.g. one trained on MasakhaNER
#   spaCy xx_ent_wiki_sm (python -m spacy download xx_ent_wiki_sm) is used as a weaker
#   fallback for organisation detection in Latin-script languages when no NER model is set.
# ─────────────────────────────────────────────
_sent_pipe = None
_ner_pipe = None
_xx_nlp = None
_loaded = False


def _load_models():
    global _sent_pipe, _ner_pipe, _xx_nlp, _loaded
    if _loaded:
        return
    _loaded = True
    sent_id, ner_id = os.getenv("MP_SENTIMENT_MODEL"), os.getenv("MP_NER_MODEL")
    if sent_id or ner_id:
        try:
            from transformers import pipeline as hf_pipeline
            if sent_id:
                _sent_pipe = hf_pipeline("text-classification", model=sent_id, truncation=True)
                log.info(f"[Lang] sentiment model: {sent_id}")
            if ner_id:
                _ner_pipe = hf_pipeline("token-classification", model=ner_id, aggregation_strategy="simple")
                log.info(f"[Lang] NER model: {ner_id}")
        except Exception as e:
            log.warning(f"[Lang] could not load transformers model(s): {e}")
    if _ner_pipe is None:
        try:
            import spacy
            _xx_nlp = spacy.load("xx_ent_wiki_sm")
        except Exception:
            _xx_nlp = None


def set_models(sentiment_fn=None, ner_fn=None):
    """Inject callables (tests, or a custom model server). sentiment_fn(text)->[{'label','score'}];
    ner_fn(text)->[{'entity_group','word'}]."""
    global _sent_pipe, _ner_pipe, _loaded
    _loaded = True
    _sent_pipe, _ner_pipe = sentiment_fn, ner_fn


def _label_map() -> dict:
    raw = os.getenv("MP_SENTIMENT_LABELS", "")
    return {k.strip(): v.strip() for k, v in (p.split("=", 1) for p in raw.split(",") if "=" in p)}


def model_sentiment(text: str) -> Optional[tuple[float, str]]:
    _load_models()
    if not _sent_pipe or not text.strip():
        return None
    try:
        r = _sent_pipe(text[:1500])[0]
    except Exception as e:
        log.debug(f"[Lang] sentiment failed: {e}")
        return None
    raw = str(r.get("label", ""))
    label = _label_map().get(raw, raw)
    low = label.lower()
    if low.startswith("pos"):
        return round(float(r.get("score", 0)), 4), "Positive"
    if low.startswith("neg"):
        return round(-float(r.get("score", 0)), 4), "Negative"
    if low.startswith("neu"):
        return 0.0, "Neutral"
    return None


def model_sentiment_available() -> bool:
    _load_models()
    return _sent_pipe is not None


def use_model_for_english() -> bool:
    return os.getenv("MP_SENTIMENT_ALL", "0") == "1" and model_sentiment_available()


def model_orgs(text: str, lang: str) -> list[str]:
    _load_models()
    if _ner_pipe:
        try:
            ents = _ner_pipe(text[:5000])
            return [e["word"].strip() for e in ents if str(e.get("entity_group", "")).upper() in ("ORG", "B-ORG", "I-ORG")]
        except Exception as e:
            log.debug(f"[Lang] NER failed: {e}")
            return []
    if _xx_nlp and not is_nonlatin(text[:500]):
        doc = _xx_nlp(text[:10000])
        return [e.text for e in doc.ents if e.label_ == "ORG"]
    return []


# ════════════════════════════════════════════════════════════════════════════
# PART 3 — PIPELINE
# ════════════════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────
# OPTIONAL NLP
# ─────────────────────────────────────────────
try:
    import spacy
    try:
        NLP = spacy.load("en_core_web_sm", exclude=["parser", "tagger", "lemmatizer", "attribute_ruler", "senter"])
    except OSError:
        NLP = None
        log.warning("[NER] en_core_web_sm missing — run: python -m spacy download en_core_web_sm")
except ImportError:
    NLP = None

try:
    import py3langid as _langid

    def _lang_detect(text: str) -> str:
        return _langid.classify(text)[0]
except ImportError:
    _lang_detect = None


# ─────────────────────────────────────────────
# SMALL UTILITIES
# ─────────────────────────────────────────────
def now_utc() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso(d: Optional[dt.datetime]) -> str:
    return d.astimezone(UTC).isoformat(timespec="seconds") if d else ""


def parse_date(value) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        d = dateparser.parse(str(value))
    except (ValueError, OverflowError, TypeError):
        return None
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC)


def parse_gdelt_date(value: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return parse_date(value)


_TRACKING = re.compile(r"^(utm_|fbclid$|gclid$|mc_|igshid$|ocid$|cmpid$|ref$|ref_src$|at_)", re.I)


def canonicalize(url: str) -> str:
    """Stable key for one article: drops tracking params and fragments."""
    if not url:
        return ""
    p = urlparse(url.strip())
    if p.scheme not in ("http", "https") or not p.netloc:
        return ""
    query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not _TRACKING.match(k)]
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path or "/", "", urlencode(query, doseq=True), ""))


def host_key(netloc: str) -> str:
    netloc = netloc.lower().split(":")[0]
    return netloc[4:] if netloc.startswith("www.") else netloc


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()


def chunked(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def clean_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(text or ""))
    text = re.sub(r"http\S+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _index_sources(sources: list) -> tuple[dict, dict]:
    by_host, by_name = {}, {}
    for s in sources:
        u = urlparse(s["url"])
        by_host.setdefault(host_key(u.netloc), []).append((u.path.rstrip("/"), s))
        by_name.setdefault(s["name"].lower(), s)
    for entries in by_host.values():
        entries.sort(key=lambda e: len(e[0]), reverse=True)      # longest path prefix wins
    return by_host, by_name


SOURCE_BY_HOST, SOURCE_BY_NAME = _index_sources(SOURCES)


def source_info(url: str, publisher: str = "") -> Optional[dict]:
    """Known outlet for a URL (subdomains included), or by publisher name for Google News items."""
    u = urlparse(url)
    parts = host_key(u.netloc).split(".")
    for i in range(len(parts) - 1):
        entries = SOURCE_BY_HOST.get(".".join(parts[i:]))
        if entries:
            for prefix, src in entries:
                if not prefix or u.path == prefix or u.path.startswith(prefix + "/"):
                    return src
            return None          # host known, but only specific sections of it are tracked
    return SOURCE_BY_NAME.get(publisher.lower()) if publisher else None


def round_robin_by_host(cands: list) -> list:
    """Interleave hosts so each fetch batch spreads load instead of queueing on one site."""
    buckets: dict[str, deque] = defaultdict(deque)
    for c in cands:
        buckets[host_key(urlparse(c.url).netloc)].append(c)
    out = []
    while buckets:
        for h in list(buckets):
            out.append(buckets[h].popleft())
            if not buckets[h]:
                del buckets[h]
    return out


# ─────────────────────────────────────────────
# DATA TYPES
# ─────────────────────────────────────────────
@dataclass
class Candidate:
    url: str
    source: str
    country: str = ""
    lang: str = ""
    via: str = ""
    published: Optional[dt.datetime] = None
    title_hint: str = ""
    summary_hint: str = ""
    tier: str = ""
    fetch: bool = True        # False = keep title/summary only (e.g. Google News redirect links)
    queued_at: str = ""       # when the URL first went to the pending queue


@dataclass
class FetchResult:
    resp: Optional[httpx.Response]
    reason: str               # ok | robots | gone | error | http_<code>
    status: int = 0
    body: str = ""


def is_transient(reason: str) -> bool:
    return reason == "error" or reason.startswith("http_")


# ─────────────────────────────────────────────
# POLITE FETCHER — robots.txt, crawl-delay, per-host pacing
# ─────────────────────────────────────────────
class Fetcher:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.sem = asyncio.Semaphore(CONCURRENCY)
        self._turn_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_hit: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser] = {}
        self._robots_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.robots_note: dict[str, str] = {}
        self.requests = 0

    def host_delay(self, url: str) -> float:
        """Seconds between requests to this URL's host: our default, or the site's Crawl-delay."""
        rp = self._robots.get(urlparse(url).netloc.lower())
        cd = rp.crawl_delay(BOT_TOKEN) if rp else None
        return max(DOMAIN_DELAY, float(cd or 0))

    async def _raw_get(self, url: str, delay: float, headers: Optional[dict] = None) -> httpx.Response:
        host = urlparse(url).netloc.lower()
        async with self._turn_locks[host]:
            wait = self._last_hit.get(host, 0.0) + delay - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_hit[host] = time.monotonic()
        async with self.sem:
            self.requests += 1
            return await self.client.get(url, follow_redirects=True, headers=headers)

    async def robots(self, url: str) -> RobotFileParser:
        p = urlparse(url)
        host = p.netloc.lower()
        if host in self._robots:
            return self._robots[host]
        async with self._robots_locks[host]:
            if host in self._robots:
                return self._robots[host]
            rp = RobotFileParser()
            try:
                r = await self._raw_get(f"{p.scheme}://{p.netloc}/robots.txt", DOMAIN_DELAY)
                if r.status_code >= 500 or r.status_code == 429:
                    rp.disallow_all = True          # RFC 9309: server error => assume full disallow
                    self.robots_note[host] = f"robots.txt HTTP {r.status_code} (treated as disallow-all)"
                elif r.status_code >= 400:
                    rp.parse([])                    # RFC 9309: 4xx => no restrictions
                    self.robots_note[host] = (f"robots.txt HTTP {r.status_code} — site may be blocking bots"
                                              if r.status_code in (401, 403) else "no robots.txt")
                else:
                    rp.parse(r.text.splitlines())
                    self.robots_note[host] = "robots.txt ok"
            except Exception as e:                  # unreachable host: be conservative
                log.debug(f"[robots] {host}: {e}")
                rp.disallow_all = True
                self.robots_note[host] = f"robots.txt unreachable ({type(e).__name__})"
            self._robots[host] = rp
            return rp

    async def get(self, url: str, *, respect_robots: bool = True,
                  delay: Optional[float] = None, headers: Optional[dict] = None) -> FetchResult:
        delay = DOMAIN_DELAY if delay is None else delay
        if respect_robots:
            rp = await self.robots(url)
            if not rp.can_fetch(BOT_TOKEN, url):
                return FetchResult(None, "robots")
            crawl_delay = rp.crawl_delay(BOT_TOKEN)
            if crawl_delay:
                delay = max(delay, float(crawl_delay))
        try:
            r = await self._raw_get(url, delay, headers)
        except Exception as e:
            log.debug(f"[fetch] {url}: {e}")
            return FetchResult(None, "error")
        if r.status_code == 429 or r.status_code >= 500:
            return FetchResult(None, f"http_{r.status_code}", r.status_code, r.text[:300])
        if r.status_code >= 400:
            return FetchResult(None, "gone", r.status_code, r.text[:300])
        return FetchResult(r, "ok", r.status_code)


# ─────────────────────────────────────────────
# DISCOVERY HELPERS
# ─────────────────────────────────────────────
def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_sitemap(content: bytes) -> tuple[list, list]:
    """Returns (child_sitemaps, article_urls) as lists of (url, datetime|None).
    Handles sitemap indexes, plain sitemaps, Google News sitemaps and .gz."""
    if content[:2] == b"\x1f\x8b":
        try:
            content = gzip.decompress(content)
        except OSError:
            return [], []
    try:
        root = ET.fromstring(content.lstrip())
    except ET.ParseError:
        return [], []
    items = []
    for node in root:
        loc, lastmod, pubdate = None, None, None
        for child in node.iter():
            name = _local(child.tag)
            if name == "loc" and loc is None:            # first <loc> is the page, later ones are images
                loc = (child.text or "").strip()
            elif name == "publication_date":
                pubdate = parse_date(child.text)
            elif name == "lastmod":
                lastmod = parse_date(child.text)
        if loc:
            items.append((loc, pubdate or lastmod))
    if _local(root.tag) == "sitemapindex":
        return items, []
    return [], items


def _prioritise_sitemaps(items: list) -> list:
    year = str(now_utc().year)

    def key(item):
        url, d = item
        u = url.lower()
        return ("news" not in u, year not in u, d is None, -(d.timestamp() if d else 0))

    return [u for u, _ in sorted(items, key=key)]


def find_feed_links(html: str, base: str) -> list[str]:
    if not html:
        return []
    try:
        doc = lxml.html.fromstring(html)
    except Exception:
        return []
    out = []
    for el in doc.xpath('//link[@href]'):
        rel = (el.get("rel") or "").lower()
        typ = (el.get("type") or "").lower()
        if "alternate" in rel and ("rss" in typ or "atom" in typ):
            out.append(urljoin(base, el.get("href")))
    return out


_EXCLUDE_PATH = re.compile(
    r"(/(tag|tags|category|categories|author|authors|page|search|login|register|subscribe|"
    r"account|video|videos|gallery|photos|podcasts?|live|topics?)(/|$))|"
    r"\.(jpg|jpeg|png|gif|webp|pdf|xml|mp3|mp4|css|js)$",
    re.I,
)
_ARTICLE_HINT = re.compile(r"(/20\d{2}/\d{1,2}/)|(\d{5,})|([a-z0-9]+(?:-[a-z0-9]+){3,})", re.I)


def looks_like_article(path: str) -> bool:
    """Section/listing pages (/news/, /regions, /composition/hp-news-4/) are not articles.
    Articles have a long slug, a numeric id, a dated path, or an id-like last segment."""
    segs = [x for x in path.split("/") if x]
    if not segs:
        return False
    last = segs[-1]
    if re.search(r"/20\d{2}/\d{1,2}/", path) or re.search(r"\d{5,}", last) or last.count("-") >= 3:
        return True
    if "%" in last and len(last) > 40:                       # percent-encoded Arabic/Ge'ez slugs
        return True
    if re.search(r"\.(s?html?|php|aspx?)$", last, re.I) and len(last) > 15:
        return True
    return len(last) >= 10 and "-" not in last and bool(re.search(r"\d", last)) and bool(re.search(r"[a-z]", last, re.I))


def find_article_links(html: str, base: str) -> list[str]:
    if not html:
        return []
    try:
        doc = lxml.html.fromstring(html)
    except Exception:
        return []
    out = []
    for a in doc.xpath("//a[@href]"):
        href = urljoin(base, a.get("href"))
        if _ARTICLE_HINT.search(urlparse(href).path):
            out.append(href)
    return out


# ─────────────────────────────────────────────
# DISCOVERY: OWN CRAWL OF EACH OUTLET
# ─────────────────────────────────────────────
async def discover_source(f: Fetcher, src: dict, cutoff: dt.datetime, diag: Optional[dict] = None) -> list[Candidate]:
    base = src["url"].rstrip("/") + "/"
    bp = urlparse(base)
    root = f"{bp.scheme}://{bp.netloc}"
    prefix = bp.path.rstrip("/") if bp.path not in ("", "/") else ""
    src_host = host_key(bp.netloc)
    found: dict[str, Candidate] = {}
    dropped = Counter()

    def add(url: str, published=None, via: str = "", title: str = "", summary: str = ""):
        cu = canonicalize(url)
        if not cu:
            return
        up = urlparse(cu)
        if host_key(up.netloc) != src_host:
            dropped["other_site"] += 1
            return
        if prefix and via != "rss" and not up.path.startswith(prefix):
            dropped["outside_section"] += 1
            return
        if up.path in ("", "/") or _EXCLUDE_PATH.search(up.path):
            dropped["tag/video/page_url"] += 1
            return
        if via != "rss" and not looks_like_article(up.path):
            dropped["not_article"] += 1
            return
        if published and published < cutoff:
            dropped["older_than_lookback"] += 1
            return
        if cu in found:
            c = found[cu]
            c.published = c.published or published
            c.title_hint = c.title_hint or clean_text(title)
            return
        found[cu] = Candidate(url=cu, source=src["name"], country=src.get("country", ""),
                              lang=src.get("lang", ""), via=via, published=published,
                              tier=src.get("tier", "national"),
                              title_hint=clean_text(title), summary_hint=clean_text(summary)[:500])

    # 1) Sitemaps listed in robots.txt (news sitemaps first), fallback /sitemap.xml
    rp = await f.robots(root + "/")
    if "sitemaps" in src:
        sitemaps = list(src["sitemaps"])
    elif prefix:
        sitemaps = []        # a section of a big site (bbc.com/hausa): its host-wide sitemaps are slow and off-topic
    else:
        sitemaps = list(rp.site_maps() or []) or [root + "/sitemap.xml"]
    queue = deque(_prioritise_sitemaps([(s, None) for s in sitemaps]))
    visited: set[str] = set()
    sm_notes = Counter()
    while queue and len(visited) < MAX_SITEMAPS:
        sm = queue.popleft()
        if sm in visited:
            continue
        visited.add(sm)
        res = await f.get(sm)
        if not res.resp:
            sm_notes[res.reason if res.reason != "gone" else f"HTTP {res.status}"] += 1
            continue
        sm_notes["ok"] += 1
        children, urls = parse_sitemap(res.resp.content)
        fresh_children = [(u, d) for u, d in children if d is None or d >= cutoff]
        queue.extend(_prioritise_sitemaps(fresh_children))
        news_like = "news" in sm.lower()
        for u, d in urls:
            if d is None and not news_like:      # undated URLs in a general sitemap = archive noise
                dropped["undated_in_sitemap"] += 1
                continue
            add(u, d, "sitemap")

    # 2) RSS / Atom: configured + auto-discovered from homepage <link rel=alternate>
    home = await f.get(base)
    feed_notes = Counter()
    html = ""
    if home.resp and "html" in home.resp.headers.get("content-type", "").lower():
        html = home.resp.text
    feeds = list(src.get("feeds", [])) + find_feed_links(html, base)
    if not feeds:
        feeds = [urljoin(base, "feed/"), urljoin(base, "rss")]
    for feed_url in list(dict.fromkeys(feeds))[:4]:
        res = await f.get(feed_url)
        if not res.resp:
            feed_notes[res.reason if res.reason != "gone" else f"HTTP {res.status}"] += 1
            continue
        parsed = feedparser.parse(res.resp.content)
        feed_notes["ok" if parsed.entries else "empty"] += 1
        for e in parsed.entries:
            link = e.get("link")
            if link:
                add(link, parse_date(e.get("published") or e.get("updated")), "rss",
                    e.get("title", ""), e.get("summary", ""))

    # 3) Homepage links — last resort when a site has no usable sitemap or feed
    if len(found) < 15:
        links = find_article_links(html, base)
        if html and not links:
            dropped["homepage_has_no_article_links"] += 1
        for link in links:
            add(link, None, "homepage")

    cands = sorted(found.values(), key=lambda c: c.published or EPOCH, reverse=True)
    cands = cands[:MAX_URLS_PER_SOURCE]
    vias = Counter(c.via for c in cands)
    if cands:
        log.info(f"[Discover] {src['name']}: {len(cands)} candidates {dict(vias)}")
    else:
        home_note = "ok" if home.resp else (home.reason if home.reason != "gone" else f"HTTP {home.status}")
        why = (f"{f.robots_note.get(bp.netloc.lower(), 'robots.txt ?')}; homepage: {home_note}; "
               f"sitemaps: {dict(sm_notes) or 'none listed'}; feeds: {dict(feed_notes) or 'none found'}; "
               f"filtered: {dict(dropped) or 'nothing'}")
        log.info(f"[Discover] {src['name']}: 0 candidates — {why}")
        if diag is not None:
            diag[src["name"]] = why
    return cands


# ─────────────────────────────────────────────
# DISCOVERY: GLOBAL INDEXES (catch outlets not in SOURCES)
# ─────────────────────────────────────────────
GDELT_LANG = {"English": "en", "French": "fr", "Swahili": "sw", "Arabic": "ar", "Portuguese": "pt",
              "Amharic": "am", "Tigrinya": "ti", "Oromo": "om", "Hausa": "ha", "Yoruba": "yo", "Igbo": "ig",
              "Somali": "so", "Afrikaans": "af", "Zulu": "zu", "Xhosa": "xh", "Kinyarwanda": "rw",
              "Kirundi": "rn", "Shona": "sn", "Malagasy": "mg", "Wolof": "wo", "Lingala": "ln",
              "Sesotho": "st", "Setswana": "tn", "Chichewa": "ny"}
GDELT_COUNTRY = {  # GDELT reports country names; the rest of the pipeline uses ISO-2 codes
    "Kenya": "KE", "Uganda": "UG", "Tanzania": "TZ", "Rwanda": "RW", "Burundi": "BI", "Ethiopia": "ET",
    "Somalia": "SO", "South Sudan": "SS", "Sudan": "SD", "Nigeria": "NG", "Ghana": "GH", "Liberia": "LR",
    "Sierra Leone": "SL", "Senegal": "SN", "Ivory Coast": "CI", "Cote d'Ivoire": "CI", "Togo": "TG", "Benin": "BJ",
    "Cameroon": "CM", "Mali": "ML", "Burkina Faso": "BF", "Niger": "NE", "Guinea": "GN", "Gambia": "GM",
    "South Africa": "ZA", "Zambia": "ZM", "Zimbabwe": "ZW", "Botswana": "BW", "Namibia": "NA", "Malawi": "MW",
    "Mozambique": "MZ", "Angola": "AO", "Madagascar": "MG", "Mauritius": "MU", "Egypt": "EG", "Morocco": "MA",
    "Algeria": "DZ", "Tunisia": "TN", "Libya": "LY", "Democratic Republic of the Congo": "CD",
    "Republic of the Congo": "CG", "Gabon": "GA", "United Kingdom": "GB", "United States": "US",
    "France": "FR", "India": "IN", "China": "CN", "United Arab Emirates": "AE",
}
GDELT_MAX_HOURS = 90 * 24      # the GDELT DOC API only searches roughly the last 3 months


def _gdelt_term(t: str) -> Optional[str]:
    """GDELT rejects punctuation and very short words inside phrases ("The specified phrase is too short")."""
    t = re.sub(r"[^\w\s-]", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    words = t.split()
    if not words or len(t) < 5 or any(len(w) < 2 for w in words):
        return None
    return t


async def discover_gdelt(f: Fetcher, terms: list[str], hours: int = LOOKBACK_HOURS,
                         deadline: Optional[float] = None) -> list[Candidate]:
    out: list[Candidate] = []
    clean = [g for g in (_gdelt_term(t) for t in dict.fromkeys(terms)) if g]
    hours = max(1, min(hours, GDELT_MAX_HOURS))
    rate_limited = 0
    for batch in chunked(list(dict.fromkeys(clean)), GDELT_BATCH):
        if deadline and time.monotonic() > deadline:
            log.warning("[GDELT] discovery time budget reached — remaining queries skipped")
            break
        q = " OR ".join(f'"{t}"' if " " in t else t for t in batch)
        if len(batch) > 1:
            q = f"({q})"
        url = "https://api.gdeltproject.org/api/v2/doc/doc?" + urlencode({
            "query": q, "mode": "artlist", "maxrecords": "250", "format": "json",
            "sort": "datedesc", "timespan": f"{hours}h",
        })
        res = None
        for attempt in range(2):
            res = await f.get(url, respect_robots=False, delay=GDELT_DELAY)
            if res.status != 429 and res.reason != "error":
                break
            await asyncio.sleep(20 * (attempt + 1))          # back off, then one retry
        if res.status == 429:
            rate_limited += 1
            if rate_limited >= GDELT_MAX_429:
                log.warning(f"[GDELT] rate-limited {rate_limited}× in a row — skipping GDELT for this run "
                            "(GDELT throttles shared CI IP addresses)")
                break
            continue
        rate_limited = 0
        if not res.resp:
            log.warning(f"[GDELT] {res.reason} for batch starting '{batch[0]}'")
            continue
        try:
            articles = res.resp.json().get("articles", [])
        except ValueError:
            log.warning(f"[GDELT] rejected query starting '{batch[0]}': {res.resp.text[:100].strip()!r}")
            continue
        for a in articles:
            cu = canonicalize(a.get("url", ""))
            if not cu:
                continue
            out.append(Candidate(
                url=cu, source=a.get("domain") or host_key(urlparse(cu).netloc),
                country=GDELT_COUNTRY.get(a.get("sourcecountry", ""), a.get("sourcecountry", "")),
                lang=GDELT_LANG.get(a.get("language", ""), ""),
                via="gdelt", published=parse_gdelt_date(a.get("seendate", "")),
                title_hint=clean_text(a.get("title", "")),
            ))
    log.info(f"[GDELT] {len(out)} candidates")
    return out


class _RobotsBlocked(Exception):
    pass


async def _gnews_search(f: Fetcher, names: list[str], gl: str, lang: str, days: int) -> list[Candidate]:
    out: list[Candidate] = []
    for batch in chunked(names, 8):
        q = " OR ".join(f'"{n}"' for n in batch) + f" when:{days}d"
        url = "https://news.google.com/rss/search?" + urlencode(
            {"q": q, "hl": f"{lang}-{gl}", "gl": gl, "ceid": f"{gl}:{lang}"})
        res = await f.get(url)
        if not res.resp:
            if res.reason == "robots":
                raise _RobotsBlocked()
            continue
        for e in feedparser.parse(res.resp.content).entries:
            publisher = (e.get("source") or {}).get("title", "") or "Google News"
            title = e.get("title", "")
            if publisher and title.endswith(f" - {publisher}"):
                title = title[: -len(publisher) - 3]
            cu = canonicalize(e.get("link", ""))
            if cu:
                out.append(Candidate(url=cu, source=publisher, country=gl, lang=lang, via="google_news",
                                     published=parse_date(e.get("published")),
                                     title_hint=clean_text(title), fetch=False))
    return out


async def discover_google_news(f: Fetcher, registry: Optional[dict] = None, days: Optional[int] = None,
                               all_editions: bool = False) -> list[Candidate]:
    """Google News RSS links are encoded redirects that can't be fetched directly,
    so these are stored as headline-level records (fetch=False).
    Each brand is searched in its home-country edition; brands whose country has no
    edition (PAN, ZM, LR, BW, ...) go to the first edition. all_editions=True searches
    every name in every edition (used by on-demand collection)."""
    registry = BRANDS if registry is None else registry
    days = days or max(1, LOOKBACK_HOURS // 24)
    editions = GOOGLE_NEWS_EDITIONS
    covered = {gl for gl, _ in editions}
    out: list[Candidate] = []
    try:
        for i, (gl, lang) in enumerate(editions):
            if all_editions:
                names = list(registry)
            else:
                names = [n for n, s in registry.items() if s.get("country") == gl]
                if i == 0:
                    names += [n for n, s in registry.items() if s.get("country") not in covered]
            out += await _gnews_search(f, names, gl, lang, days)
    except _RobotsBlocked:
        log.warning("[GNews] blocked by robots.txt — skipping Google News")
    log.info(f"[GNews] {len(out)} candidates")
    return out


async def discover_newsapi(f: Fetcher, cutoff: dt.datetime) -> list[Candidate]:
    out: list[Candidate] = []
    for batch in chunked(list(BRANDS), 10):
        q = " OR ".join(f'"{n}"' for n in batch)
        url = "https://newsapi.org/v2/everything?" + urlencode({
            "q": q, "from": cutoff.isoformat(timespec="seconds"),
            "sortBy": "publishedAt", "pageSize": "100",
        })
        res = await f.get(url, respect_robots=False, delay=1.0, headers={"X-Api-Key": NEWS_API_KEY})
        if not res.resp:
            try:
                err = json.loads(res.body)
                msg = f"{err.get('code')}: {err.get('message')}"
            except ValueError:
                msg = res.body[:200] or res.reason
            log.warning(f"[NewsAPI] HTTP {res.status} — {msg}. Skipping NewsAPI for this run.")
            break
        for a in res.resp.json().get("articles", []):
            cu = canonicalize(a.get("url", ""))
            if cu:
                out.append(Candidate(url=cu, source=(a.get("source") or {}).get("name", "NewsAPI"),
                                     via="newsapi", published=parse_date(a.get("publishedAt")),
                                     title_hint=clean_text(a.get("title", "")),
                                     summary_hint=clean_text(a.get("description", ""))[:500]))
    log.info(f"[NewsAPI] {len(out)} candidates")
    return out


# ─────────────────────────────────────────────
# FETCH + EXTRACT
# ─────────────────────────────────────────────
def _extract(html: str, url: str, recall: bool = False) -> Optional[dict]:
    out = trafilatura.extract(html, url=url, output_format="json", with_metadata=True,
                              include_comments=False, include_tables=False,
                              favor_precision=not recall, favor_recall=recall)
    return json.loads(out) if out else None


def _extract_page(html: str, url: str) -> tuple[str, Optional[dict]]:
    """Full text if possible; otherwise headline + description from the page's metadata, so a
    paywalled or script-rendered article is still recorded (and brand-matched) instead of dropped."""
    for recall in (False, True):
        try:
            meta = _extract(html, url, recall)
        except Exception:
            meta = None
        body = (meta or {}).get("text") or (meta or {}).get("raw_text") or ""
        if meta and len(body) >= min_article_chars(body, MIN_TEXT_CHARS):
            return "ok", meta
    try:
        md = trafilatura.extract_metadata(html, default_url=url)
        md = md.as_dict() if md else {}
    except Exception:
        md = {}
    pagetype = (md.get("pagetype") or "").lower()
    if md.get("title") and md.get("description") and pagetype in ("", "article", "newsarticle"):
        return "meta_only", {"title": md.get("title"), "excerpt": md.get("description"), "text": "",
                             "date": md.get("date"), "author": md.get("author")}
    return "no_text", None


async def fetch_candidate(f: Fetcher, c: Candidate):
    if not c.fetch:
        return "hint", c, None, c.url
    res = await f.get(c.url)
    if not res.resp:
        return res.reason, c, None, c.url
    if "html" not in res.resp.headers.get("content-type", "").lower():
        return "not_html", c, None, c.url
    final = canonicalize(str(res.resp.url)) or c.url
    try:
        status, meta = await asyncio.to_thread(_extract_page, res.resp.text, final)
    except Exception as e:
        log.debug(f"[extract] {final}: {e}")
        status, meta = "no_text", None
    if meta is None:
        return status, c, None, final
    meta["_page_lang"] = page_lang(res.resp.text)
    return status, c, meta, final


# ─────────────────────────────────────────────
# ANALYSIS
# ─────────────────────────────────────────────
class TermMatcher:
    """Latin script: word-boundary matching; short ALL-CAPS aliases are case-sensitive.
    Ge'ez / Arabic script: normalised substring matching (prefixes attach to words there).
    Optional context words disambiguate names like Shell / Bolt / Glo."""

    def __init__(self, registry: dict):
        self.entries = []
        for name, spec in registry.items():
            pats = []
            for alias in spec["aliases"]:
                if is_nonlatin(alias):
                    pats.append((alias, re.compile(re.escape(normalize_script(alias))), True))
                else:
                    flags = 0 if (alias.upper() == alias and len(alias) <= 5) else re.IGNORECASE
                    pats.append((alias, re.compile(rf"(?<![\w-]){re.escape(alias)}(?![\w-])", flags), False))
            ctx = []
            for w in spec.get("context", []):
                if is_nonlatin(w):
                    ctx.append((re.compile(re.escape(normalize_script(w))), True))
                else:
                    ctx.append((re.compile(rf"(?<!\w){re.escape(w)}(?!\w)", re.IGNORECASE), False))
            self.entries.append((name, spec, pats, ctx))

    def find(self, text: str, title: str = "", lang: Optional[str] = None,
             score_sentiment: bool = False) -> list[dict]:
        """lang = article language; mentions are sentiment-scored when a model exists for it.
        (score_sentiment=True is the older call style and means English.)"""
        if lang is None and score_sentiment:
            lang = "en"
        norm_text = norm_title = None
        results = []
        for name, spec, pats, ctx in self.entries:
            hits, aliases, in_title = [], [], False
            for alias, p, nonlatin in pats:
                if nonlatin:
                    if norm_text is None:
                        norm_text, norm_title = normalize_script(text), normalize_script(title)
                    src, ttl = norm_text, norm_title
                else:
                    src, ttl = text, title
                found = [(m.start(), m.end(), src) for m in p.finditer(src)]
                if found:
                    hits.extend(found)
                    aliases.append(alias)
                    in_title = in_title or bool(ttl and p.search(ttl))
            if not hits:
                continue
            if ctx:
                if norm_text is None:
                    norm_text, norm_title = normalize_script(text), normalize_script(title)
                if not any(rx.search(norm_text if nl else text) for rx, nl in ctx):
                    continue
            hits.sort(key=lambda h: h[0])
            windows = [src[max(0, st - 160): en + 160] for st, en, src in hits[:5]]
            score, label = (None, "Not scored")
            if lang and scorable(lang):
                vals = [v for v, _ in (sentiment_for(w, lang) for w in windows) if v is not None]
                if vals:
                    score = round(sum(vals) / len(vals), 4)
                    label = polarity_label(score)
            results.append({
                "brand": name, "brand_country": spec.get("country", ""), "sector": spec.get("sector", ""),
                "aliases_matched": ", ".join(aliases), "hit_count": len(hits),
                "in_title": int(in_title), "sentiment_score": score, "sentiment_label": label,
                "snippet": clean_text(windows[0])[:320],
            })
        return results


BRAND_MATCHER = TermMatcher(BRANDS)
TARGET_MATCHER = TermMatcher({label: {"aliases": terms} for label, terms in MONITORING_TARGETS.items()})

CATEGORY_RULES = {
    "AI & Tech": ["artificial intelligence", "machine learning", "deep learning", "generative AI",
                  "LLM", "agentic", "startup", "software", "intelligence artificielle", "akili bandia"],
    "Health": ["health", "hospital", "disease", "malaria", "covid", "HIV", "maternal", "vaccination",
               "clinic", "santé", "hôpital", "afya", "hospitali"],
    "Politics": ["election", "government", "president", "parliament", "senate", "minister", "cabinet",
                 "legislation", "élection", "gouvernement", "uchaguzi", "serikali", "bunge"],
    "Business": ["business", "market", "finance", "economy", "GDP", "inflation", "investment",
                 "IPO", "funding", "profit", "revenue", "économie", "uchumi", "biashara"],
    "Climate & Environment": ["climate", "flood", "floods", "drought", "carbon", "emissions",
                              "renewable", "solar", "climat", "sécheresse", "ukame", "mafuriko"],
    "Education": ["education", "school", "university", "students", "curriculum", "teacher",
                  "scholarship", "éducation", "école", "elimu", "shule", "wanafunzi"],
    "Agriculture": ["agriculture", "farming", "farmers", "crop", "harvest", "food security",
                    "smallholder", "irrigation", "kilimo", "wakulima"],
    "Security & Conflict": ["security", "conflict", "terrorism", "militia", "peacekeeping",
                            "coup", "protest", "protests", "strike", "maandamano"],
}
CATEGORY_RULES = {cat: kws + CATEGORY_WORDS_AFRICAN.get(cat, []) for cat, kws in CATEGORY_RULES.items()}
_CAT_RX = {cat: re.compile(r"(?<!\w)(?:" + "|".join(re.escape(k) for k in kws if not is_nonlatin(k))
                           + r")(?!\w)", re.IGNORECASE)
           for cat, kws in CATEGORY_RULES.items()}
_CAT_RX_NONLATIN = {cat: re.compile("|".join(re.escape(normalize_script(k)) for k in kws if is_nonlatin(k)))
                    for cat, kws in CATEGORY_RULES.items() if any(is_nonlatin(k) for k in kws)}
_AI_RX = re.compile(r"(?<!\w)AI(?!\w)")          # case-sensitive: the v3 " ai " check missed most uses


def classify(text: str) -> str:
    scores = Counter({cat: len(rx.findall(text)) for cat, rx in _CAT_RX.items()})
    if is_nonlatin(text[:2000]):
        norm = normalize_script(text)
        for cat, rx in _CAT_RX_NONLATIN.items():
            scores[cat] += len(rx.findall(norm))
    scores["AI & Tech"] += len(_AI_RX.findall(text))
    best, n = scores.most_common(1)[0]
    return best if n else "General"


def polarity_label(p: float) -> str:
    return "Positive" if p > 0.05 else "Negative" if p < -0.05 else "Neutral"


def sentiment(text: str) -> tuple[float, str]:
    """English lexicon sentiment (TextBlob)."""
    try:
        p = TextBlob(text).sentiment.polarity
    except Exception:
        p = 0.0
    return round(p, 4), polarity_label(p)


def scorable(lang: str) -> bool:
    return lang == "en" or model_sentiment_available()


def sentiment_for(text: str, lang: str) -> tuple[Optional[float], str]:
    """TextBlob for English; the MP_SENTIMENT_MODEL model for other languages (and English if
    MP_SENTIMENT_ALL=1); otherwise 'Not scored' — never a fake Neutral."""
    if lang == "en" and not use_model_for_english():
        return sentiment(text)
    r = model_sentiment(text)
    if r:
        return r
    return sentiment(text) if lang == "en" else (None, "Not scored")


_STOP = {"about", "after", "again", "before", "between", "could", "every", "first", "found", "great",
         "group", "here", "large", "later", "might", "never", "other", "often", "place", "right",
         "should", "since", "small", "still", "their", "there", "these", "thing", "think", "those",
         "three", "under", "until", "where", "which", "while", "world", "would", "years", "your",
         "said", "also", "according", "including", "however", "because", "being", "percent"}


def extract_keywords(text: str, n: int = 8) -> str:
    words = [w for w in re.findall(r"\b[^\W\d_]{5,}\b", text.lower()) if w not in _STOP]
    return ", ".join(w for w, _ in Counter(words).most_common(n))


def detect_lang(text: str, hint: str) -> str:
    detected = ""
    if _lang_detect and len(text) >= 40:
        try:
            detected = _lang_detect(text)
        except Exception:
            detected = ""
    return choose_language(detected, hint)


def ner_orgs(text: str, lang: str = "en") -> list[str]:
    if not text:
        return []
    if lang == "en":
        raw = [e.text for e in NLP(text[:5000]).ents if e.label_ == "ORG"] if NLP else []
    else:
        raw = model_orgs(text, lang)
    orgs = {re.sub(r"^(?:the|The)\s+", "", re.sub(r"\s+", " ", o).strip(" .,'\"’")) for o in raw}
    return sorted(o for o in orgs if 2 <= len(o) <= 80 and not o.isdigit())[:30]


def build_record(status: str, c: Candidate, meta: Optional[dict], final_url: str, cutoff: dt.datetime):
    """Returns (record, mentions, targets, orgs) or a skip reason string."""
    if status in ("ok", "meta_only") and meta:
        title = clean_text(meta.get("title") or c.title_hint)
        text = clean_text(meta.get("text") or meta.get("raw_text") or "")
        summary = clean_text(meta.get("excerpt") or meta.get("description") or "")[:500] or text[:300]
        author = meta.get("author") or ""
        published = parse_date(meta.get("date")) or c.published
        lang_hint = (c.lang if normalize_lang(c.lang) in UNDETECTABLE
                     else meta.get("_page_lang") or meta.get("language") or c.lang)
        url = final_url
    elif c.title_hint:
        title, text, summary, author = c.title_hint, "", c.summary_hint, ""
        published, lang_hint, url = c.published, c.lang, c.url
    else:
        return "empty"
    if not title and not text:
        return "empty"
    if published and published < cutoff:
        return "old"

    info = None if c.tier else source_info(url, c.source)
    tier = c.tier or (info.get("tier", "national") if info else "unlisted")
    source = info["name"] if info else c.source
    country = (info.get("country") if info else "") or c.country

    full = f"{title}\n{summary}\n{text}" if not text else f"{title}\n{text}"
    lang = detect_lang(f"{title} {text[:2000] or summary}", lang_hint)
    score, label = sentiment_for(f"{title}. {text[:1500] or summary}", lang) if scorable(lang) \
        else (None, "Not scored")

    record = {
        "url": url, "source": source, "source_country": country, "tier": tier,
        "domain": host_key(urlparse(url).netloc), "discovered_via": c.via,
        "title": title, "summary": summary, "text": text, "author": author, "language": lang,
        "published_at": iso(published), "collected_at": iso(now_utc()),
        "category": classify(full), "sentiment_score": score, "sentiment_label": label,
        "keywords": extract_keywords(full), "char_count": len(full),
        "content_hash": sha1(text[:3000].lower()) if len(text) >= min_article_chars(text, MIN_TEXT_CHARS) else "",
        "title_hash": sha1(re.sub(r"\W+", " ", title.lower()).strip()) if len(title) >= 25 else "",
    }
    mentions = BRAND_MATCHER.find(full, title, lang=lang)
    targets = [m["brand"] for m in TARGET_MATCHER.find(full)]
    orgs = ner_orgs(full, lang)
    return record, mentions, targets, orgs


# ─────────────────────────────────────────────
# STORAGE (SQLite)
# ─────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS articles(
  id INTEGER PRIMARY KEY,
  url TEXT UNIQUE NOT NULL,
  source TEXT, source_country TEXT, tier TEXT, domain TEXT, discovered_via TEXT,
  title TEXT, summary TEXT, text TEXT, author TEXT, language TEXT,
  published_at TEXT, collected_at TEXT,
  category TEXT, sentiment_score REAL, sentiment_label TEXT, keywords TEXT, char_count INTEGER,
  content_hash TEXT, title_hash TEXT, duplicate_of INTEGER
);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_collected ON articles(collected_at);
CREATE INDEX IF NOT EXISTS idx_articles_chash ON articles(content_hash);
CREATE INDEX IF NOT EXISTS idx_articles_thash ON articles(title_hash);
CREATE TABLE IF NOT EXISTS mentions(
  article_id INTEGER, brand TEXT, brand_country TEXT, sector TEXT, aliases_matched TEXT,
  hit_count INTEGER, in_title INTEGER, sentiment_score REAL, sentiment_label TEXT, snippet TEXT,
  PRIMARY KEY(article_id, brand));
CREATE INDEX IF NOT EXISTS idx_mentions_brand ON mentions(brand);
CREATE TABLE IF NOT EXISTS target_hits(article_id INTEGER, target TEXT, PRIMARY KEY(article_id, target));
CREATE TABLE IF NOT EXISTS orgs(article_id INTEGER, org TEXT, PRIMARY KEY(article_id, org));
CREATE INDEX IF NOT EXISTS idx_orgs_org ON orgs(org);
CREATE TABLE IF NOT EXISTS seen_urls(url TEXT PRIMARY KEY, status TEXT, first_seen TEXT);
CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT, stats TEXT);

-- Brands added from the command line (search.py --track), on top of config.BRANDS
CREATE TABLE IF NOT EXISTS watchlist(
  name TEXT PRIMARY KEY, aliases TEXT, context TEXT, country TEXT, sector TEXT, added_at TEXT);
-- Which version of each brand's aliases has been back-applied to stored articles
CREATE TABLE IF NOT EXISTS brand_backfill(name TEXT PRIMARY KEY, spec_hash TEXT, done_at TEXT);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
-- URLs discovered but not fetched before the time budget ran out; fetched first next run
CREATE TABLE IF NOT EXISTS pending(url TEXT PRIMARY KEY, data TEXT, added_at TEXT);

-- Full-text index over every stored article: this is what makes ANY brand searchable,
-- including ones nobody listed when the article was collected.
CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
  title, text, content='articles', content_rowid='id', tokenize='unicode61 remove_diacritics 2');
CREATE TRIGGER IF NOT EXISTS articles_fts_ai AFTER INSERT ON articles BEGIN
  INSERT INTO articles_fts(rowid, title, text) VALUES (new.id, new.title, new.text);
END;
CREATE TRIGGER IF NOT EXISTS articles_fts_ad AFTER DELETE ON articles BEGIN
  INSERT INTO articles_fts(articles_fts, rowid, title, text) VALUES ('delete', old.id, old.title, old.text);
END;
CREATE TRIGGER IF NOT EXISTS articles_fts_au AFTER UPDATE OF title, text ON articles BEGIN
  INSERT INTO articles_fts(articles_fts, rowid, title, text) VALUES ('delete', old.id, old.title, old.text);
  INSERT INTO articles_fts(rowid, title, text) VALUES (new.id, new.title, new.text);
END;
"""

ARTICLE_COLS = ["url", "source", "source_country", "tier", "domain", "discovered_via", "title", "summary", "text",
                "author", "language", "published_at", "collected_at", "category", "sentiment_score",
                "sentiment_label", "keywords", "char_count", "content_hash", "title_hash", "duplicate_of"]
MENTION_COLS = ["brand", "brand_country", "sector", "aliases_matched", "hit_count", "in_title",
                "sentiment_score", "sentiment_label", "snippet"]


def db_connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
    if cols and "tier" not in cols:                 # database created by an earlier v4 build
        conn.execute("ALTER TABLE articles ADD COLUMN tier TEXT")
    conn.executescript(SCHEMA)
    if not conn.execute("SELECT 1 FROM meta WHERE key = 'fts_built'").fetchone():
        # First run on a database that predates the full-text index: index what's already there
        conn.execute("INSERT INTO articles_fts(articles_fts) VALUES ('rebuild')")
        conn.execute("INSERT INTO meta VALUES ('fts_built', ?)", (iso(now_utc()),))
        conn.commit()
    return conn


def already_seen(conn: sqlite3.Connection, urls: list[str]) -> set[str]:
    seen = set()
    for batch in chunked(urls, 500):
        marks = ",".join("?" * len(batch))
        seen.update(r[0] for r in conn.execute(f"SELECT url FROM seen_urls WHERE url IN ({marks})", batch))
        seen.update(r[0] for r in conn.execute(f"SELECT url FROM articles WHERE url IN ({marks})", batch))
    return seen


def mark_seen(conn: sqlite3.Connection, url: str, status: str):
    conn.execute("INSERT OR IGNORE INTO seen_urls(url, status, first_seen) VALUES(?,?,?)",
                 (url, status, iso(now_utc())))


def save_article(conn, record: dict, mentions: list, targets: list, orgs: list) -> bool:
    dup = None
    if record["content_hash"] or record["title_hash"]:
        row = conn.execute(
            "SELECT id FROM articles WHERE (content_hash != '' AND content_hash = ?) "
            "OR (title_hash != '' AND title_hash = ?) ORDER BY id LIMIT 1",
            (record["content_hash"], record["title_hash"])).fetchone()
        dup = row[0] if row else None
    record["duplicate_of"] = dup          # syndicated copy: kept for reach, flagged for unique counts
    cur = conn.execute(
        f"INSERT OR IGNORE INTO articles({','.join(ARTICLE_COLS)}) VALUES({','.join('?' * len(ARTICLE_COLS))})",
        [record[k] for k in ARTICLE_COLS])
    if cur.rowcount == 0:
        return False
    aid = cur.lastrowid
    _insert_mentions(conn, aid, mentions)
    conn.executemany("INSERT OR IGNORE INTO target_hits VALUES(?,?)", [(aid, t) for t in targets])
    conn.executemany("INSERT OR IGNORE INTO orgs VALUES(?,?)", [(aid, o) for o in orgs])
    return True


def _insert_mentions(conn, article_id: int, mentions: list):
    conn.executemany(
        f"INSERT OR IGNORE INTO mentions(article_id,{','.join(MENTION_COLS)}) "
        f"VALUES(?,{','.join('?' * len(MENTION_COLS))})",
        [[article_id] + [m[k] for k in MENTION_COLS] for m in mentions])


# ─────────────────────────────────────────────
# BRAND REGISTRY = config.BRANDS + watchlist, back-applied to stored articles
# ─────────────────────────────────────────────
def load_watchlist(conn: sqlite3.Connection) -> dict:
    out = {}
    for name, aliases, context, country, sector in conn.execute(
            "SELECT name, aliases, context, country, sector FROM watchlist"):
        spec = {"aliases": json.loads(aliases or "[]") or [name], "country": country or "",
                "sector": sector or "", "watchlist": True}
        ctx = json.loads(context or "[]")
        if ctx:
            spec["context"] = ctx
        out[name] = spec
    return out


def active_registry(conn: sqlite3.Connection) -> dict:
    reg = dict(BRANDS)
    reg.update(load_watchlist(conn))
    return reg


def use_registry(registry: dict):
    global BRAND_MATCHER
    BRAND_MATCHER = TermMatcher(registry)


def fts_phrase(term: str) -> str:
    return '"' + term.replace('"', '""') + '"'


def _spec_hash(spec: dict) -> str:
    return sha1(json.dumps({k: spec.get(k) for k in ("aliases", "context")}, sort_keys=True))


def backfill_brand(conn: sqlite3.Connection, name: str, spec: dict) -> int:
    """Re-apply one brand to every stored article (the 'search the archive for a new brand' step).
    FTS narrows the candidates; the regex matcher then applies case and context rules."""
    matcher = TermMatcher({name: spec})
    conn.execute("DELETE FROM mentions WHERE brand = ?", (name,))
    latin = [a for a in spec["aliases"] if not is_nonlatin(a)]
    rows = []
    if latin:
        q = " OR ".join(fts_phrase(a) for a in latin)
        rows += conn.execute(
            "SELECT a.id, a.title, a.summary, a.text, a.language FROM articles_fts "
            "JOIN articles a ON a.id = articles_fts.rowid WHERE articles_fts MATCH ?", (q,)).fetchall()
    if len(latin) < len(spec["aliases"]):
        rows += conn.execute(
            "SELECT id, title, summary, text, language FROM articles "
            "WHERE language IN ('am', 'ti', 'ar', 'und', '') OR language IS NULL").fetchall()
    n, done = 0, set()
    for aid, title, summary, text, lang in rows:
        if aid in done:
            continue
        done.add(aid)
        title, text = title or "", text or ""
        full = f"{title}\n{text}" if text else f"{title}\n{summary or ''}"
        found = matcher.find(full, title, lang=lang or "und")
        if found:
            _insert_mentions(conn, aid, found)
            n += 1
    conn.execute("INSERT OR REPLACE INTO brand_backfill VALUES (?,?,?)", (name, _spec_hash(spec), iso(now_utc())))
    return n


def sync_backfills(conn: sqlite3.Connection, registry: dict) -> dict:
    """Any brand that is new, or whose aliases/context changed since last run, is back-applied
    to the whole archive — so adding a brand to config.py also covers past coverage."""
    done = dict(conn.execute("SELECT name, spec_hash FROM brand_backfill"))
    changed = {n: s for n, s in registry.items() if done.get(n) != _spec_hash(s)}
    has_articles = conn.execute("SELECT 1 FROM articles LIMIT 1").fetchone()
    results = {}
    for name, spec in changed.items():
        if has_articles:
            results[name] = backfill_brand(conn, name, spec)
        else:
            conn.execute("INSERT OR REPLACE INTO brand_backfill VALUES (?,?,?)",
                         (name, _spec_hash(spec), iso(now_utc())))
    conn.commit()
    hits = {n: k for n, k in results.items() if k}
    if results:
        log.info(f"[Backfill] {len(results)} new/changed brands applied to archive; with past coverage: {hits}")
    return results


def export_csvs(conn: sqlite3.Connection):
    since = iso(now_utc() - dt.timedelta(days=EXPORT_DAYS))
    # Same columns as v3's daily_news.csv so existing dashboards keep working (+ new ones at the end)
    rows = conn.execute("""
        SELECT a.source, a.title, a.summary, a.url AS link, a.author, a.published_at AS published_date,
               a.collected_at AS collected_date, a.category,
               (SELECT group_concat(target, ', ') FROM target_hits t WHERE t.article_id = a.id) AS monitoring_targets,
               a.sentiment_score, a.sentiment_label,
               (SELECT group_concat(brand, ', ') FROM mentions m WHERE m.article_id = a.id) AS companies_mentioned,
               (SELECT group_concat(org, ', ') FROM orgs o WHERE o.article_id = a.id) AS companies_detected,
               a.keywords, a.char_count, a.language, a.source_country, a.tier, a.discovered_via,
               a.duplicate_of
        FROM articles a WHERE a.collected_at >= ?
        ORDER BY COALESCE(NULLIF(a.published_at, ''), a.collected_at) DESC""", (since,))
    _write_csv(CSV_EXPORT, rows)
    rows = conn.execute("""
        SELECT m.brand, m.brand_country, m.sector, a.published_at, a.source, a.source_country, a.tier, a.language,
               a.title, a.url, m.hit_count, m.in_title, m.sentiment_score, m.sentiment_label, m.snippet,
               a.duplicate_of
        FROM mentions m JOIN articles a ON a.id = m.article_id
        WHERE a.collected_at >= ? ORDER BY m.brand, a.published_at DESC""", (since,))
    _write_csv(MENTIONS_EXPORT, rows)


def _write_csv(path: str, cursor):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow([d[0] for d in cursor.description])
        w.writerows(cursor)


# ─────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────
def make_client(transport: Optional[httpx.AsyncBaseTransport] = None) -> httpx.AsyncClient:
    kwargs = dict(
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en,fr;q=0.8,sw;q=0.7,*;q=0.5"},
        timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
        limits=httpx.Limits(max_connections=CONCURRENCY * 2, max_keepalive_connections=CONCURRENCY),
    )
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.AsyncClient(**kwargs)


def _cand_to_json(c: Candidate) -> str:
    d = dict(c.__dict__)
    d["published"] = iso(c.published) if c.published else None
    return json.dumps(d)


def _cand_from_json(s_: str) -> Candidate:
    d = json.loads(s_)
    d["published"] = parse_date(d.get("published"))
    return Candidate(**d)


def load_pending(conn: sqlite3.Connection) -> list[Candidate]:
    rows = conn.execute("SELECT data FROM pending").fetchall()
    conn.execute("DELETE FROM pending")
    conn.commit()
    limit = now_utc() - dt.timedelta(days=PENDING_MAX_AGE_DAYS)
    out = []
    for (d,) in rows:
        try:
            c = _cand_from_json(d)
        except Exception:
            continue
        if parse_date(c.queued_at) and parse_date(c.queued_at) < limit:
            continue                     # waited too long — the story is stale now
        out.append(c)
    return out


def save_pending(conn: sqlite3.Connection, cands: list[Candidate]):
    now = iso(now_utc())
    for c in cands:
        c.queued_at = c.queued_at or now
    conn.executemany("INSERT OR REPLACE INTO pending VALUES (?,?,?)",
                     [(c.url, _cand_to_json(c), c.queued_at) for c in cands])
    conn.commit()


async def process_candidates(conn: sqlite3.Connection, f: Fetcher, cands: list[Candidate],
                             cutoff: dt.datetime, stats: Counter, deadline: Optional[float] = None) -> Counter:
    """Dedupe -> skip already-seen -> fetch -> extract -> analyse -> store.
    One worker per site fetches that site's URLs in order (so a slow site or a long Crawl-delay
    never holds up the others); a single writer analyses and saves results as they arrive.
    When the deadline passes, workers stop and unfetched URLs go to the pending queue."""
    deadline = deadline or (time.monotonic() + 10 ** 9)
    unique: dict[str, Candidate] = {}
    for c in cands:
        if c.url and c.url not in unique:
            unique[c.url] = c
    seen = already_seen(conn, list(unique))
    todo = [c for u, c in unique.items() if u not in seen]
    stats["candidates"] += len(unique)
    stats["already_seen"] += len(seen)

    hints = [c for c in todo if not c.fetch]
    by_host: dict[str, list] = defaultdict(list)
    for c in todo:
        if c.fetch:
            by_host[urlparse(c.url).netloc.lower()].append(c)

    # How many URLs each site can get this run: its cap, and what its Crawl-delay allows in the time left
    leftover: list[Candidate] = []
    queues: dict[str, list] = {}
    total = 0
    remaining = max(0.0, deadline - time.monotonic())
    for host, lst in sorted(by_host.items(), key=lambda kv: -len(kv[1])):
        lst.sort(key=lambda c: (c.via != "gdelt", -(c.published or EPOCH).timestamp()))
        allow = min(MAX_PER_HOST, max(1, int(remaining * 0.9 / max(f.host_delay(lst[0].url), 0.5))))
        queues[host], over = lst[:allow], lst[allow:]
        leftover += over
        total += len(queues[host])
    if total > MAX_FETCH:                      # trim the biggest queues first
        for host in sorted(queues, key=lambda h: -len(queues[h])):
            while total > MAX_FETCH and len(queues[host]) > 1:
                leftover.append(queues[host].pop())
                total -= 1
    log.info(f"[Stage 1] {len(unique)} unique candidates, {len(seen)} already seen, "
             f"{total} to fetch across {len(queues)} sites, {len(hints)} headline-only, "
             f"{len(leftover)} queued for later runs")

    log.info("[Stage 2] Fetch + analyse")
    out_q: asyncio.Queue = asyncio.Queue(maxsize=CONCURRENCY * 4)

    async def worker(host: str, items: list):
        for i, c in enumerate(items):
            if time.monotonic() > deadline:
                leftover.extend(items[i:])
                stats["budget_hit"] = 1
                return
            try:
                await out_q.put(await fetch_candidate(f, c))
            except Exception as e:
                log.debug(f"[fetch] {c.url}: {e!r}")
                stats["fetch_exceptions"] += 1

    async def feed_hints():
        for c in hints:
            await out_q.put(("hint", c, None, c.url))

    async def producers():
        await asyncio.gather(feed_hints(), *(worker(h, q) for h, q in queues.items()))
        await out_q.put(None)

    prod = asyncio.create_task(producers())
    since_commit, last_log = 0, time.monotonic()
    while True:
        item = await out_q.get()
        if item is None:
            break
        status, c, meta, final = item
        stats[f"fetch_{status}"] += 1
        try:
            built = await asyncio.to_thread(build_record, status, c, meta, final, cutoff)
        except Exception as e:
            log.error(f"[Analyse] {c.url}: {e!r}")
            stats["analyse_errors"] += 1
            continue
        if isinstance(built, str):
            stats[f"skip_{built}"] += 1
            if not is_transient(status):
                mark_seen(conn, c.url, f"{status}/{built}")
        else:
            record, mentions, targets, orgs = built
            if save_article(conn, record, mentions, targets, orgs):
                stats["saved"] += 1
                stats["brand_mentions"] += len(mentions)
                stats["saved_with_brand"] += bool(mentions)
            mark_seen(conn, c.url, status)
            if final != c.url:
                mark_seen(conn, final, status)
        since_commit += 1
        if since_commit >= COMMIT_EVERY:
            conn.commit()
            since_commit = 0
        if time.monotonic() - last_log > 60:
            mins_left = max(0, (deadline - time.monotonic()) / 60)
            log.info(f"[Stage 2] saved {stats['saved']} so far, {mins_left:.0f} min of budget left")
            last_log = time.monotonic()
    await prod
    conn.commit()
    if leftover:
        save_pending(conn, leftover)
        log.info(f"[Stage 2] {len(leftover)} URLs queued for the next run")
    stats["queued_for_next_run"] += len(leftover)
    return stats


async def _discover_all(f: Fetcher, sources: list, cutoff: dt.datetime, registry: dict,
                        use_indexes: bool, stats: Counter, diag: dict) -> list[Candidate]:
    """All outlets and the global indexes in parallel, each outlet with its own timeout and the
    whole stage with a budget; whatever has finished when the budget ends is used."""
    disc_deadline = time.monotonic() + DISCOVERY_BUDGET_MIN * 60

    async def one(src):
        try:
            return await asyncio.wait_for(discover_source(f, src, cutoff, diag), SOURCE_DISCOVERY_TIMEOUT)
        except asyncio.TimeoutError:
            diag[src["name"]] = f"discovery timed out after {SOURCE_DISCOVERY_TIMEOUT:.0f}s (slow site)"
            log.info(f"[Discover] {src['name']}: timed out")
            return []

    tasks = {asyncio.create_task(one(s_)): s_["name"] for s_ in sources}
    if use_indexes:
        if ENABLE_GDELT:
            terms = list(registry) + [t for ts in MONITORING_TARGETS.values() for t in ts]
            tasks[asyncio.create_task(discover_gdelt(f, terms, deadline=disc_deadline - 90))] = "GDELT"
        if ENABLE_GNEWS:
            tasks[asyncio.create_task(discover_google_news(f, registry))] = "Google News"
        if NEWS_API_KEY:
            tasks[asyncio.create_task(discover_newsapi(f, cutoff))] = "NewsAPI"
    done, not_done = await asyncio.wait(tasks, timeout=max(1.0, disc_deadline - time.monotonic()))
    for t in not_done:
        t.cancel()
        diag.setdefault(tasks[t], "not finished within the discovery budget")
    if not_done:
        log.warning(f"[Stage 1] discovery budget reached; unfinished: {', '.join(tasks[t] for t in not_done)}")
    cands: list[Candidate] = []
    for t in done:
        if t.exception():
            log.error(f"[Discover] {tasks[t]} failed: {t.exception()!r}")
            stats["source_errors"] += 1
        else:
            cands.extend(t.result())
    return cands


async def run(sources: Optional[list] = None, transport: Optional[httpx.AsyncBaseTransport] = None,
              use_indexes: bool = True, db_path: str = DB_PATH) -> Counter:
    sources = [s_ for s_ in (SOURCES if sources is None else sources) if s_.get("crawl", True)]
    started = now_utc()
    cutoff = started - dt.timedelta(hours=LOOKBACK_HOURS)
    t0 = time.monotonic()
    deadline = t0 + TIME_BUDGET_MIN * 60
    conn = db_connect(db_path)
    stats: Counter = Counter()
    diag: dict = {}
    finished = False

    try:
        registry = active_registry(conn)
        use_registry(registry)
        sync_backfills(conn, registry)

        log.info("=" * 60)
        log.info(f"MediaPulse Africa Pipeline v5 — {len(sources)} outlets, {len(registry)} tracked brands, "
                 f"lookback {LOOKBACK_HOURS}h, time budget {TIME_BUDGET_MIN:.0f} min")
        log.info("=" * 60)

        pending = load_pending(conn)
        if pending:
            log.info(f"[Stage 0] {len(pending)} URLs carried over from the previous run")

        async with make_client(transport) as client:
            f = Fetcher(client)
            log.info("[Stage 1] Discovery")
            cands = pending + await _discover_all(f, sources, cutoff, registry, use_indexes, stats, diag)
            await process_candidates(conn, f, cands, cutoff, stats, deadline=deadline)
            stats["http_requests"] = f.requests
        finished = True
    finally:
        export_csvs(conn)
        elapsed = round(time.monotonic() - t0, 1)
        stats["elapsed_s"] = elapsed
        conn.execute("INSERT INTO runs(started_at, finished_at, stats) VALUES(?,?,?)",
                     (iso(started), iso(now_utc()), json.dumps(dict(stats))))
        conn.commit()
        per_source = conn.execute(
            "SELECT source, COUNT(*) FROM articles WHERE collected_at >= ? GROUP BY source ORDER BY 2 DESC",
            (iso(started),)).fetchall()
        conn.close()

    log.info("=" * 60)
    if diag:
        log.info(f"Outlets with nothing collected this run ({len(diag)}) — fix or remove these in SOURCES:")
        for name, why in sorted(diag.items()):
            log.info(f"  - {name}: {why}")
    if per_source:
        log.info("Saved this run by outlet: " + ", ".join(f"{s_} {n}" for s_, n in per_source[:25])
                 + (" …" if len(per_source) > 25 else ""))
    q = stats.get("queued_for_next_run", 0)
    why = ("time budget reached" if stats.get("budget_hit")
           else f"busy sites hit the {MAX_PER_HOST}-per-site limit")
    budget_note = f" ({why}; {q} URLs queued for the next run)" if q else ""
    log.info(f"Pipeline complete in {elapsed}s — saved {stats['saved']} articles, "
             f"{stats['brand_mentions']} brand mentions{budget_note}")
    log.info(f"Stats: {dict(stats)}")
    log.info(f"Outputs: {db_path}, {CSV_EXPORT}, {MENTIONS_EXPORT}")
    log.info("=" * 60)
    return stats


async def collect_terms(terms: list[str], days: int, transport: Optional[httpx.AsyncBaseTransport] = None,
                        db_path: str = DB_PATH) -> Counter:
    """On-demand collection for brands you're not crawling for yet: asks GDELT and Google News
    (every African edition) for the terms, then fetches and stores full articles."""
    cutoff = now_utc() - dt.timedelta(days=days)
    conn = db_connect(db_path)
    use_registry(active_registry(conn))
    stats: Counter = Counter()
    async with make_client(transport) as client:
        f = Fetcher(client)
        cands = []
        if ENABLE_GDELT:
            cands += await discover_gdelt(f, terms, hours=days * 24, deadline=time.monotonic() + 600)
        if ENABLE_GNEWS:
            cands += await discover_google_news(f, {t: {} for t in terms}, days=days, all_editions=True)
        await process_candidates(conn, f, cands, cutoff, stats)
        stats["http_requests"] = f.requests
    conn.close()
    return stats


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        log.critical(f"Pipeline terminated: {e}", exc_info=True)
        raise SystemExit(1)
