import os
import functools, sys
print = functools.partial(print, flush=True, file=sys.stderr)
import re
import json
import random
import requests
from datetime import datetime, timezone
from telethon import TelegramClient, events
from pymongo import MongoClient
from upstash_redis import Redis
from telethon.sessions import StringSession 
from dotenv import load_dotenv

# === 🔧 ENV YÜKLE ===
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
REDIS_TOKEN = os.getenv("REDIS_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

if not REDIS_URL or not REDIS_TOKEN or not MONGO_URI:
    raise RuntimeError("🚨 REDIS_URL, REDIS_TOKEN ve MONGO_URI tanımla kral!")

# === 🔌 BAĞLANTILAR ===
redis_db = Redis(url=REDIS_URL, token=REDIS_TOKEN)
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["boostdb"]
mongo_col = mongo_db["bmcnewtokens"]

# === 🤖 TELEGRAM ===
api_id = int(os.getenv("TG_API_ID", 26678625))
api_hash = os.getenv("TG_API_HASH", "922bfce8ffaf0a253a28e569105b70f8")
CMC_CHANNEL = -1001292331458
CG_CHANNEL = -1001559069277
MAKN_CHANNEL = os.getenv("TG_CHANNEL", "makntrendin")

TG_SESSION = os.getenv("TG_SESSION")

if not TG_SESSION:
    raise RuntimeError("🚨 TG_SESSION tanımlı değil kral! Lütfen .env içine string session ekle.")

client = TelegramClient(StringSession(TG_SESSION), api_id, api_hash)

# === 🔍 SABİTLER ===
pattern = re.compile(
    r"(?:CA:|Ca:|contract:|Contract:|🧾|Address:)\s*("
    r"(?:0x[a-fA-F0-9]{40})"
    r"|[0-9a-zA-Z:\-\._]{20,120}"
    r")"
)
COINGECKO = "https://api.coingecko.com/api/v3"
DEXSCREENER = "https://api.dexscreener.com/latest/dex/search"
CMC_URL = "https://s3.coinmarketcap.com/generated/core/crypto/cryptos.json"

LIQUIDITY_THRESHOLD = 300_000.0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/117.0",
    "curl/7.79.1",
    "PostmanRuntime/7.32.2",
]

def safe_request(url, params=None):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=12)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"\033[33m[!] HTTP {resp.status_code} for {url}\033[0m")
    except Exception as e:
        print(f"\033[31m[x] Request error ({url}): {e}\033[0m")
    return None

def normalize(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    addr = addr.strip()
    return addr.lower() if addr.startswith("0x") else addr

def build_cmc_contract_map():
    data = safe_request(CMC_URL)
    if not data:
        return {}
    mapping = {}
    entries = data.get("values", data)
    for entry in entries:
        if isinstance(entry, list) and len(entry) >= 8:
            slug = entry[3]
            contracts = entry[7]
            if isinstance(contracts, list):
                for c in contracts:
                    if isinstance(c, str):
                        mapping[normalize(c)] = slug
    return mapping

CMC_MAP = build_cmc_contract_map()

# DexScreener chainId -> CoinGecko platform mapping (geniş liste)
DEX_TO_CG = {
    "ethereum": "ethereum",
    "eth": "ethereum",
    "bsc": "bsc",
    "binance-smart-chain": "bsc",
    "bnb": "bsc",
    "arbitrum": "arbitrum-one",
    "arbitrum-one": "arbitrum-one",
    "polygon": "polygon-pos",
    "polygon-pos": "polygon-pos",
    "matic": "polygon-pos",
    "avalanche": "avalanche",
    "avax": "avalanche",
    "base": "base",
    "optimism": "optimism",
    "tron": "tron",
    "solana": "solana",
    "sui": "sui",
    "aptos": "aptos",
    "ton": "ton",
    "fantom": "fantom",
    "cronos": "cronos",
    "linea": "linea",
    "mantle": "mantle",
    "zksync": "zksync",
    "blast": "blast",
    "scroll": "scroll",
    "moonbeam": "moonbeam",
    "celo": "celo",
    "hedera": "hedera",
}

def get_token_id(contract, dex_data=None, symbol_hint=None):
    platform_tried = []
    if dex_data:
        dex_chain = (dex_data.get("chainId") or "").lower()
        if dex_chain:
            cg_platform = DEX_TO_CG.get(dex_chain, dex_chain)
            platform_tried.append(cg_platform)
            url = f"{COINGECKO}/coins/{cg_platform}/contract/{contract}"
            data = safe_request(url)
            if data and data.get("id"):
                return data.get("id")
    coin_list = safe_request(f"{COINGECKO}/coins/list")
    if coin_list and symbol_hint:
        hint = symbol_hint.lower()
        for coin in coin_list:
            if coin.get("symbol", "").lower() == hint:
                return coin.get("id")
    return None

def get_token_info(coin_id):
    if not coin_id:
        return None
    url = f"{COINGECKO}/coins/{coin_id}"
    params = {"localization": "false", "tickers": "true", "market_data": "false"}
    return safe_request(url, params=params)

def get_dexscreener_info(contract):
    r = safe_request(f"{DEXSCREENER}?q={contract}")
    if r and "pairs" in r and r["pairs"]:
        pairs = r["pairs"]
        main_pair = max(pairs, key=lambda x: (x.get("liquidity", {}) or {}).get("usd", 0))
        dex_ids = []
        for p in pairs:
            dex = p.get("dexId", "")
            if isinstance(dex, str):
                dex = dex.lower()
            if dex and dex not in dex_ids:
                dex_ids.append(dex)
        main_pair["dexIds"] = dex_ids
        return main_pair
    print(f"\033[33m[!] Dexscreener sonucu yok: {contract}\033[0m")
    return None

def safe_first(lst):
    return lst[0] if isinstance(lst, list) and len(lst) > 0 else ""

# --- Bu fonksiyon: örnek verdiğin tüm alanları garanti eder ---
def ensure_full_schema(obj: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    template = {
        "name": "",
        "symbol": "",
        "contracts": [],
        "explorerLinks": [],
        "tokenType": "Utility",
        "description": "",
        "logo": "",
        "banner": "",
        "website": "",
        "twitter": "",
        "farcaster": "",
        "discordContact": "",
        "email": "",
        "medium": "",
        "youtube": "",
        "linkedin": "",
        "coingecko": "",
        "coinmarketcap": "",
        "github": "",
        "whitepaper": "",
        "reddit": None,
        "facebook": "",
        "instagram": "",
        "tiktok": "",
        "mobileApp": {"android": "", "ios": ""},
        "audits": [],
        "partners": [],
        "kycs": [],
        "teamMembers": [],
        "news": [],
        "nftCollections": [],
        "dappLinks": [],
        "tokenLocks": [],
        "totalSupply": "",
        "roadmap": [],
        "airdrops": [],
        "telegramLinks": [],
        "telegramChannels": [],
        "exchanges": [],
        "status": "approved",
        "source": "user",
        "isTrending": False,
        "trendScore": 0,
        "article": "",
        "dexIds": [],
        "lastProfileUpdate": now,
        "boosts": [],
        "createdAt": now,
        "updatedAt": now,
        "__v": 0
    }

    out = {}
    for k, v in template.items():
        if k in obj and obj[k] is not None:
            out[k] = obj[k]
        else:
            out[k] = (list(v) if isinstance(v, list) else dict(v) if isinstance(v, dict) else v)

    contracts = obj.get("contracts", []) if isinstance(obj.get("contracts", []), list) else []
    normalized_contracts = []
    for c in contracts:
        if isinstance(c, dict):
            addr = c.get("address", "") or ""
            chain = c.get("chain", "") or ""
            if isinstance(addr, str) and addr.startswith("0x"):
                addr = addr.lower()
            normalized_contracts.append({"address": addr, "chain": chain})
    if not normalized_contracts:
        single = obj.get("contracts")
        if isinstance(single, str):
            addr = single
            if addr.startswith("0x"):
                addr = addr.lower()
            normalized_contracts = [{"address": addr, "chain": obj.get("chain", "") or ""}]
    out["contracts"] = normalized_contracts

    ts = obj.get("updatedAt") or obj.get("createdAt") or now
    out["lastProfileUpdate"] = ts
    out["createdAt"] = out.get("createdAt") or now
    out["updatedAt"] = out.get("updatedAt") or now
    out["dexIds"] = obj.get("dexIds", out["dexIds"]) or out["dexIds"]
    out["exchanges"] = obj.get("exchanges", []) or out["exchanges"]

    for k in obj:
        if k not in out:
            out[k] = obj[k]

    return out

def normalize_json(contract, coin_data, dex_data):
    now = datetime.now(timezone.utc)
    name, symbol, logo, description, banner = "", "", "", "", ""
    links = {}

    if coin_data:
        name = coin_data.get("name", "") or name
        symbol = coin_data.get("symbol", "") or symbol
        logo = (coin_data.get("image") or {}).get("large", "") or logo
        description = (coin_data.get("description") or {}).get("en", "") or description
        links = coin_data.get("links", {}) or {}

    if dex_data:
        base = dex_data.get("baseToken", {}) or {}
        info = dex_data.get("info", {}) or {}

        if not name:
            name = base.get("name", "")
        if not symbol:
            symbol = base.get("symbol", "")
        if not logo:
            logo = info.get("imageUrl") or dex_data.get("image", "") or logo
        banner = info.get("header", "") or banner
        if not description:
            description = info.get("description", "") or description
                # --- 🕸️ DexScreener'dan Website linkini de çek ---
    website_from_dex = ""
    info_websites = info.get("websites", [])
    if isinstance(info_websites, list):
        for w in info_websites:
            if not isinstance(w, dict):
                continue
            label = w.get("label", "").lower()
            url = w.get("url", "")
            if not url:
                continue
            if any(k in label for k in ["website", "official", "app", "home"]):
                website_from_dex = url
                break
            if not website_from_dex:
                website_from_dex = url

    twitter = ""
    telegram_links = []

    if links.get("twitter_screen_name"):
        twitter = f"https://twitter.com/{links.get('twitter_screen_name')}"
    if links.get("telegram_channel_identifier"):
        tg_user = links.get("telegram_channel_identifier")
        tg_url = f"https://t.me/{tg_user}" if "t.me" not in tg_user else tg_user
        telegram_links.append({"label": "Official", "url": tg_url})

    socials = (dex_data.get("info", {}).get("socials", []) if dex_data else [])
    for s in socials:
        if not isinstance(s, dict):
            continue
        s_type = s.get("type", "").lower()
        s_url = s.get("url", "")
        if s_type == "twitter" or "x.com" in s_url or "twitter.com" in s_url:
            twitter = s_url
        elif s_type == "telegram" and "t.me" in s_url:
            telegram_links.append({"label": "Group", "url": s_url})

    exchanges = []
    if coin_data and "tickers" in coin_data:
        for t in coin_data["tickers"]:
            trade_url = t.get("trade_url")
            if trade_url:
                exchanges.append({
                    "exchangeName": (t.get("market") or {}).get("name", ""),
                    "exchangeUrl": trade_url
                })

    chain = (dex_data.get("chainId") if dex_data else "") or coin_data.get("asset_platform_id", "")
    cmc_slug = CMC_MAP.get(normalize(contract), "")
    cmc_link = f"https://coinmarketcap.com/currencies/{cmc_slug}/" if cmc_slug else ""

    base_contract_address = (dex_data.get("baseToken") or {}).get("address") if dex_data else contract
    address_field = base_contract_address or contract
    if isinstance(address_field, str) and address_field.startswith("0x"):
        address_field = address_field.lower()

    obj = {
        "name": name,
        "symbol": symbol,
        "contracts": [{"address": address_field, "chain": chain}],
        "description": description,
        "logo": logo,
        "banner": banner,
        "website": website_from_dex or (safe_first(links.get("homepage", [])) if links else ""),
        "twitter": twitter,
        "telegramLinks": telegram_links,
        "exchanges": exchanges,
        "coinmarketcap": cmc_link,
        "coingecko": f"https://www.coingecko.com/en/coins/{coin_data.get('id')}" if coin_data else "",
        "dexIds": dex_data.get("dexIds", []) if dex_data else [],
        "createdAt": now.isoformat(),
        "updatedAt": now.isoformat(),
    }
    return obj

def save_to_databases(obj_partial, dex_data):
    full_obj = ensure_full_schema(obj_partial)
    contracts = full_obj.get("contracts", [])
    if not contracts or not isinstance(contracts, list) or not contracts[0].get("address"):
        print(f"\033[31m[x] Kaydetme başarısız: primary contract yok veya hatalı.\033[0m")
        return

    primary = contracts[0]
    contract_addr = primary.get("address")
    chain = primary.get("chain", "")

    exists = mongo_col.find_one({"contracts.address": contract_addr, "contracts.chain": chain})
    if exists:
        print(f"\033[33m⚠️ SKIP: {full_obj.get('symbol') or full_obj.get('name')} zaten kayıtlı ({chain}:{contract_addr}) — atlandı.\033[0m")
        return

    try:
        res = mongo_col.insert_one(full_obj)
        print(f"\033[32m[✓] Mongo'ya kaydedildi: {full_obj.get('symbol') or full_obj.get('name')} ({chain}:{contract_addr}) — _id: {res.inserted_id}\033[0m")
    except Exception as e:
        print(f"\033[31m[x] Mongo kayıt hatası: {e}\033[0m")
        return

    try:
        if not dex_data:
            dex_data = get_dexscreener_info(contract_addr)
        base = (dex_data.get("baseToken") if dex_data else {}) or {}
        chain_ = dex_data.get("chainId", "") or chain
        contract_addr_ = base.get("address") or contract_addr

        def calc_age(timestamp_ms):
            try:
                age_days = (datetime.now(timezone.utc) - datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)).days
                if age_days < 1:
                    return "new"
                elif age_days < 30:
                    return f"{age_days}d"
                elif age_days < 365:
                    return f"{age_days // 30}m"
                else:
                    return f"{age_days // 365}y"
            except:
                return "unknown"

        redis_obj = {
            "name": base.get("name") or full_obj.get("name"),
            "symbol": base.get("symbol") or full_obj.get("symbol"),
            "chain": chain_,
            "contract": (contract_addr_.lower() if isinstance(contract_addr_, str) else contract_addr_),
            "priceUsd": float(dex_data.get("priceUsd", 0) or 0) if dex_data else 0,
            "priceChange": dex_data.get("priceChange", {}) if dex_data else {},
            "liquidityUsd": float((dex_data.get("liquidity") or {}).get("usd", 0) or 0) if dex_data else 0,
            "fdv": float(dex_data.get("fdv", 0) or 0) if dex_data else 0,
            "marketCap": float(dex_data.get("marketCap", 0) or 0) if dex_data else 0,
            "pairCreatedAt": dex_data.get("pairCreatedAt", 0) if dex_data else 0,
            "age": calc_age(dex_data.get("pairCreatedAt", 0)) if dex_data else "unknown",
            "txns": ((dex_data.get("txns") or {}).get("h24", {}).get("buys", 0) + (dex_data.get("txns") or {}).get("h24", {}).get("sells", 0)) if dex_data else 0,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        }

        redis_key = f"{chain_}:{(contract_addr_.lower() if isinstance(contract_addr_, str) else contract_addr_)}"
        redis_db.hset("bmcnewtokens", redis_key, json.dumps(redis_obj, ensure_ascii=False))
        print(f"\033[36m[✓] Redis’e kaydedildi ({redis_key})\033[0m")
    except Exception as e:
        print(f"\033[31m[x] Redis kayıt hatası: {e}\033[0m")

# === TELEGRAM HANDLER ===
LISTEN_CHATS = [CMC_CHANNEL, CG_CHANNEL, MAKN_CHANNEL]

@client.on(events.NewMessage(chats=LISTEN_CHATS))
async def handler(event):
    chat_id = getattr(event, "chat_id", None) or getattr(event.message, "chat_id", None)
    text = getattr(event.message, "message", "") or getattr(event, "text", "") or ""
    match = pattern.search(text)
    if not match:
        return

    contract_raw = match.group(1).strip()
    contract = contract_raw
    print(f"\n\033[35m[+] Yeni contract bulundu ({chat_id}): {contract}\033[0m")

    dex_data = get_dexscreener_info(contract)
    if not dex_data:
        print(f"\033[33m[!] Dexscreener verisi yok — atlandı ({contract})\033[0m")
        return

    require_liquidity_check = chat_id in (CMC_CHANNEL, CG_CHANNEL)
    if require_liquidity_check:
        liquidity_usd = float((dex_data.get("liquidity") or {}).get("usd", 0) or 0)
        if liquidity_usd < LIQUIDITY_THRESHOLD:
            print(f"\033[33m⚠️ Likidite yetersiz ({liquidity_usd:,.0f}$) — atlandı ({contract})\033[0m")
            return
        else:
            print(f"\033[32m💰 Likidite: {liquidity_usd:,.0f}$ (threshold: {LIQUIDITY_THRESHOLD:,.0f}$)\033[0m")
    else:
        print(f"\033[36mℹ️ Kanal (makn): likidite kontrolü atlandı — direk Mongo kontrolü yapılacak.\033[0m")

    symbol_hint = (dex_data.get("baseToken") or {}).get("symbol")
    coin_id = get_token_id(contract, dex_data=dex_data, symbol_hint=symbol_hint)
    coin_data = get_token_info(coin_id) if coin_id else None

    partial_obj = normalize_json(contract, coin_data, dex_data)
    save_to_databases(partial_obj, dex_data)

# === KEEP-ALIVE (Flask Sunucu) ===
from flask import Flask
from threading import Thread
import asyncio, time, requests

app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Bot aktif ve dinliyor."

def run_flask():
    app.run(host="0.0.0.0", port=8080)

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

def self_ping():
    url = os.getenv("KEEPALIVE_URL", "https://<your-render-app>.onrender.com/")
    while True:
        try:
            requests.get(url)
            print("💓 Keepalive ping gönderildi")
        except Exception as e:
            print("⚠️ Keepalive ping başarısız:", e)
        time.sleep(280)

# === BOTU BAŞLAT ===
print("\033[34m🚀 Bot başlatıldı! Telegram kanalını dinliyorum 🔥\033[0m")

async def main():
    while True:
        try:
            await client.start()
            print("✅ Telegram bağlantısı kuruldu.")
            await client.run_until_disconnected()
        except Exception as e:
            print(f"⚠️ Bağlantı koptu, yeniden bağlanıyor: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    keep_alive()
    Thread(target=self_ping).start()
    asyncio.run(main())
