"""
PUBLIC API DATA INGESTION ENGINE
Fetches real-time data from multiple public APIs, transforms and loads into warehouse.
Sources: CoinGecko (crypto), REST Countries, Open-Meteo (weather), Exchange Rates.
"""

import json
import os
import sqlite3
import urllib.request
import urllib.error
from datetime import datetime

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..')
DB_PATH = os.path.join(BASE_DIR, 'data', 'warehouse.db')
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'api_responses')


def _fetch_json(url, timeout=15):
    """Fetch JSON from a URL."""
    req = urllib.request.Request(url, headers={'User-Agent': 'DataEngineeringProject/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _save_raw(filename, data):
    """Save raw API response for lineage."""
    os.makedirs(RAW_DIR, exist_ok=True)
    path = os.path.join(RAW_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"    Raw saved: {filename}")


def setup_api_tables(conn):
    """Create tables for public API data."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS api_crypto_prices (
            id TEXT,
            symbol TEXT,
            name TEXT,
            current_price_usd REAL,
            market_cap REAL,
            total_volume REAL,
            price_change_24h REAL,
            price_change_pct_24h REAL,
            high_24h REAL,
            low_24h REAL,
            circulating_supply REAL,
            rank INTEGER,
            last_updated TEXT,
            ingested_at TEXT
        );

        CREATE TABLE IF NOT EXISTS api_countries (
            name TEXT,
            official_name TEXT,
            capital TEXT,
            region TEXT,
            subregion TEXT,
            population INTEGER,
            area REAL,
            languages TEXT,
            currencies TEXT,
            flag_emoji TEXT,
            latitude REAL,
            longitude REAL,
            timezone TEXT,
            ingested_at TEXT
        );

        CREATE TABLE IF NOT EXISTS api_weather (
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            temperature_c REAL,
            wind_speed_kmh REAL,
            humidity_pct REAL,
            weather_code INTEGER,
            weather_desc TEXT,
            measured_at TEXT,
            ingested_at TEXT
        );

        CREATE TABLE IF NOT EXISTS api_exchange_rates (
            base_currency TEXT,
            target_currency TEXT,
            rate REAL,
            rate_date TEXT,
            ingested_at TEXT
        );

        CREATE TABLE IF NOT EXISTS api_github_trending (
            repo_name TEXT,
            full_name TEXT,
            description TEXT,
            language TEXT,
            stars INTEGER,
            forks INTEGER,
            open_issues INTEGER,
            watchers INTEGER,
            created_at TEXT,
            updated_at TEXT,
            html_url TEXT,
            ingested_at TEXT
        );

        CREATE TABLE IF NOT EXISTS api_ingestion_log (
            source TEXT,
            status TEXT,
            records_count INTEGER,
            error_message TEXT,
            started_at TEXT,
            completed_at TEXT
        );
    """)
    conn.commit()


# ===== CRYPTO PRICES (CoinGecko) =====
def ingest_crypto(conn):
    """Fetch top cryptocurrency prices from CoinGecko."""
    source = "CoinGecko"
    started = datetime.now().isoformat()
    print(f"\n  [{source}] Fetching cryptocurrency prices...")

    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false"
        data = _fetch_json(url)
        _save_raw(f"crypto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", data)

        conn.execute("DELETE FROM api_crypto_prices")
        now = datetime.now().isoformat()

        for coin in data:
            conn.execute("""
                INSERT INTO api_crypto_prices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                coin.get('id'), coin.get('symbol'), coin.get('name'),
                coin.get('current_price'), coin.get('market_cap'),
                coin.get('total_volume'), coin.get('price_change_24h'),
                coin.get('price_change_percentage_24h'),
                coin.get('high_24h'), coin.get('low_24h'),
                coin.get('circulating_supply'), coin.get('market_cap_rank'),
                coin.get('last_updated'), now
            ))

        conn.commit()
        _log_ingestion(conn, source, "SUCCESS", len(data), None, started)
        print(f"    Loaded {len(data)} cryptocurrencies")
        return True
    except Exception as e:
        _log_ingestion(conn, source, "FAILED", 0, str(e), started)
        print(f"    FAILED: {e}")
        return False


# ===== COUNTRIES (REST Countries) =====
def ingest_countries(conn):
    """Fetch country data from REST Countries API."""
    source = "REST Countries"
    started = datetime.now().isoformat()
    print(f"\n  [{source}] Fetching country data...")

    try:
        regions = ['africa', 'americas', 'asia', 'europe', 'oceania']
        data = []
        for region in regions:
            url = f"https://restcountries.com/v3.1/region/{region}"
            data.extend(_fetch_json(url))
        _save_raw(f"countries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", data)

        conn.execute("DELETE FROM api_countries")
        now = datetime.now().isoformat()

        for c in data:
            name = c.get('name', {}).get('common', '')
            official = c.get('name', {}).get('official', '')
            capital = ', '.join(c.get('capital', [])) if c.get('capital') else ''
            languages = ', '.join(c.get('languages', {}).values()) if c.get('languages') else ''
            currencies_data = c.get('currencies', {})
            currencies = ', '.join(f"{v.get('name','')} ({k})" for k, v in currencies_data.items()) if currencies_data else ''
            latlng = c.get('latlng', [0, 0])

            conn.execute("""
                INSERT INTO api_countries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                name, official, capital,
                c.get('region', ''), c.get('subregion', ''),
                c.get('population', 0), c.get('area', 0),
                languages, currencies, c.get('flag', ''),
                latlng[0] if len(latlng) > 0 else 0,
                latlng[1] if len(latlng) > 1 else 0,
                c.get('timezones', [''])[0] if c.get('timezones') else '',
                now
            ))

        conn.commit()
        _log_ingestion(conn, source, "SUCCESS", len(data), None, started)
        print(f"    Loaded {len(data)} countries")
        return True
    except Exception as e:
        _log_ingestion(conn, source, "FAILED", 0, str(e), started)
        print(f"    FAILED: {e}")
        return False


# ===== WEATHER (Open-Meteo) =====
def ingest_weather(conn):
    """Fetch current weather for major cities from Open-Meteo (no API key needed)."""
    source = "Open-Meteo"
    started = datetime.now().isoformat()
    print(f"\n  [{source}] Fetching weather data...")

    cities = [
        ("New York", "US", 40.71, -74.01), ("London", "UK", 51.51, -0.13),
        ("Tokyo", "JP", 35.68, 139.69), ("Sydney", "AU", -33.87, 151.21),
        ("Paris", "FR", 48.86, 2.35), ("Berlin", "DE", 52.52, 13.40),
        ("Mumbai", "IN", 19.08, 72.88), ("Toronto", "CA", 43.65, -79.38),
        ("Colombo", "LK", 6.93, 79.85), ("Dubai", "AE", 25.20, 55.27),
        ("Singapore", "SG", 1.35, 103.82), ("Sao Paulo", "BR", -23.55, -46.63),
        ("Cairo", "EG", 30.04, 31.24), ("Seoul", "KR", 37.57, 126.98),
        ("Moscow", "RU", 55.76, 37.62),
    ]

    weather_codes = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
        55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
        80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
        95: "Thunderstorm", 96: "Thunderstorm with hail",
    }

    try:
        # Build multi-city request
        lats = ",".join(str(c[2]) for c in cities)
        lons = ",".join(str(c[3]) for c in cities)
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}&current=temperature_2m,wind_speed_10m,relative_humidity_2m,weather_code"
        data = _fetch_json(url)
        _save_raw(f"weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", data)

        conn.execute("DELETE FROM api_weather")
        now = datetime.now().isoformat()

        results = data if isinstance(data, list) else [data]
        count = 0
        for i, city_data in enumerate(results):
            if i >= len(cities):
                break
            city_name, country, lat, lon = cities[i]
            current = city_data.get('current', {})
            wcode = current.get('weather_code', 0)

            conn.execute("""
                INSERT INTO api_weather VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                city_name, country, lat, lon,
                current.get('temperature_2m'),
                current.get('wind_speed_10m'),
                current.get('relative_humidity_2m'),
                wcode, weather_codes.get(wcode, f"Code {wcode}"),
                current.get('time', now), now
            ))
            count += 1

        conn.commit()
        _log_ingestion(conn, source, "SUCCESS", count, None, started)
        print(f"    Loaded weather for {count} cities")
        return True
    except Exception as e:
        _log_ingestion(conn, source, "FAILED", 0, str(e), started)
        print(f"    FAILED: {e}")
        return False


# ===== EXCHANGE RATES (exchangerate.host / frankfurter) =====
def ingest_exchange_rates(conn):
    """Fetch exchange rates from Frankfurter API."""
    source = "Frankfurter"
    started = datetime.now().isoformat()
    print(f"\n  [{source}] Fetching exchange rates...")

    try:
        url = "https://api.frankfurter.dev/v1/latest?base=USD"
        data = _fetch_json(url)
        _save_raw(f"exchange_rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", data)

        conn.execute("DELETE FROM api_exchange_rates")
        now = datetime.now().isoformat()
        rate_date = data.get('date', now[:10])
        rates = data.get('rates', {})

        for currency, rate in rates.items():
            conn.execute("""
                INSERT INTO api_exchange_rates VALUES (?,?,?,?,?)
            """, ('USD', currency, rate, rate_date, now))

        conn.commit()
        _log_ingestion(conn, source, "SUCCESS", len(rates), None, started)
        print(f"    Loaded {len(rates)} exchange rates (base: USD)")
        return True
    except Exception as e:
        _log_ingestion(conn, source, "FAILED", 0, str(e), started)
        print(f"    FAILED: {e}")
        return False


# ===== GITHUB TRENDING (Public repos) =====
def ingest_github_trending(conn):
    """Fetch trending/popular repos from GitHub public API."""
    source = "GitHub"
    started = datetime.now().isoformat()
    print(f"\n  [{source}] Fetching trending repositories...")

    try:
        url = "https://api.github.com/search/repositories?q=stars:>50000&sort=stars&order=desc&per_page=30"
        data = _fetch_json(url)
        _save_raw(f"github_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", data)

        conn.execute("DELETE FROM api_github_trending")
        now = datetime.now().isoformat()
        repos = data.get('items', [])

        for r in repos:
            conn.execute("""
                INSERT INTO api_github_trending VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r.get('name'), r.get('full_name'),
                (r.get('description') or '')[:200],
                r.get('language'), r.get('stargazers_count'),
                r.get('forks_count'), r.get('open_issues_count'),
                r.get('watchers_count'), r.get('created_at'),
                r.get('updated_at'), r.get('html_url'), now
            ))

        conn.commit()
        _log_ingestion(conn, source, "SUCCESS", len(repos), None, started)
        print(f"    Loaded {len(repos)} trending repositories")
        return True
    except Exception as e:
        _log_ingestion(conn, source, "FAILED", 0, str(e), started)
        print(f"    FAILED: {e}")
        return False


def _log_ingestion(conn, source, status, count, error, started):
    conn.execute("""
        INSERT INTO api_ingestion_log VALUES (?,?,?,?,?,?)
    """, (source, status, count, error, started, datetime.now().isoformat()))
    conn.commit()


def run_all_ingestions():
    """Run all public API ingestions."""
    print("=" * 70)
    print("PUBLIC API DATA INGESTION")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    conn = sqlite3.connect(DB_PATH)
    setup_api_tables(conn)

    results = {}
    results['crypto'] = ingest_crypto(conn)
    results['countries'] = ingest_countries(conn)
    results['weather'] = ingest_weather(conn)
    results['exchange_rates'] = ingest_exchange_rates(conn)
    results['github'] = ingest_github_trending(conn)

    conn.close()

    succeeded = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    print(f"\n{'=' * 70}")
    print(f"INGESTION COMPLETE: {succeeded} succeeded, {failed} failed")
    print(f"{'=' * 70}")
    return results


if __name__ == '__main__':
    run_all_ingestions()
