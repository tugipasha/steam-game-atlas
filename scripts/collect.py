import gzip
import json
import os
import random
import sys
import time
from pathlib import Path

import requests

API_KEY = os.getenv("STEAM_API_KEY", "").strip()
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "3000")))
DELAY = max(0.0, float(os.getenv("REQUEST_DELAY", "1.5")))
DATA = Path(os.getenv("DATA_DIR", "data-store"))
CHUNKS = DATA / "chunks"
CATALOG = DATA / "catalog.jsonl.gz"
CHECKPOINT = DATA / "checkpoint.json"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "SteamGameAtlas/1.0 (catalog research)"})

def request_json(url, params=None, attempts=6):
    for attempt in range(attempts):
        try:
            response = SESSION.get(url, params=params, timeout=(15, 60))
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"temporary HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if attempt == attempts - 1:
                raise
            wait = min(180, 4 * (2 ** attempt)) + random.random() * 3
            print(f"Request failed ({exc}); retrying in {wait:.1f}s", flush=True)
            time.sleep(wait)

def ensure_catalog():
    if CATALOG.exists():
        return
    if not API_KEY:
        raise RuntimeError("STEAM_API_KEY secret is missing. Add it in Settings > Secrets and variables > Actions.")
    print("Downloading Steam game catalog...", flush=True)
    CHUNKS.mkdir(parents=True, exist_ok=True)
    temp = CATALOG.with_suffix(".tmp")
    cursor = 0
    count = 0
    with gzip.open(temp, "wt", encoding="utf-8") as out:
        while True:
            payload = request_json(
                "https://api.steampowered.com/IStoreService/GetAppList/v1/",
                params={"key": API_KEY, "include_games": "true", "max_results": 50000, "last_appid": cursor},
            )
            response = payload.get("response", {})
            apps = response.get("apps", [])
            if not apps:
                break
            for app in apps:
                out.write(json.dumps(app, ensure_ascii=False) + "\n")
            count += len(apps)
            next_cursor = int(response.get("last_appid") or apps[-1]["appid"])
            print(f"Catalog: {count} apps; cursor={next_cursor}", flush=True)
            if not response.get("have_more_results") or next_cursor <= cursor:
                break
            cursor = next_cursor
            time.sleep(1)
    temp.replace(CATALOG)
    print(f"Catalog saved: {count} entries", flush=True)

def read_checkpoint():
    if not CHECKPOINT.exists():
        return {"catalog_offset": 0, "last_appid": 0, "processed": 0, "failed_appids": [], "complete": False}
    return json.loads(CHECKPOINT.read_text(encoding="utf-8"))

def fetch_details(appid):
    payload = request_json(
        "https://store.steampowered.com/api/appdetails/",
        params={"appids": str(appid), "cc": "us", "l": "english"},
    )
    item = payload.get(str(appid), {})
    if not item.get("success"):
        return None
    game = item.get("data") or {}
    if game.get("type") != "game":
        return None
    keep = (
        "steam_appid", "name", "type", "required_age", "is_free", "dlc",
        "detailed_description", "about_the_game", "short_description",
        "supported_languages", "header_image", "website", "pc_requirements",
        "mac_requirements", "linux_requirements", "developers", "publishers",
        "price_overview", "platforms", "categories", "genres", "screenshots",
        "movies", "release_date", "recommendations", "metacritic", "tags"
    )
    return {key: game[key] for key in keep if key in game}

def main():
    if not DATA.exists():
        raise RuntimeError(f"Data directory does not exist: {DATA}")
    CHUNKS.mkdir(parents=True, exist_ok=True)
    ensure_catalog()
    state = read_checkpoint()
    if state.get("complete"):
        print("Collection is already complete.")
        return

    # Stream catalog entries, skipping the durable offset.
    batch = []
    offset = int(state.get("catalog_offset", 0))
    scanned = 0
    with gzip.open(CATALOG, "rt", encoding="utf-8") as catalog:
        for line in catalog:
            if scanned < offset:
                scanned += 1
                continue
            entry = json.loads(line)
            batch.append(entry)
            scanned += 1
            if len(batch) >= BATCH_SIZE:
                break

    if not batch:
        state["complete"] = True
        CHECKPOINT.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print("Reached end of catalog.")
        return

    stamp = f"{int(time.time())}_{offset}"
    chunk_path = CHUNKS / f"games_{stamp}.jsonl.gz"
    failed = set(int(x) for x in state.get("failed_appids", []))
    done = 0
    saved = 0
    with gzip.open(chunk_path, "wt", encoding="utf-8") as out:
        for entry in batch:
            appid = int(entry["appid"])
            try:
                detail = fetch_details(appid)
                if detail:
                    detail["catalog_name"] = entry.get("name")
                    out.write(json.dumps(detail, ensure_ascii=False) + "\n")
                    saved += 1
                failed.discard(appid)
            except Exception as exc:
                failed.add(appid)
                print(f"AppID {appid} failed: {exc}", flush=True)
            done += 1
            if done % 50 == 0:
                print(f"Progress: {done}/{len(batch)}; games saved: {saved}", flush=True)
            time.sleep(DELAY)

    state["catalog_offset"] = offset + len(batch)
    state["last_appid"] = int(batch[-1]["appid"])
    state["processed"] = int(state.get("processed", 0)) + len(batch)
    state["failed_appids"] = sorted(failed)
    state["updated_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    CHECKPOINT.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(f"Batch finished: scanned={len(batch)}, games_saved={saved}, total_scanned={state['processed']}", flush=True)
    print(f"Checkpoint offset={state['catalog_offset']}; failed={len(failed)}", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FATAL: {error}", file=sys.stderr)
        raise
