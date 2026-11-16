import asyncio
import gzip
import json
import os
import random
import sys
import time
from pathlib import Path
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

import aiohttp

API_KEY = os.getenv("STEAM_API_KEY", "").strip()
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "3000")))
CONCURRENCY = max(1, int(os.getenv("MAX_CONCURRENCY", "10")))
REQUEST_INTERVAL = max(0.0, float(os.getenv("REQUEST_INTERVAL", "0.35")))
DATA = Path(os.getenv("DATA_DIR", "data-store"))
CHUNKS = DATA / "chunks"
CATALOG = DATA / "catalog.jsonl.gz"
CHECKPOINT = DATA / "checkpoint.json"

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
KEEP_FIELDS = (
    "steam_appid", "name", "type", "required_age", "is_free", "dlc",
    "detailed_description", "about_the_game", "short_description",
    "supported_languages", "header_image", "website", "pc_requirements",
    "mac_requirements", "linux_requirements", "developers", "publishers",
    "price_overview", "platforms", "categories", "genres", "screenshots",
    "movies", "release_date", "recommendations", "metacritic", "tags"
)


class RequestPacer:
    """Global pacing with shared cooldown and adaptive slowdown after HTTP 429."""
    def __init__(self, interval):
        self.interval = interval
        self.lock = asyncio.Lock()
        self.next_start = 0.0
        self.cooldown_until = 0.0

    async def wait(self):
        while True:
            async with self.lock:
                now = asyncio.get_running_loop().time()
                target = max(self.next_start, self.cooldown_until)
                delay = target - now
                if delay <= 0:
                    self.next_start = now + self.interval
                    return
            await asyncio.sleep(delay + random.uniform(0.1, 0.8))

    async def rate_limited(self, retry_after=None):
        async with self.lock:
            now = asyncio.get_running_loop().time()
            self.interval = min(max(self.interval * 1.8, 1.5), 8.0)
            pause = max(60.0, retry_after or 0.0)
            self.cooldown_until = max(self.cooldown_until, now + pause)
            self.next_start = max(self.next_start, self.cooldown_until)
            print(f"Shared rate-limit cooldown: {pause:.0f}s; new request interval={self.interval:.2f}s", flush=True)

async def request_json(session, url, params=None, pacer=None, attempts=6):
    for attempt in range(attempts):
        try:
            if pacer:
                await pacer.wait()
            async with session.get(url, params=params) as response:
                if response.status in RETRYABLE_STATUS:
                    retry_after = response.headers.get("Retry-After")
                    if response.status == 429 and pacer:
                        parsed_retry = None
                        if retry_after:
                            try:
                                parsed_retry = max(1.0, float(retry_after))
                            except ValueError:
                                try:
                                    retry_at = parsedate_to_datetime(retry_after)
                                    parsed_retry = max(1.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
                                except Exception:
                                    parsed_retry = None
                        await pacer.rate_limited(parsed_retry)
                    if retry_after:
                        try:
                            wait = max(1.0, float(retry_after))
                        except ValueError:
                            try:
                                retry_at = parsedate_to_datetime(retry_after)
                                wait = max(1.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
                            except Exception:
                                wait = min(180, 4 * (2 ** attempt))
                    else:
                        wait = min(180, 4 * (2 ** attempt))
                    if attempt == attempts - 1:
                        response.raise_for_status()
                    print(f"HTTP {response.status}; retrying in {wait:.1f}s", flush=True)
                    await asyncio.sleep(wait + random.random())
                    continue
                response.raise_for_status()
                return await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            if attempt == attempts - 1:
                raise
            wait = min(180, 4 * (2 ** attempt)) + random.random() * 3
            print(f"Request failed ({exc}); retrying in {wait:.1f}s", flush=True)
            await asyncio.sleep(wait)
    raise RuntimeError("Request exhausted retries")


async def ensure_catalog(session):
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
            payload = await request_json(
                session,
                "https://api.steampowered.com/IStoreService/GetAppList/v1/",
                params={"key": API_KEY, "include_games": "true", "max_results": 50000, "last_appid": cursor},
            )
            response = payload.get("response", {})
            apps = response.get("apps", [])
            if not apps:
                break
            page = apps[: max(0, 150000 - count)]
            for app in page:
                out.write(json.dumps(app, ensure_ascii=False) + "\n")
            count += len(page)
            if count >= 150000:
                break
            next_cursor = int(response.get("last_appid") or apps[-1]["appid"])
            print(f"Catalog: {count} apps; cursor={next_cursor}", flush=True)
            if not response.get("have_more_results") or next_cursor <= cursor:
                break
            cursor = next_cursor
            await asyncio.sleep(1)
    temp.replace(CATALOG)
    print(f"Catalog saved: {count} entries", flush=True)


def read_checkpoint():
    if not CHECKPOINT.exists():
        return {"catalog_offset": 0, "last_appid": 0, "processed": 0, "failed_appids": [], "complete": False}
    return json.loads(CHECKPOINT.read_text(encoding="utf-8"))


async def fetch_details(session, semaphore, pacer, appid):
    async with semaphore:
        payload = await request_json(
            session,
            "https://store.steampowered.com/api/appdetails/",
            params={"appids": str(appid), "cc": "us", "l": "english"},
            pacer=pacer,
        )
    item = payload.get(str(appid), {})
    if not item.get("success"):
        return None
    game = item.get("data") or {}
    if game.get("type") != "game":
        return None
    return {key: game[key] for key in KEEP_FIELDS if key in game}


async def collect_batch(session, batch, offset, state):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    pacer = RequestPacer(REQUEST_INTERVAL)
    tasks = [
        asyncio.create_task(fetch_details(session, semaphore, pacer, int(entry["appid"])))
        for entry in batch
    ]
    failed = set(int(x) for x in state.get("failed_appids", []))
    saved = 0
    completed = 0
    for future in asyncio.as_completed(tasks):
        try:
            await future
        except Exception:
            # The ordered gather below records the exact AppID for each failure.
            pass
        completed += 1
        if completed % 100 == 0 or completed == len(batch):
            print(f"Progress: {completed}/{len(batch)} completed", flush=True)

    # The gather below preserves input order and makes per-AppID failure accounting deterministic.
    ordered_results = await asyncio.gather(*tasks, return_exceptions=True)
    chunk_path = CHUNKS / f"games_{offset}_{offset + len(batch)}.jsonl.gz"
    with gzip.open(chunk_path, "wt", encoding="utf-8") as out:
        for entry, result in zip(batch, ordered_results):
            appid = int(entry["appid"])
            if isinstance(result, Exception):
                failed.add(appid)
                print(f"AppID {appid} failed: {result}", flush=True)
                continue
            if result:
                result["catalog_name"] = entry.get("name")
                out.write(json.dumps(result, ensure_ascii=False) + "\n")
                saved += 1
            failed.discard(appid)

    state["catalog_offset"] = offset + len(batch)
    state["last_appid"] = int(batch[-1]["appid"])
    state["processed"] = int(state.get("processed", 0)) + len(batch)
    state["failed_appids"] = sorted(failed)
    state["updated_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    CHECKPOINT.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(f"Batch finished: scanned={len(batch)}, games_saved={saved}, total_scanned={state['processed']}", flush=True)
    print(f"Checkpoint offset={state['catalog_offset']}; failed={len(failed)}", flush=True)


async def main():
    if not DATA.exists():
        raise RuntimeError(f"Data directory does not exist: {DATA}")
    CHUNKS.mkdir(parents=True, exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=75, connect=20, sock_read=60)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY, ttl_dns_cache=300)
    headers = {"User-Agent": "SteamGameAtlas/1.0 (catalog research)"}

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        await ensure_catalog(session)
        state = read_checkpoint()
        if state.get("complete"):
            print("Collection is already complete.")
            return

        batch = []
        offset = int(state.get("catalog_offset", 0))
        scanned = 0
        with gzip.open(CATALOG, "rt", encoding="utf-8") as catalog:
            for line in catalog:
                if scanned < offset:
                    scanned += 1
                    continue
                batch.append(json.loads(line))
                scanned += 1
                if len(batch) >= BATCH_SIZE:
                    break

        if not batch:
            state["complete"] = True
            CHECKPOINT.write_text(json.dumps(state, indent=2), encoding="utf-8")
            print("Reached end of catalog.")
            return

        print(f"Starting batch at offset {offset}: {len(batch)} AppIDs; concurrency={CONCURRENCY}; request interval={REQUEST_INTERVAL}s", flush=True)
        await collect_batch(session, batch, offset, state)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"FATAL: {error}", file=sys.stderr)
        raise
