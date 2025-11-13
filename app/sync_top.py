import asyncio
from datetime import datetime
import httpx
from httpx import HTTPStatusError, ConnectError, ReadTimeout

from app.config import settings
from app.logging import logger
from app.mongo import (
    db,
    movies_collection,
    sync_errors_collection,
    sync_cursors_collection,
)
from app.catalog.upsert import upsert_movie
from app.sync import enrich_common_fields, fetch_title_ru
from app.tmdb_client import fetch_backdrops, fetch_details, TMDB_TIMEOUT


CURSOR_KEY = "top_vote_count_movie"  # ключ для прогресса


async def _get_cursor() -> dict:
    cur = await sync_cursors_collection.find_one({"key": CURSOR_KEY})  # type: ignore
    return cur or {"key": CURSOR_KEY, "page": 0, "inserted": 0, "updated": 0, "ts": datetime.utcnow()}


async def _save_cursor(cur: dict):
    cur["ts"] = datetime.utcnow()
    await sync_cursors_collection.update_one({"key": CURSOR_KEY}, {"$set": cur}, upsert=True)  # type: ignore


async def _fetch_discover_vote_count(page: int) -> dict | None:
    """
    Топ по vote_count, с ретраями.
    Возвращает JSON или None, если после нескольких попыток так и не удалось достучаться.
    """
    params = {
        "api_key": settings.tmdb_api_key,
        "language": "en-US",
        "include_adult": False,
        "include_video": False,
        "sort_by": "vote_count.desc",
        "page": page,
    }

    max_attempts = 5
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
                r = await client.get(
                    "https://api.themoviedb.org/3/discover/movie",
                    params=params,
                )
                r.raise_for_status()
                return r.json()

        except HTTPStatusError as e:
            # TMDB ответил 4xx/5xx — ретраи мало помогут
            logger.error(
                "TMDB discover(top-votes) HTTP error %s %s page=%s",
                e.response.status_code,
                e.request.url,
                page,
            )
            await sync_errors_collection.insert_one({
                "endpoint": e.request.url.path,
                "url": str(e.request.url),
                "status_code": e.response.status_code,
                "params": dict(e.request.url.params),
                "response_text": e.response.text,
                "page": page,
                "timestamp": datetime.utcnow(),
            })
            return None

        except (ConnectError, ReadTimeout) as e:
            last_exc = e
            logger.warning(
                "TMDB discover(top-votes) network error page=%s attempt %s/%s: %r",
                page,
                attempt,
                max_attempts,
                e,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": "/discover/movie",
                    "page": page,
                    "error": f"network error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return None
            await asyncio.sleep(attempt)

        except Exception as e:
            last_exc = e
            logger.exception(
                "Unexpected error in discover(top-votes) page=%s attempt %s/%s",
                page,
                attempt,
                max_attempts,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": "/discover/movie",
                    "page": page,
                    "error": f"unexpected error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return None
            await asyncio.sleep(attempt)

    logger.error(
        "TMDB discover(top-votes) page=%s failed after %s attempts: %r",
        page,
        max_attempts,
        last_exc,
    )
    return None


async def sync_top_by_vote_count(
    limit: int = 10000,
    resume: bool = True,
    start_page: int | None = None,
) -> dict:
    cur = await _get_cursor()
    page = start_page or (cur["page"] + 1 if resume else 1)

    processed = 0
    inserted = 0
    updated = 0

    while True:
        data = await _fetch_discover_vote_count(page)

        if data is None:
            logger.error(
                "Stopping sync_top_by_vote_count at page=%s due to TMDB discover errors",
                page,
            )
            break

        results = data.get("results") or []
        if not results:
            break

        for movie in results:
            processed += 1
            if processed > limit:
                await _save_cursor({
                    "key": CURSOR_KEY,
                    "page": page,
                    "inserted": cur.get("inserted", 0) + inserted,
                    "updated": cur.get("updated", 0) + updated,
                })
                return {
                    "status": "ok",
                    "page": page,
                    "processed": processed,
                    "inserted": inserted,
                    "updated": updated,
                }

            tmdb_id = movie.get("id")
            if not tmdb_id:
                continue

            try:
                # --- детали ---
                async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
                    details = await client.get(
                        f"https://api.themoviedb.org/3/movie/{tmdb_id}",
                        params={
                            "api_key": settings.tmdb_api_key,
                            "language": "en-US",
                        },
                    )
                    details.raise_for_status()
                    det = details.json()

                movie["production_countries"] = det.get("production_countries", [])

                # --- общие поля ---
                movie = enrich_common_fields(movie, "movie", "discover_top_votes")
                movie["title_ru"] = await fetch_title_ru(tmdb_id, "movie")

                # --- ВСЕ кадры ---
                movie["frames"] = await fetch_backdrops(tmdb_id, "movie")

                # --- апсёрт ---
                before = await movies_collection.find_one(
                    {"id": tmdb_id, "_type": "movie"},
                    {"_id": 1},
                )
                await upsert_movie(movie)
                if before:
                    updated += 1
                else:
                    inserted += 1

            except HTTPStatusError as e:
                logger.error(
                    "TMDB details HTTP error %s %s (movie_id=%s)",
                    e.response.status_code,
                    e.request.url,
                    tmdb_id,
                )
                await sync_errors_collection.insert_one({
                    "endpoint": e.request.url.path,
                    "url": str(e.request.url),
                    "status_code": e.response.status_code,
                    "params": dict(e.request.url.params),
                    "response_text": e.response.text,
                    "movie_id": tmdb_id,
                    "timestamp": datetime.utcnow(),
                })
                # просто пропускаем этот фильм
                continue

            except (ConnectError, ReadTimeout) as e:
                logger.warning(
                    "Network error while processing movie_id=%s in top-votes: %r",
                    tmdb_id,
                    e,
                )
                await sync_errors_collection.insert_one({
                    "endpoint": "/movie/details-or-images",
                    "movie_id": tmdb_id,
                    "error": f"network error: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                # тоже пропускаем этот фильм, двигаемся дальше
                continue

            except Exception as e:
                logger.exception("Unexpected while upserting %s", tmdb_id)
                await sync_errors_collection.insert_one({
                    "endpoint": "upsert_movie",
                    "error": str(e),
                    "movie_id": tmdb_id,
                    "timestamp": datetime.utcnow(),
                })
                # и здесь тоже: не роняем процесс, идём далее
                continue

        await _save_cursor({
            "key": CURSOR_KEY,
            "page": page,
            "inserted": cur.get("inserted", 0) + inserted,
            "updated": cur.get("updated", 0) + updated,
        })
        page += 1

    return {
        "status": "done",
        "page": page,
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
    }
