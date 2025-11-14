from datetime import datetime
import asyncio

import httpx
from httpx import HTTPStatusError, ConnectError, ReadTimeout

from app.catalog.upsert import upsert_movie
from app.config import settings
from app.logging import logger
from app.mongo import movies_collection
from app.mongo import sync_errors_collection
from app.tmdb_client import (
    get_tmdb_client,
    fetch_category,
    fetch_tv_category,
    fetch_best_frames,
    fetch_discover_movies,
    fetch_details,
    TMDB_TIMEOUT,
)


async def fetch_title_ru(item_id: int, content_type: str = "movie") -> str | None:
    """Получить локализованный заголовок для фильма или сериала (ru-RU) с ретраями."""
    max_attempts = 3
    client = await get_tmdb_client()

    for attempt in range(1, max_attempts + 1):
        try:
            response = await client.get(
                f"https://api.themoviedb.org/3/{content_type}/{item_id}",
                params={"api_key": settings.tmdb_api_key, "language": "ru-RU"},
            )
            response.raise_for_status()
            data = response.json()
            return data.get("title") or data.get("name")

        except HTTPStatusError as e:
            logger.error(
                "TMDB RU-title HTTP error %s for %s id=%s: %s",
                e.response.status_code,
                content_type,
                item_id,
                e.request.url,
            )
            await sync_errors_collection.insert_one({
                "endpoint": e.request.url.path,
                "url": str(e.request.url),
                "status_code": e.response.status_code,
                "params": dict(e.request.url.params),
                "response_text": e.response.text,
                "item_id": item_id,
                "content_type": content_type,
                "timestamp": datetime.utcnow(),
            })
            return None

        except (ConnectError, ReadTimeout) as e:
            logger.warning(
                "TMDB RU-title network error (%s id=%s) attempt %s/%s: %r",
                content_type,
                item_id,
                attempt,
                max_attempts,
                e,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": f"/{content_type}/{item_id}",
                    "item_id": item_id,
                    "content_type": content_type,
                    "error": f"RU-title network error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return None
            await asyncio.sleep(attempt)

        except Exception as e:
            logger.exception(
                "Unexpected error fetching RU-title for %s id=%s (attempt %s/%s)",
                content_type,
                item_id,
                attempt,
                max_attempts,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": f"/{content_type}/{item_id}",
                    "item_id": item_id,
                    "content_type": content_type,
                    "error": f"RU-title unexpected error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return None
            await asyncio.sleep(attempt)

    return None


def enrich_common_fields(item: dict, content_type: str, category: str) -> dict:
    item["_type"] = content_type
    item["_category"] = category
    item["synced_at"] = datetime.utcnow()
    item["is_animated"] = 16 in item.get("genre_ids", [])

    countries = item.get("production_countries", [])
    item["country_codes"] = [c["iso_3166_1"] for c in countries]

    return item


async def sync_category(category: str):
    """Синхронизация фильмов"""
    data = await fetch_category(category)
    results = data.get("results", [])

    for movie in results:
        details = await fetch_details(movie["id"], "movie")
        if not details:
            continue
        movie["production_countries"] = details.get("production_countries", [])

        movie = enrich_common_fields(movie, "movie", category)
        movie["title_ru"] = await fetch_title_ru(movie["id"], "movie")
        frames = await fetch_best_frames(movie["id"], "movie")

        if not frames:
            continue

        movie["frames"] = frames
        await upsert_movie(movie)

    return {"inserted_or_updated": len(results), "type": "movie", "category": category}


async def sync_tv_category(category: str):
    """Синхронизация сериалов"""
    data = await fetch_tv_category(category)
    results = data.get("results", [])

    for tv in results:
        details = await fetch_details(tv["id"], "tv")
        if not details:
            continue
        tv["production_countries"] = details.get("production_countries", [])

        tv = enrich_common_fields(tv, "tv", category)
        tv["title_ru"] = await fetch_title_ru(tv["id"], "tv")
        frames = await fetch_best_frames(tv["id"], "tv")

        if not frames:
            continue

        tv["frames"] = frames
        await upsert_movie(tv)

    return {"inserted_or_updated": len(results), "type": "tv", "category": category}


async def sync_discover_movies(pages: int = 1):
    total = 0
    for page in range(1, pages + 1):
        try:
            data = await fetch_discover_movies(page)
            results = data.get("results", [])

            for movie in results:
                details = await fetch_details(movie["id"], "movie")
                if not details:
                    continue

                movie["production_countries"] = details.get("production_countries", [])

                movie = enrich_common_fields(movie, "movie", "discover")
                movie["title_ru"] = await fetch_title_ru(movie["id"], "movie")
                movie["frames"] = await fetch_best_frames(movie["id"], "movie")

                if not movie["frames"]:
                    continue

                await upsert_movie(movie)
                total += 1

        except Exception as e:
            print(f"[Page {page}] Failed to sync discover movies: {e}")

    return {"inserted_or_updated": total, "source": "discover"}
