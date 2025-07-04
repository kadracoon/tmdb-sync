from datetime import datetime

import httpx
from httpx import HTTPStatusError
from app.config import settings
from app.logging import logger
from app.mongo import movies_collection
from app.mongo import sync_errors_collection
from app.tmdb_client import fetch_category, fetch_tv_category, fetch_best_frames, fetch_discover_movies, fetch_details


async def fetch_title_ru(item_id: int, content_type: str = "movie") -> dict:
    """Получить локализованный заголовок для фильма или сериала"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.themoviedb.org/3/{content_type}/{item_id}",
                params={"api_key": settings.tmdb_api_key, "language": "ru-RU"}
            )
            response.raise_for_status()
            data = response.json()
            return data.get("title") or data.get("name")
    except HTTPStatusError as e:
        logger.error(f"TMDB API error: {e.response.status_code} on {e.request.url}")
        await sync_errors_collection.insert_one({
            "endpoint": e.request.url.path,
            "url": str(e.request.url),
            "status_code": e.response.status_code,
            "params": dict(e.request.url.params),
            "response_text": e.response.text,
            "timestamp": datetime.utcnow()
        })
        return {}
    except Exception as e:
        logger.exception("Unexpected error during TMDB fetch")
        await sync_errors_collection.insert_one({
            "endpoint": "unknown",
            "error": str(e),
            "timestamp": datetime.utcnow()
        })
        return {}


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

        await movies_collection.update_one(
            {"id": movie["id"], "_type": "movie"},
            {"$set": movie},
            upsert=True
        )

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

        await movies_collection.update_one(
            {"id": tv["id"], "_type": "tv"},
            {"$set": tv},
            upsert=True
        )

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

                await movies_collection.update_one(
                    {"id": movie["id"], "_type": "movie"},
                    {"$set": movie},
                    upsert=True
                )
                total += 1

        except Exception as e:
            print(f"[Page {page}] Failed to sync discover movies: {e}")

    return {"inserted_or_updated": total, "source": "discover"}
