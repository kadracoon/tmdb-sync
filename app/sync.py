from datetime import datetime

import httpx
from app.config import settings
from app.mongo import movies_collection
from app.tmdb_client import fetch_category, fetch_tv_category, fetch_best_frames, fetch_discover_movies


async def fetch_title_ru(item_id: int, content_type: str = "movie") -> str | None:
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
    except Exception as e:
        print(f"Failed to fetch localized title for {content_type} {item_id}: {e}")
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
                movie = enrich_common_fields(movie, "movie", "discover")
                movie["title_ru"] = await fetch_title_ru(movie["id"], "movie")
                movie["frame_url"] = await fetch_best_frames(movie["id"], "movie")

                if not movie["frame_url"]:
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
