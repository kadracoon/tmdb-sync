from datetime import datetime

import httpx
from httpx import HTTPStatusError

from app.config import settings
from app.logging import logger
from app.mongo import sync_errors_collection

BASE_URL = "https://api.themoviedb.org/3"
IMAGE_CDN = "https://image.tmdb.org/t/p/"


async def fetch_category(category: str, page: int = 1):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE_URL}/movie/{category}",
            params={"api_key": settings.tmdb_api_key, "language": "en-US", "page": page}
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_tv_category(category: str, page: int = 1):
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/tv/{category}",
                params={"api_key": settings.tmdb_api_key, "language": "en-US", "page": page}
            )
            resp.raise_for_status()
            return resp.json()
    except HTTPStatusError as e:
        logger.error(f"TMDB TV API error: {e.response.status_code} on {e.request.url}")
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
        logger.exception("Unexpected error in fetch_tv_category")
        await sync_errors_collection.insert_one({
            "endpoint": "unknown",
            "error": str(e),
            "timestamp": datetime.utcnow()
        })
        return {}


async def fetch_best_frames(item_id: int, content_type: str = "movie", limit: int = 5) -> list[dict]:
    """Возвращает до `limit` лучших кадров с допустимым соотношением сторон"""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/{content_type}/{item_id}/images",
                params={"api_key": settings.tmdb_api_key}
            )
            resp.raise_for_status()
            data = resp.json()
            backdrops = data.get("backdrops", [])
            if not backdrops:
                return []

            def is_valid(b):
                ar = b.get("aspect_ratio", 0)
                return 1.6 <= ar <= 1.85 and b.get("vote_average", 0) >= 3

            filtered = list(filter(is_valid, backdrops))
            if not filtered:
                return []

            sorted_frames = sorted(
                filtered,
                key=lambda b: (b["vote_average"], b["width"]),
                reverse=True
            )[:limit]

            return [
                {
                    "path": f["file_path"],
                    "aspect_ratio": f["aspect_ratio"],
                    "vote_average": f["vote_average"],
                    "width": f["width"]
                }
                for f in sorted_frames
            ]

    except HTTPStatusError as e:
        logger.error(f"TMDB frame API error: {e.response.status_code} on {e.request.url}")
        await sync_errors_collection.insert_one({
            "endpoint": e.request.url.path,
            "url": str(e.request.url),
            "status_code": e.response.status_code,
            "params": dict(e.request.url.params),
            "response_text": e.response.text,
            "timestamp": datetime.utcnow()
        })
    except Exception as e:
        logger.exception(f"Failed to fetch frames for {content_type} {item_id}")
        await sync_errors_collection.insert_one({
            "endpoint": f"{content_type}/{item_id}/images",
            "error": str(e),
            "timestamp": datetime.utcnow()
        })

    return []


async def fetch_discover_movies(page: int = 1) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE_URL}/discover/movie",
            params={
                "api_key": settings.tmdb_api_key,
                "language": "en-US",
                "include_adult": False,
                "sort_by": "popularity.desc",
                "release_date.gte": "1980-01-01",
                "release_date.lte": "2025-12-31",
                "page": page,
            }
        )
        resp.raise_for_status()
        return resp.json()
