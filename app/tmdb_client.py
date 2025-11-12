from datetime import datetime

import httpx
from httpx import HTTPStatusError

from app.config import settings
from app.logging import logger
from app.mongo import sync_errors_collection

BASE_URL = "https://api.themoviedb.org/3"
IMAGE_CDN = "https://image.tmdb.org/t/p/"
TMDB_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


async def fetch_category(category: str, page: int = 1):
    async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
        resp = await client.get(
            f"{BASE_URL}/movie/{category}",
            params={"api_key": settings.tmdb_api_key, "language": "en-US", "page": page}
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_tv_category(category: str, page: int = 1):
    try:
        async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
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


async def fetch_backdrops(item_id: int, content_type: str = "movie") -> list[dict]:
    """
    Возвращает ВСЕ backdrops (НЕ постеры) для фильма/сериала.
    Формат элемента: {path, aspect_ratio, vote_average, width}
    Отфильтрованы по разумному AR и отсортированы по (vote_average desc, width desc).
    """
    try:
        async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
            resp = await client.get(
                f"{BASE_URL}/{content_type}/{item_id}/images",
                params={
                    "api_key": settings.tmdb_api_key,
                    # расширяем языки, чтобы не терять кадры
                    "include_image_language": "null,en,ru"
                }
            )
            resp.raise_for_status()
            data = resp.json()

        backdrops = data.get("backdrops", []) or []
        if not backdrops:
            return []

        def is_valid(b: dict) -> bool:
            # мягкий фильтр: кадры кинематографического формата и не совсем низкооценённые
            ar = b.get("aspect_ratio", 0) or 0
            return 1.5 <= ar <= 2.2 and (b.get("vote_average") or 0) >= 0

        frames = []
        seen = set()
        for b in backdrops:
            if not is_valid(b):
                continue
            path = b.get("file_path")
            if not path or path in seen:
                continue
            seen.add(path)
            frames.append({
                "path": path,
                "aspect_ratio": b.get("aspect_ratio"),
                "vote_average": b.get("vote_average") or 0,
                "width": b.get("width"),
            })

        # сортируем по качеству: сначала оценка кадра, потом ширина
        frames.sort(key=lambda f: (f.get("vote_average", 0) or 0, f.get("width", 0) or 0), reverse=True)
        return frames

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


async def fetch_best_frames(item_id: int, content_type: str = "movie", limit: int = 5) -> list[dict]:
    """
    DEPRECATED: возвращает теперь ВСЕ backdrops (лимит игнорируется).
    Оставлено для обратной совместимости с существующими вызовами.
    """
    return await fetch_backdrops(item_id, content_type)



async def fetch_discover_movies(page: int = 1) -> dict:
    async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
        resp = await client.get(
            f"{BASE_URL}/discover/movie",
            params={
                "api_key": settings.tmdb_api_key,
                "language": "en-US",
                "include_adult": False,
                "sort_by": "vote_count.desc",
                "release_date.gte": "1900-01-01",
                "release_date.lte": "2025-12-31",
                "page": page,
            }
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_details(item_id: int, content_type: str = "movie") -> dict:
    try:
        async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
            resp = await client.get(
                f"{BASE_URL}/{content_type}/{item_id}",
                params={"api_key": settings.tmdb_api_key, "language": "en-US"},
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.exception(f"Failed to fetch {content_type} details for {item_id}: {e}")
        return {}
