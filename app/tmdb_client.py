from datetime import datetime

import httpx
from httpx import HTTPStatusError, ConnectError, ReadTimeout

from app.config import settings
from app.logging import logger
from app.mongo import sync_errors_collection

BASE_URL = "https://api.themoviedb.org/3"
IMAGE_CDN = "https://image.tmdb.org/t/p/"
TMDB_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


async def fetch_category(category: str, page: int = 1) -> dict:
    """
    Топовые / популярные / now_playing и прочие movie/{category}.
    При HTTP-ошибках и сетевых фейлах логирует и возвращает {}.
    """
    params = {
        "api_key": settings.tmdb_api_key,
        "language": "en-US",
        "page": page,
    }

    max_attempts = 5
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
                resp = await client.get(
                    f"{BASE_URL}/movie/{category}",
                    params=params,
                )
                resp.raise_for_status()
                return resp.json()

        except HTTPStatusError as e:
            logger.error(
                "TMDB movie category HTTP error %s %s (category=%s page=%s)",
                e.response.status_code,
                e.request.url,
                category,
                page,
            )
            await sync_errors_collection.insert_one({
                "endpoint": e.request.url.path,
                "url": str(e.request.url),
                "status_code": e.response.status_code,
                "params": dict(e.request.url.params),
                "response_text": e.response.text,
                "category": category,
                "page": page,
                "timestamp": datetime.utcnow(),
            })
            return {}

        except (ConnectError, ReadTimeout) as e:
            last_exc = e
            logger.warning(
                "TMDB movie category network error (category=%s page=%s) attempt %s/%s: %r",
                category,
                page,
                attempt,
                max_attempts,
                e,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": f"/movie/{category}",
                    "category": category,
                    "page": page,
                    "error": f"network error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return {}
            await asyncio.sleep(attempt)

        except Exception as e:
            last_exc = e
            logger.exception(
                "Unexpected error in fetch_category (category=%s page=%s) attempt %s/%s",
                category,
                page,
                attempt,
                max_attempts,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": f"/movie/{category}",
                    "category": category,
                    "page": page,
                    "error": f"unexpected error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return {}
            await asyncio.sleep(attempt)

    logger.error(
        "fetch_category(category=%s page=%s) failed after %s attempts: %r",
        category,
        page,
        max_attempts,
        last_exc,
    )
    return {}


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
    """
    Общий discover/movie, который используется в учебной ручке sync_discover_movies.
    При любых проблемах возвращает {} и пишет в sync_errors.
    """
    params = {
        "api_key": settings.tmdb_api_key,
        "language": "en-US",
        "include_adult": False,
        "sort_by": "vote_count.desc",
        "release_date.gte": "1900-01-01",
        "release_date.lte": "2025-12-31",
        "page": page,
    }

    max_attempts = 5
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
                resp = await client.get(
                    f"{BASE_URL}/discover/movie",
                    params=params,
                )
                resp.raise_for_status()
                return resp.json()

        except HTTPStatusError as e:
            logger.error(
                "TMDB discover(movie) HTTP error %s %s (page=%s)",
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
            return {}

        except (ConnectError, ReadTimeout) as e:
            last_exc = e
            logger.warning(
                "TMDB discover(movie) network error page=%s attempt %s/%s: %r",
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
                return {}
            await asyncio.sleep(attempt)

        except Exception as e:
            last_exc = e
            logger.exception(
                "Unexpected error in fetch_discover_movies page=%s attempt %s/%s",
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
                return {}
            await asyncio.sleep(attempt)

    logger.error(
        "fetch_discover_movies(page=%s) failed after %s attempts: %r",
        page,
        max_attempts,
        last_exc,
    )
    return {}


async def fetch_details(item_id: int, content_type: str = "movie") -> dict:
    """
    Детали фильма/сериала (без ретраев по HTTP 4xx/5xx, с ретраями по сетевым ошибкам).
    При фейле возвращает {}.
    """
    params = {
        "api_key": settings.tmdb_api_key,
        "language": "en-US",
    }

    max_attempts = 3
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(http2=False, timeout=TMDB_TIMEOUT) as client:
                resp = await client.get(
                    f"{BASE_URL}/{content_type}/{item_id}",
                    params=params,
                )
                resp.raise_for_status()
                return resp.json()

        except HTTPStatusError as e:
            logger.error(
                "TMDB details HTTP error %s %s (%s %s)",
                e.response.status_code,
                e.request.url,
                content_type,
                item_id,
            )
            await sync_errors_collection.insert_one({
                "endpoint": e.request.url.path,
                "url": str(e.request.url),
                "status_code": e.response.status_code,
                "params": dict(e.request.url.params),
                "response_text": e.response.text,
                "content_type": content_type,
                "item_id": item_id,
                "timestamp": datetime.utcnow(),
            })
            return {}

        except (ConnectError, ReadTimeout) as e:
            last_exc = e
            logger.warning(
                "TMDB details network error (%s %s) attempt %s/%s: %r",
                content_type,
                item_id,
                attempt,
                max_attempts,
                e,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": f"/{content_type}/{item_id}",
                    "content_type": content_type,
                    "item_id": item_id,
                    "error": f"network error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return {}
            await asyncio.sleep(attempt)

        except Exception as e:
            last_exc = e
            logger.exception(
                "Unexpected error in fetch_details(%s %s) attempt %s/%s",
                content_type,
                item_id,
                attempt,
                max_attempts,
            )
            if attempt == max_attempts:
                await sync_errors_collection.insert_one({
                    "endpoint": f"/{content_type}/{item_id}",
                    "content_type": content_type,
                    "item_id": item_id,
                    "error": f"unexpected error after {max_attempts} attempts: {repr(e)}",
                    "timestamp": datetime.utcnow(),
                })
                return {}
            await asyncio.sleep(attempt)

    logger.error(
        "fetch_details(%s %s) failed after %s attempts: %r",
        content_type,
        item_id,
        max_attempts,
        last_exc,
    )
    return {}
