from datetime import datetime
import httpx
from httpx import HTTPStatusError

from app.config import settings
from app.logging import logger
from app.mongo import movies_collection, sync_cursors_collection, sync_errors_collection
from app.catalog.upsert import upsert_movie
from app.sync import enrich_common_fields, fetch_title_ru, fetch_details
from app.tmdb_client import fetch_backdrops


MAX_PAGES = 500


def _cursor_key(year: int, content_type: str) -> str:
    # отдельный курсор на КАЖДЫЙ год и тип, чтобы резюмилось точно
    return f"years:{content_type}:{year}"


async def _get_cursor(year: int, content_type: str) -> dict:
    key = _cursor_key(year, content_type)
    cur = await sync_cursors_collection.find_one({"key": key})  # type: ignore
    return cur or {"key": key, "page": 0, "inserted": 0, "updated": 0, "ts": datetime.utcnow()}


async def _save_cursor(cur: dict):
    cur["ts"] = datetime.utcnow()
    await sync_cursors_collection.update_one({"key": cur["key"]}, {"$set": cur}, upsert=True)  # type: ignore


async def _fetch_discover_year_page(year: int, page: int, content_type: str = "movie", sort_by: str = "popularity.desc") -> dict:
    # берём discover по году. Для фильмов используем primary_release_date.*,
    # для сериалов — first_air_date.* (если захочешь поддержать "tv")
    base = "movie" if content_type == "movie" else "tv"
    params = {
        "api_key": settings.tmdb_api_key,
        "language": "en-US",
        "include_adult": False,
        "include_video": False,
        "page": page,
    }
    if base == "movie":
        params.update({
            "sort_by": sort_by,
            "primary_release_date.gte": f"{year}-01-01",
            "primary_release_date.lte": f"{year}-12-31",
        })
    else:
        params.update({
            "sort_by": sort_by,
            "first_air_date.gte": f"{year}-01-01",
            "first_air_date.lte": f"{year}-12-31",
        })

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"https://api.themoviedb.org/3/discover/{base}", params=params)
        r.raise_for_status()
        return r.json()


async def sync_years(
    start_year: int,
    end_year: int | None = None,
    limit: int = 5000,
    resume: bool = True,
    content_type: str = "movie",
    sort_by: str = "popularity.desc"
) -> dict:
    """
    Синхронизирует фильмы/сериалы по диапазону лет.
    - Идёт год за годом (чтобы не упереться в лимит 500 страниц).
    - На каждый год ведётся отдельный курсор (resume).
    - Не перезатирает incorrect_frames, пересчитывает backdrop_path (через upsert_movie).
    """
    end_year = end_year or start_year
    if end_year < start_year:
        start_year, end_year = end_year, start_year

    processed_total = 0
    inserted_total = 0
    updated_total = 0
    last_year = start_year

    for year in range(start_year, end_year + 1):
        if processed_total >= limit:
            break

        cur = await _get_cursor(year, content_type)
        page = (cur["page"] + 1) if resume and cur["page"] > 0 else 1
        inserted_year = 0
        updated_year = 0
        processed_year = 0

        while page <= MAX_PAGES and processed_total < limit:
            try:
                data = await _fetch_discover_year_page(year, page, content_type=content_type, sort_by=sort_by)
            except HTTPStatusError as e:
                logger.error("TMDB discover error %s %s", e.response.status_code, e.request.url)
                await sync_errors_collection.insert_one({
                    "endpoint": e.request.url.path,
                    "url": str(e.request.url),
                    "status_code": e.response.status_code,
                    "params": dict(e.request.url.params),
                    "response_text": e.response.text,
                    "timestamp": datetime.utcnow(),
                })
                break
            except Exception as e:
                logger.exception("discover fatal at page %s (year=%s)", page, year)
                await sync_errors_collection.insert_one({
                    "endpoint": "discover/year",
                    "year": year,
                    "error": str(e),
                    "timestamp": datetime.utcnow(),
                })
                break

            results = data.get("results") or []
            if not results:
                break

            for item in results:
                if processed_total >= limit:
                    break

                tmdb_id = item.get("id")
                if not tmdb_id:
                    continue

                try:
                    # детали
                    det = await fetch_details(tmdb_id, content_type)
                    if not det:
                        continue
                    item["production_countries"] = det.get("production_countries", [])

                    # общие поля
                    item = enrich_common_fields(item, content_type, f"discover_year_{year}")
                    item["title_ru"] = await fetch_title_ru(tmdb_id, content_type)

                    # все кадры
                    item["frames"] = await fetch_backdrops(tmdb_id, content_type)

                    item["_sort_by"] = sort_by

                    # апсёрт
                    before = await movies_collection.find_one({"id": tmdb_id, "_type": content_type}, {"_id": 1})
                    await upsert_movie(item)
                    if before:
                        updated_year += 1
                    else:
                        inserted_year += 1

                    processed_year += 1
                    processed_total += 1

                except HTTPStatusError as e:
                    logger.error("TMDB details/frames error %s %s", e.response.status_code, e.request.url)
                    await sync_errors_collection.insert_one({
                        "endpoint": e.request.url.path,
                        "url": str(e.request.url),
                        "status_code": e.response.status_code,
                        "params": dict(e.request.url.params),
                        "response_text": e.response.text,
                        "timestamp": datetime.utcnow(),
                    })
                except Exception as e:
                    logger.exception("Unexpected while upserting %s", tmdb_id)
                    await sync_errors_collection.insert_one({
                        "endpoint": "upsert_movie",
                        "error": str(e),
                        "movie_id": tmdb_id,
                        "timestamp": datetime.utcnow(),
                    })

            # сохраняем курсор по году
            await _save_cursor({
                "key": _cursor_key(year, content_type),
                "page": page,
                "inserted": (cur.get("inserted", 0) + inserted_year),
                "updated": (cur.get("updated", 0) + updated_year),
                "ts": datetime.utcnow()
            })
            page += 1

        inserted_total += inserted_year
        updated_total += updated_year
        last_year = year

    return {
        "status": "ok",
        "start_year": start_year,
        "end_year": end_year,
        "last_year": last_year,
        "processed": processed_total,
        "inserted": inserted_total,
        "updated": updated_total,
    }
