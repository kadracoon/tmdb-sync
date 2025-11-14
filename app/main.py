from datetime import datetime, timedelta
from typing import Literal

from fastapi import BackgroundTasks, FastAPI, Query
from pydantic import BaseModel

from app.endpoints import frames, meta_sync, movies, reports
from app.mongo import ensure_indexes, sync_cursors_collection, sync_errors_collection
from app.sync_top import sync_top_by_vote_count
from app.sync_years import sync_years
from app.tmdb_client import close_tmdb_client


app = FastAPI()
app.include_router(frames.router)
app.include_router(reports.router)
app.include_router(meta_sync.router)
app.include_router(movies.router)


class SyncYearsPayload(BaseModel):
    start_year: int
    end_year: int | None = None
    limit: int = 5000
    resume: bool = True
    content_type: str = "movie"
    sort_by: str = "vote_count.desc"


@app.on_event("startup")
async def startup_event():
    await ensure_indexes()


@app.on_event("shutdown")
async def shutdown_event():
    await close_tmdb_client()


@app.post("/sync/top-votes", status_code=202)
async def sync_by_top_votes(
    background_tasks: BackgroundTasks,
    limit: int = Query(10_000, ge=1, le=50_000),
    resume: bool = True,
    start_page: int | None = None,
):
    """
    Стартует синхронизацию топа по vote_count в фоне.
    Ответ возвращаем сразу, прогресс смотрим по sync_cursors_collection.
    """
    background_tasks.add_task(
        sync_top_by_vote_count,
        limit=limit,
        resume=resume,
        start_page=start_page,
    )
    return {
        "status": "accepted",
        "detail": "sync_top_by_vote_count started in background",
        "params": {"limit": limit, "resume": resume, "start_page": start_page},
    }


@app.get("/sync/status")
async def get_sync_status():
    """
    Краткий статус синхронизаций:

    - top-votes: прогресс по курсу top_vote_count_movie
    - years: список курсоров по годам (years:{content_type}:{year})
    - errors: количество ошибок из sync_errors_collection
    """
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)
    one_day_ago = now - timedelta(days=1)

    # --- собираем курсоры ---
    top_votes: dict | None = None
    years: list[dict] = []

    async for cur in sync_cursors_collection.find({}):  # type: ignore
        key = cur.get("key")
        if not key:
            continue

        # топ по голосам
        if key == "top_vote_count_movie":
            top_votes = {
                "key": key,
                "page": cur.get("page", 0),
                "inserted": cur.get("inserted", 0),
                "updated": cur.get("updated", 0),
                "ts": cur.get("ts"),
            }
            continue

        # курсоры по годам: years:{content_type}:{year}
        if isinstance(key, str) and key.startswith("years:"):
            # ожидаем формат years:movie:1999 или years:tv:2010
            try:
                _, content_type, year_str = key.split(":", 2)
                year = int(year_str)
            except ValueError:
                # странный ключ — пропускаем
                continue

            years.append({
                "key": key,
                "content_type": content_type,
                "year": year,
                "page": cur.get("page", 0),
                "inserted": cur.get("inserted", 0),
                "updated": cur.get("updated", 0),
                "ts": cur.get("ts"),
            })

    # чтобы было красиво — отсортируем годы
    years.sort(key=lambda x: (x["content_type"], x["year"]))

    # --- собираем статистику по ошибкам ---
    errors_last_hour = await sync_errors_collection.count_documents({  # type: ignore
        "timestamp": {"$gte": one_hour_ago},
    })
    errors_last_day = await sync_errors_collection.count_documents({  # type: ignore
        "timestamp": {"$gte": one_day_ago},
    })
    errors_total = await sync_errors_collection.estimated_document_count()  # type: ignore

    return {
        "top_votes": top_votes,
        "years": years,
        "errors": {
            "last_hour": errors_last_hour,
            "last_24h": errors_last_day,
            "total": errors_total,
        },
        "generated_at": now,
    }


@app.post("/sync/years", status_code=202)
async def sync_by_years(
    payload: SyncYearsPayload,
    background_tasks: BackgroundTasks,
):
    """
    Стартует синхронизацию по годам в фоне.
    """
    background_tasks.add_task(
        sync_years,
        payload.start_year,
        payload.end_year,
        payload.limit,
        payload.resume,
        payload.content_type,
        payload.sort_by,
    )
    return {
        "status": "accepted",
        "detail": "sync_years started in background",
        "params": payload.model_dump(),
    }


@app.get("/sync/status/years")
async def sync_status_years(
    year: int | None = Query(None),
    end_year: int | None = Query(None),
    _type: str = Query("movie", pattern="^(movie|tv)$"),
):
    """
    Возвращает курсоры year-sync.
    - Если указан только year: вернёт один курсор years:<type>:<year> (или пусто).
    - Если указан диапазон year..end_year: вернёт список курсоров по годам.
    - Если не указано ничего: вернёт все курсоры для указанного _type.
    """
    # 1) Один год
    if year is not None and end_year is None:
        key = f"years:{_type}:{year}"
        doc = await sync_cursors_collection.find_one({"key": key}, {"_id": 0})
        return doc or {"key": key, "page": 0, "inserted": 0, "updated": 0}

    # 2) Диапазон годов
    if year is not None and end_year is not None:
        if end_year < year:
            year, end_year = end_year, year
        keys = [f"years:{_type}:{y}" for y in range(year, end_year + 1)]
        cursor = sync_cursors_collection.find({"key": {"$in": keys}}, {"_id": 0})
        items = await cursor.to_list(length=len(keys))
        # добавим отсутствующие ключи с нулевыми значениями
        found = {i["key"] for i in items}
        for k in keys:
            if k not in found:
                items.append({"key": k, "page": 0, "inserted": 0, "updated": 0})
        # отсортируем по году
        items.sort(key=lambda x: int(x["key"].rsplit(":", 1)[-1]))
        return {"items": items}

    # 3) Все курсоры этого типа
    # ключи вида: years:<type>:<year>
    regex = f"^years:{_type}:\\d+$"
    cursor = sync_cursors_collection.find({"key": {"$regex": regex}}, {"_id": 0})
    items = await cursor.to_list(length=10_000)
    # упорядочим по году
    items.sort(key=lambda x: int(x["key"].rsplit(":", 1)[-1]))
    return {"items": items}


@app.post("/sync/years/current")
async def sync_current_year(limit: int = 5000, resume: bool = True):
    """
    Синхронизирует фильмы текущего года по popularity.desc — для ежемесячного обновления.
    """
    year = datetime.utcnow().year
    return await sync_years(start_year=year, end_year=year, limit=limit, resume=resume, sort_by="popularity.desc")


@app.post("/sync/years/finalize")
async def sync_finalize_year(year: int, limit: int = 5000, resume: bool = True):
    """
    Финализирует фильмы указанного года по vote_count.desc — для закрытия года.
    """
    return await sync_years(start_year=year, end_year=year, limit=limit, resume=resume, sort_by="vote_count.desc")
