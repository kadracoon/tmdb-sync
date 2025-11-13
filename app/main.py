from typing import Literal

from fastapi import BackgroundTasks, FastAPI, Query
from pydantic import BaseModel

from app.endpoints import frames, meta_sync, movies, reports
# from app.query import get_random_movie
# from app.meta import get_meta_info
from app.mongo import ensure_indexes, sync_cursors_collection
from app.scheduler import start_scheduler
# from app.sync import sync_category, sync_discover_movies
from app.sync_top import sync_top_by_vote_count
from app.sync_years import sync_years


app = FastAPI()
# app.include_router(catalog.router)
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
    # start_scheduler()


# @app.post("/sync/{category}")
# async def sync(category: Literal["popular", "top_rated", "upcoming"]):
#     result = await sync_category(category)
#     return result


# @app.post("/sync_discover")
# async def sync_discover(pages: int = 1):
#     return await sync_discover_movies(pages)


# @app.get("/movies/random")
# async def random_movie(
#     genre_id: int | None = Query(None),
#     country_code: str | None = Query(None),
#     year_from: int | None = Query(None),
#     year_to: int | None = Query(None),
#     is_animated: bool | None = Query(None),
#     _type: str | None = Query(None, pattern="^(movie|tv)$")
# ):
#     return await get_random_movie(
#         genre_id=genre_id,
#         country_code=country_code,
#         year_from=year_from,
#         year_to=year_to,
#         is_animated=is_animated,
#         _type=_type,
#     )


# @app.get("/meta")
# async def meta():
#     return await get_meta_info()


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
async def sync_status():
    doc = await sync_cursors_collection.find_one({"key": "top_vote_count_movie"}, {"_id": 0})  # type: ignore
    return doc or {"key": "top_vote_count_movie", "page": 0, "inserted": 0, "updated": 0}


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