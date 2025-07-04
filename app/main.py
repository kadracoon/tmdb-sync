from typing import Literal

from fastapi import FastAPI, Query

from app.endpoints import reports
from app.meta import get_meta_info
from app.scheduler import start_scheduler
from app.sync import sync_category, sync_discover_movies
from app.query import get_random_movie


app = FastAPI()
app.include_router(reports.router)


@app.on_event("startup")
async def startup_event():
    start_scheduler()


@app.post("/sync/{category}")
async def sync(category: Literal["popular", "top_rated", "upcoming"]):
    result = await sync_category(category)
    return result


@app.post("/sync_discover")
async def sync_discover(pages: int = 1):
    return await sync_discover_movies(pages)


@app.get("/movies/random")
async def random_movie(
    genre_id: int | None = Query(None),
    country_code: str | None = Query(None),
    year_from: int | None = Query(None),
    year_to: int | None = Query(None),
    is_animated: bool | None = Query(None),
    _type: str | None = Query(None, pattern="^(movie|tv)$")
):
    return await get_random_movie(
        genre_id=genre_id,
        country_code=country_code,
        year_from=year_from,
        year_to=year_to,
        is_animated=is_animated,
        _type=_type,
    )

@app.get("/meta")
async def meta():
    return await get_meta_info()