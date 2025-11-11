from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from app.mongo import movies_collection

router = APIRouter(prefix="/movies", tags=["catalog"])


@router.get("/{movie_id}")
async def get_movie(movie_id: int, _type: str = "movie"):
    """Возвращает полный документ фильма"""
    doc = await movies_collection.find_one({"id": movie_id, "_type": _type}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="movie not found")
    return doc


@router.get("/{movie_id}/frames")
async def get_movie_frames(movie_id: int, _type: str = "movie"):
    """Возвращает список кадров и incorrect_frames"""
    doc = await movies_collection.find_one(
        {"id": movie_id, "_type": _type},
        {"_id": 0, "frames": 1, "incorrect_frames": 1, "backdrop_path": 1},
    )
    if not doc:
        raise HTTPException(status_code=404, detail="movie not found")
    return doc


@router.get("/search")
async def search_movies(
    query: Optional[str] = Query(None, description="часть названия (англ или рус)"),
    genre_id: Optional[int] = None,
    country: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    is_animated: Optional[bool] = None,
    sort_by: str = Query("vote_count", pattern="^(vote_count|popularity|release_date)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(20, ge=1, le=100),
    skip: int = Query(0, ge=0),
):
    """
    Простая фильтрация и сортировка каталога.
    По умолчанию возвращает только фильмы, где есть frames.
    """
    mongo_filter = {"frames": {"$exists": True, "$ne": []}}

    if query:
        mongo_filter["$or"] = [
            {"title": {"$regex": query, "$options": "i"}},
            {"title_ru": {"$regex": query, "$options": "i"}},
        ]
    if genre_id is not None:
        mongo_filter["genre_ids"] = genre_id
    if country:
        mongo_filter["country_codes"] = country
    if year_from or year_to:
        yfilter = {}
        if year_from:
            yfilter["$gte"] = f"{year_from}-01-01"
        if year_to:
            yfilter["$lte"] = f"{year_to}-12-31"
        mongo_filter["release_date"] = yfilter
    if is_animated is not None:
        mongo_filter["is_animated"] = is_animated

    sort_dir = -1 if order == "desc" else 1

    cursor = (
        movies_collection.find(mongo_filter, {"_id": 0})
        .sort(sort_by, sort_dir)
        .skip(skip)
        .limit(limit)
    )
    results = await cursor.to_list(length=limit)
    return {"count": len(results), "results": results}
