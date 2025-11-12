from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException
from app.mongo import movies_collection


router = APIRouter(prefix="/movies", tags=["movies"])


def _project_movie(doc: dict) -> dict:
    doc.pop("_id", None)
    keep = {
        "id", "title", "title_ru", "name", "_type",
        "genre_ids", "release_date", "popularity",
        "vote_average", "country_codes", "is_animated",
        "frames"
    }
    return {k: v for k, v in doc.items() if k in keep}


@router.get("/search")
async def search_movies(
    genre_id: int | None = Query(None),
    country_code: str | None = Query(None),
    year_from: int | None = Query(None),
    year_to: int | None = Query(None),
    is_animated: bool | None = Query(None),
    _type: str | None = Query(None, pattern="^(movie|tv)$"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),  # ← ДОБАВИЛИ
    # ← РАСШИРИЛИ варианты сортировки
    sort_by: str = Query(
        "popularity",
        pattern="^(popularity|vote_average|vote_count|release_date|year)$"
    ),
    order: str = Query("desc", pattern="^(asc|desc)$"),
):
    q = {"frames": {"$exists": True, "$ne": []}}
    if genre_id is not None:
        q["genre_ids"] = {"$in": [genre_id]}
    if country_code is not None:
        q["country_codes"] = {"$in": [country_code]}
    if is_animated is not None:
        q["is_animated"] = is_animated
    if _type is not None:
        q["_type"] = _type
    if year_from or year_to:
        q["release_date"] = {}
        if year_from:
            q["release_date"]["$gte"] = f"{year_from}-01-01"
        if year_to:
            q["release_date"]["$lte"] = f"{year_to}-12-31"

    sort_dir = -1 if order == "desc" else 1

    # если сортируем по year, но поля нет — можно fallback на release_date (опционально)
    sort_field = sort_by
    # if sort_by == "year" and not await movies_collection.count_documents({"year": {"$exists": True}}):
    #     sort_field = "release_date"

    cursor = (
        movies_collection
        .find(q)
        .sort(sort_field, sort_dir)
        .skip(skip)                     # ← ДОБАВИЛИ
        .limit(limit)
    )
    docs = [_project_movie(d) async for d in cursor]
    return {"items": docs}


@router.get("/by-ids")
async def get_movies_by_ids(ids: List[int] = Query(..., description="Repeat ?ids=..."),
                            _type: Optional[str] = Query(None, pattern="^(movie|tv)$")):
    q = {"id": {"$in": ids}}
    if _type:
        q["_type"] = _type
    cursor = movies_collection.find(q)
    docs = [ _project_movie(d) async for d in cursor ]
    return {"items": docs}


@router.get("/{tmdb_id}")
async def get_movie(tmdb_id: int, _type: Optional[str] = Query(None, pattern="^(movie|tv)$")):
    q = {"id": tmdb_id}
    if _type:
        q["_type"] = _type
    doc = await movies_collection.find_one(q)
    if not doc:
        raise HTTPException(404, "movie not found")
    return _project_movie(doc)


@router.get("/{tmdb_id}/frames")
async def get_frames(tmdb_id: int, _type: Optional[str] = Query(None, pattern="^(movie|tv)$")):
    q = {"id": tmdb_id}
    if _type:
        q["_type"] = _type
    doc = await movies_collection.find_one(q, {"frames": 1, "_id": 0})
    if not doc:
        raise HTTPException(404, "not found")
    return {"frames": doc.get("frames", [])}
