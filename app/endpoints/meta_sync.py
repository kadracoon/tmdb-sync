from typing import Optional
from fastapi import APIRouter, Query
from app.mongo import movies_collection


router = APIRouter(prefix="/meta", tags=["meta"])


@router.get("/sync-status")
async def sync_status_meta(
    _type: str = Query("movie", pattern="^(movie|tv)$"),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
):
    """
    Возвращает по годам:
    - total: сколько фильмов в базе за год
    - last_popularity: последнее время синка popularity.desc
    - last_vote_count: последнее время синка vote_count.desc
    - popularity_coverage / vote_count_coverage: доля фильмов за год, у которых есть соответствующий таймштамп
    """
    match: dict = {"_type": _type}
    if year_from is not None or year_to is not None:
        y = {}
        if year_from is not None:
            y["$gte"] = year_from
        if year_to is not None:
            y["$lte"] = year_to
        match["year"] = y

    pipeline = [
        {"$match": match},
        {"$group": {
            "_id": "$year",
            "total": {"$sum": 1},
            "last_popularity": {"$max": "$last_popularity_sync_at"},
            "last_vote_count": {"$max": "$last_vote_count_sync_at"},
            "with_popularity": {"$sum": {"$cond": [{"$ifNull": ["$last_popularity_sync_at", False]}, 1, 0]}},
            "with_vote_count": {"$sum": {"$cond": [{"$ifNull": ["$last_vote_count_sync_at", False]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "year": "$_id",
            "total": 1,
            "last_popularity": 1,
            "last_vote_count": 1,
            "popularity_coverage": {
                "$cond": [{ "$gt": ["$total", 0] }, { "$divide": ["$with_popularity", "$total"] }, 0]
            },
            "vote_count_coverage": {
                "$cond": [{ "$gt": ["$total", 0] }, { "$divide": ["$with_vote_count", "$total"] }, 0]
            },
        }},
        {"$sort": {"year": 1}},
    ]

    items = await movies_collection.aggregate(pipeline).to_list(length=5_000)
    return {
        "type": _type,
        "year_from": year_from,
        "year_to": year_to,
        "items": items
    }
