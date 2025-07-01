from app.mongo import movies_collection
from pymongo import ASCENDING
import random


async def get_random_movie(
    genre_id: int | None,
    country_code: str | None,
    year_from: int | None,
    year_to: int | None,
    is_animated: bool | None,
    _type: str | None,
):
    query = {"frame_url": {"$exists": True}}

    print("MongoDB query:", query)

    if genre_id is not None:
        query["genre_ids"] = {"$in": [genre_id]}
    if country_code is not None:
        query["country_codes"] = {"$in": [country_code]}
    if is_animated is not None:
        query["is_animated"] = is_animated
    if _type is not None:
        query["_type"] = _type
    if year_from or year_to:
        query["release_date"] = {}
        if year_from:
            query["release_date"]["$gte"] = f"{year_from}-01-01"
        if year_to:
            query["release_date"]["$lte"] = f"{year_to}-12-31"

    pipeline = [
        {"$match": query},
        {"$sample": {"size": 1}}
    ]

    doc = await movies_collection.aggregate(pipeline).to_list(length=1)
    if doc:
        doc[0].pop("_id", None)
        return doc[0]
    return None

