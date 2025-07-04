import random
from datetime import datetime

from app.mongo import movies_collection, game_rounds_collection


async def generate_roundset(
    set_name: str,
    count: int = 100,
    _type: str = "movie",
    country_codes: list[str] | None = None,
    genre_ids: list[int] | None = None,
    is_animated: bool | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
):
    query = {
        "frames.0": {"$exists": True},
        "_type": _type,
    }

    if country_codes:
        query["country_codes"] = {"$in": country_codes}
    if genre_ids:
        query["genre_ids"] = {"$in": genre_ids}
    if is_animated is not None:
        query["is_animated"] = is_animated
    if year_from or year_to:
        query["release_date"] = {}
        if year_from:
            query["release_date"]["$gte"] = f"{year_from}-01-01"
        if year_to:
            query["release_date"]["$lte"] = f"{year_to}-12-31"

    pipeline = [
        {"$match": query},
        {"$sample": {"size": count * 3}}  # oversample to filter later
    ]

    candidates = await movies_collection.aggregate(pipeline).to_list(length=count * 3)
    rounds = []

    for movie in candidates:
        if len(rounds) >= count:
            break

        title = movie.get("title")
        if not title:
            continue

        frame = random.choice(movie.get("frames", []))
        if not frame:
            continue

        # Select wrong titles
        others = [m for m in candidates if m["id"] != movie["id"] and m.get("title")]
        wrong_titles = random.sample([m["title"] for m in others], k=3)

        rounds.append({
            "movie_id": movie["id"],
            "frame_path": frame["path"],
            "correct_title": title,
            "wrong_titles": wrong_titles,
            "title_ru": movie.get("title_ru"),
            "release_year": int(movie.get("release_date", "0000")[:4]),
            "_type": movie["_type"],
            "_category": movie.get("_category"),
            "genre_ids": movie.get("genre_ids", []),
            "country_codes": movie.get("country_codes", []),
            "is_animated": movie.get("is_animated", False),
            "difficulty": "medium",
            "set_name": set_name,
            "created_at": datetime.utcnow()
        })

    if rounds:
        await game_rounds_collection.insert_many(rounds)

    return {"inserted": len(rounds)}
