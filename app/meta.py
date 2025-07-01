from app.mongo import movies_collection


async def get_meta_info():
    pipeline = [
        {
            "$facet": {
                "genre_ids": [
                    {"$unwind": "$genre_ids"},
                    {"$group": {"_id": "$genre_ids"}}
                ],
                "country_codes": [
                    {"$unwind": "$country_codes"},
                    {"$group": {"_id": "$country_codes"}}
                ],
                "years": [
                    {
                        "$project": {
                            "year": {"$toInt": {"$substr": ["$release_date", 0, 4]}}
                        }
                    },
                    {"$group": {"_id": "$year"}}
                ]
            }
        }
    ]

    result = await movies_collection.aggregate(pipeline).to_list(length=1)
    if not result:
        return {"genres": [], "country_codes": [], "years": []}

    data = result[0]
    return {
        "genres": sorted(g["_id"] for g in data.get("genre_ids", [])),
        "country_codes": sorted(c["_id"] for c in data.get("country_codes", [])),
        "years": sorted(y["_id"] for y in data.get("years", []) if y["_id"]),
    }
