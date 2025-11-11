from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings


client = AsyncIOMotorClient(settings.mongo_url)
db = client[settings.mongo_db]

# Collections
movies_collection = db["movies"]
frame_reports_collection = db["frame_reports"]
sync_errors_collection = db["sync_errors"]
sync_cursors_collection = db["sync_cursors"]  # for long tasks

async def ensure_indexes() -> None:
    await movies_collection.create_index([("id", 1), ("_type", 1)], unique=True)
    await movies_collection.create_index([("vote_count", -1)])
    await movies_collection.create_index([("popularity", -1)])
    await movies_collection.create_index([("genre_ids", 1)])
    await movies_collection.create_index([("country_codes", 1)])
    await movies_collection.create_index([("release_date", 1)])
    await movies_collection.create_index([("year", 1)])
    await movies_collection.create_index([("frames.path", 1)])
    await frame_reports_collection.create_index([("movie_id", 1)])
    await frame_reports_collection.create_index([("path", 1)])
    await frame_reports_collection.create_index([("timestamp", -1)])
