from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings


client = AsyncIOMotorClient(settings.mongo_url)
db = client[settings.mongo_db]
movies_collection = db["movies"]
frame_reports_collection = db["frame_reports"]
sync_errors_collection = db.sync_errors
game_rounds_collection = db["game_rounds"]