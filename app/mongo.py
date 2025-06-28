from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings


client = AsyncIOMotorClient(settings.mongo_url)
db = client[settings.mongo_db]
movies_collection = db["movies"]
reports_collection = db["frame_reports"]
