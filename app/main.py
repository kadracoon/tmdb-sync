from typing import Literal

from fastapi import FastAPI
from app.sync import sync_category, sync_discover_movies
from app.schemas import FrameReport
from app.mongo import reports_collection


app = FastAPI()


@app.post("/sync/{category}")
async def sync(category: Literal["popular", "top_rated", "upcoming"]):
    result = await sync_category(category)
    return result


@app.post("/sync_discover")
async def sync_discover(pages: int = 1):
    return await sync_discover_movies(pages)


@app.post("/report")
async def report_frame(report: FrameReport):
    await reports_collection.insert_one(report.dict())
    return {"status": "ok", "reported": report.frame_path}
