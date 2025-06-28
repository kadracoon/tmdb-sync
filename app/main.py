from fastapi import FastAPI
from app.sync import sync_category
from app.schemas import FrameReport
from app.mongo import reports_collection


app = FastAPI()


@app.post("/sync/{category}")
async def sync(category: str):
    result = await sync_category(category)
    return result


@app.post("/report")
async def report_frame(report: FrameReport):
    await reports_collection.insert_one(report.dict())
    return {"status": "ok", "reported": report.frame_path}
