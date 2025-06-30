from fastapi import APIRouter
from fastapi.responses import JSONResponse
from collections import Counter

from app.mongo import frame_reports_collection
from app.schemas import FrameReport


router = APIRouter()


@router.post("/report")
async def report_frame(report: FrameReport):
    await frame_reports_collection.insert_one(report.dict())
    return {"status": "ok", "reported": report.frame_path}


@router.get("/reports/stats")
async def get_report_stats():
    pipeline = [
        {
            "$group": {
                "_id": {
                    "movie_id": "$movie_id",
                    "frame_path": "$frame_path",
                    "content_type": "$content_type",
                },
                "count": {"$sum": 1},
                "reasons": {"$push": "$reason"}
            }
        }
    ]

    cursor = frame_reports_collection.aggregate(pipeline)
    results = []
    async for doc in cursor:
        reasons_list = [r for r in doc.get("reasons", []) if r]
        reason_counts = dict(Counter(reasons_list))

        results.append({
            "movie_id": doc["_id"]["movie_id"],
            "frame_path": doc["_id"]["frame_path"],
            "content_type": doc["_id"]["content_type"],
            "count": doc["count"],
            "reasons": reason_counts
        })

    return JSONResponse(content=results)
