from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.mongo import frame_reports_collection, movies_collection
from app.utils.frames import pick_backdrop


router = APIRouter(prefix="/frames", tags=["frames"])


class FrameReportIn(BaseModel):
    movie_id: int
    path: str = Field(..., description="TMDB image path, e.g. '/abc.jpg'")
    reason: str = Field("not_a_scene", max_length=200)
    content_type: str = Field("movie", pattern="^(movie|tv)$")
    reporter: str | None = Field(None, description="optional user identifier")


@router.post("/report")
async def report_frame(payload: FrameReportIn):
    # сохраняем как простую запись (модерация вручную)
    await frame_reports_collection.insert_one({
        "movie_id": payload.movie_id,
        "path": payload.path,
        "reason": payload.reason,
        "content_type": payload.content_type,
        "reporter": payload.reporter,
        "timestamp": datetime.utcnow(),
    })
    return {"ok": True}


class IncorrectFramesIn(BaseModel):
    paths: List[str] = Field(..., min_items=1)


@router.post("/movies/{movie_id}/incorrect")
async def mark_incorrect(movie_id: int, body: IncorrectFramesIn, _type: str = "movie"):
    """
    Добавляет пути в incorrect_frames и пересчитывает backdrop_path.
    Идемпотентно (используем $addToSet).
    """
    # убедимся, что фильм есть
    doc = await movies_collection.find_one({"id": movie_id, "_type": _type})
    if not doc:
        raise HTTPException(404, "movie not found")

    # добавляем пути без дублей
    await movies_collection.update_one(
        {"id": movie_id, "_type": _type},
        {"$addToSet": {"incorrect_frames": {"$each": body.paths}}},
    )

    # перечитываем документ и пересчитываем обложку
    doc = await movies_collection.find_one({"id": movie_id, "_type": _type})
    new_backdrop = pick_backdrop(doc or {})

    await movies_collection.update_one(
        {"id": movie_id, "_type": _type},
        {"$set": {"backdrop_path": new_backdrop}},
    )

    # небольшой UX: вернём, какие из paths реально присутствуют в frames, а какие нет
    frames_paths = {f.get("path") for f in (doc.get("frames") or [])}
    present = [p for p in body.paths if p in frames_paths]
    missing = [p for p in body.paths if p not in frames_paths]

    return {
        "ok": True,
        "backdrop_path": new_backdrop,
        "added": body.paths,
        "present_in_frames": present,
        "not_in_frames": missing,
    }


class UnmarkIn(BaseModel):
    paths: List[str] = Field(..., min_items=1)


@router.post("/movies/{movie_id}/unmark-incorrect")
async def unmark_incorrect(movie_id: int, body: UnmarkIn, _type: str = "movie"):
    """
    Удаляет пути из incorrect_frames и пересчитывает backdrop_path.
    """
    doc = await movies_collection.find_one({"id": movie_id, "_type": _type})
    if not doc:
        raise HTTPException(404, "movie not found")

    await movies_collection.update_one(
        {"id": movie_id, "_type": _type},
        {"$pull": {"incorrect_frames": {"$in": body.paths}}},
    )

    doc = await movies_collection.find_one({"id": movie_id, "_type": _type})
    new_backdrop = pick_backdrop(doc or {})

    await movies_collection.update_one(
        {"id": movie_id, "_type": _type},
        {"$set": {"backdrop_path": new_backdrop}},
    )

    return {"ok": True, "backdrop_path": new_backdrop, "removed": body.paths}
