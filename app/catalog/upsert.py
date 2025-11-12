from datetime import datetime
from typing import Dict, Any, List

from app.mongo import movies_collection
from app.utils.frames import pick_backdrop


def _normalize_frames(raw_frames: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    """Приводим фреймы к единому виду со свойством 'path'."""

    frames = raw_frames or []
    norm: List[Dict[str, Any]] = []
    for f in frames:
        # совместимость: могли прийти 'frame_path' или 'path'
        path = f.get("path") or f.get("frame_path")
        if not path:
            continue
        norm.append({
            "path": path,
            "aspect_ratio": f.get("aspect_ratio"),
            "vote_average": f.get("vote_average"),
            "width": f.get("width"),
        })

    # убираем дубликаты по path, сохраняя лучший вариант по width
    by_path: Dict[str, Dict[str, Any]] = {}

    for f in norm:
        p = f["path"]
        cur = by_path.get(p)
        if not cur or (f.get("width", 0) or 0) > (cur.get("width", 0) or 0):
            by_path[p] = f

    return list(by_path.values())


def _extract_year(release_date: str | None) -> int | None:
    if not release_date or len(release_date) < 4:
        return None
    try:
        return int(release_date[:4])
    except ValueError:
        return None


async def upsert_movie(doc: Dict[str, Any]) -> None:
    """Мягкий апсёрт фильма:
    - нормализуем frames
    - добавляем/пересчитываем year, is_animated, country_codes
    - сохраняем/не перетираем incorrect_frames
    - считаем backdrop_path по валидным кадрам
    - created_at только на insert, synced_at всегда
    - отмечаем время последнего синка по типу сортировки (popularity/vote_count)
    """

    doc = dict(doc)

    # нормализация фреймов
    doc["frames"] = _normalize_frames(doc.get("frames"))

    # вычислим производные поля
    doc["year"] = _extract_year(doc.get("release_date"))
    doc["is_animated"] = 16 in (doc.get("genre_ids") or [])

    countries = doc.get("production_countries") or []
    doc["country_codes"] = [c["iso_3166_1"] for c in countries if c.get("iso_3166_1")]

    doc["synced_at"] = datetime.utcnow()

    # извлечём sort_by (если есть) — чтобы знать тип синка
    sort_by = doc.get("_sort_by") or doc.get("sort_by")

    # подмешиваем уже существующие вручную отметки и только потом считаем backdrop
    existing = await movies_collection.find_one(
        {"id": doc["id"], "_type": doc.get("_type", "movie")},
        {"incorrect_frames": 1, "backdrop_path": 1}
    )

    if existing:
        # сохраняем ручные пометки
        if "incorrect_frames" in existing:
            doc["incorrect_frames"] = existing["incorrect_frames"]
        # если backdrop_path уже был — не затираем без надобности
        if "backdrop_path" in existing and not doc.get("backdrop_path"):
            doc["backdrop_path"] = existing["backdrop_path"]

    # пересчёт валидного кадра
    doc["backdrop_path"] = pick_backdrop(doc)

    # выставляем метки синхронизации в зависимости от типа
    update_fields = {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}}

    if sort_by == "popularity.desc":
        update_fields["$set"]["last_popularity_sync_at"] = datetime.utcnow()
    elif sort_by == "vote_count.desc":
        update_fields["$set"]["last_vote_count_sync_at"] = datetime.utcnow()

    # апсёрт
    await movies_collection.update_one(
        {"id": doc["id"], "_type": doc.get("_type", "movie")},
        update_fields,
        upsert=True
    )
