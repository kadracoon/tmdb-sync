from typing import Optional, Dict, Any, List


def pick_backdrop(doc: Dict[str, Any]) -> Optional[str]:
    """Выбрать лучший кадр не из incorrect_frames.
    Сортируем по vote_average desc, затем по width desc.
    Возвращаем путь '/abc.jpg' или None.
    """

    frames: List[Dict[str, Any]] = doc.get("frames") or []
    bad = set(doc.get("incorrect_frames") or [])
    valid = [f for f in frames if f.get("path") and f["path"] not in bad]

    if not valid:
        return None
    
    valid.sort(key=lambda f: (f.get("vote_average", 0) or 0, f.get("width", 0) or 0), reverse=True)

    return valid[0]["path"]
