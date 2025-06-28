import httpx
from app.config import settings

BASE_URL = "https://api.themoviedb.org/3"
IMAGE_CDN = "https://image.tmdb.org/t/p/"


async def fetch_category(category: str, page: int = 1):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE_URL}/movie/{category}",
            params={"api_key": settings.tmdb_api_key, "language": "en-US", "page": page}
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_tv_category(category: str, page: int = 1):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE_URL}/tv/{category}",
            params={"api_key": settings.tmdb_api_key, "language": "en-US", "page": page}
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_best_frames(item_id: int, content_type: str = "movie", limit: int = 5) -> list[dict]:
    """Возвращает до `limit` лучших кадров с допустимым соотношением сторон"""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/{content_type}/{item_id}/images",
                params={"api_key": settings.tmdb_api_key}
            )
            resp.raise_for_status()
            data = resp.json()
            backdrops = data.get("backdrops", [])
            if not backdrops:
                return []

            def is_valid(b):
                ar = b.get("aspect_ratio", 0)
                return 1.6 <= ar <= 1.85 and b.get("vote_average", 0) >= 3

            filtered = list(filter(is_valid, backdrops))
            if not filtered:
                return []

            sorted_frames = sorted(
                filtered,
                key=lambda b: (b["vote_average"], b["width"]),
                reverse=True
            )[:limit]

            return [
                {
                    "path": f["file_path"],
                    "aspect_ratio": f["aspect_ratio"],
                    "vote_average": f["vote_average"],
                    "width": f["width"]
                }
                for f in sorted_frames
            ]

    except Exception as e:
        print(f"Failed to fetch frames for {content_type} {item_id}: {e}")
        return []

