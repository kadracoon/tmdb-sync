import asyncio
from app.sync import sync_discover_movies


if __name__ == "__main__":
    result = asyncio.run(sync_discover_movies(pages=50))
    print(result)
