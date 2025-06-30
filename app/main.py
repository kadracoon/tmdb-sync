from typing import Literal

from fastapi import FastAPI
from app.sync import sync_category, sync_discover_movies
from app.endpoints import reports


app = FastAPI()
app.include_router(reports.router)



@app.post("/sync/{category}")
async def sync(category: Literal["popular", "top_rated", "upcoming"]):
    result = await sync_category(category)
    return result


@app.post("/sync_discover")
async def sync_discover(pages: int = 1):
    return await sync_discover_movies(pages)

