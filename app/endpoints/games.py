from fastapi import APIRouter
from app.generate_rounds import generate_roundset


router = APIRouter()


@router.post("/generate_rounds")
async def generate(set_name: str):
    result = await generate_roundset(set_name=set_name)
    return result
