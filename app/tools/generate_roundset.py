import asyncio
from app.generate_rounds import generate_roundset  # путь к твоей функции
from app.mongo import connect_to_mongo, close_mongo

async def main():
    await connect_to_mongo()
    result = await generate_roundset(
        set_name="roundset_01",
        count=100,
        _type="movie",
        country_codes=["US"],
        genre_ids=[18, 53],
        is_animated=False,
        year_from=1980,
        year_to=2023
    )
    print(result)
    await close_mongo()

if __name__ == "__main__":
    asyncio.run(main())
