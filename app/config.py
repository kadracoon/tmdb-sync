from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    tmdb_api_key: str
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "tmdb"

    class Config:
        env_file = ".env"


settings = Settings()
