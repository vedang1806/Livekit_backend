"""
app/config.py — loads all settings from .env via pydantic-settings.
Never hardcode credentials here — use the .env file.
"""

from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LiveKit
    livekit_url:        str
    livekit_api_key:    str
    livekit_api_secret: str

    # AWS / S3
    aws_access_key: str
    aws_secret_key: str
    aws_region:     str = "us-east-1"
    s3_bucket:      str

    # CORS
    cors_origins: List[str] = ["http://localhost:3000"]

    # Database — async URL used by the app, sync URL used by alembic
    database_url: str = "postgresql+asyncpg://livekit:livekit@postgres:5432/livekit_db"

    @property
    def database_url_sync(self) -> str:
        """psycopg2 URL for alembic migrations (sync driver)."""
        return self.database_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
