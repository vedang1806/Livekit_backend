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

    # Admin panel credentials — override in .env
    admin_username: str = "admin"
    admin_password: str = "changeme"
    admin_secret:   str = "super-secret-session-key-change-in-prod"

    # Database — async URL used by the app, sync URL used by alembic
    database_url: str = "postgresql+asyncpg://livekit:livekit@postgres:5432/livekit_db"

    @property
    def database_url_sync(self) -> str:
        """psycopg2 URL for alembic migrations (sync driver)."""
        return self.database_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")

    # Redis — individual fields so they can be set separately in any environment
    redis_host:     str = "localhost"
    redis_port:     int = 6379
    redis_db:       int = 0
    redis_username: str = "default"
    redis_password: str = ""

    @property
    def redis_url(self) -> str:
        """Build Redis URL from individual fields."""
        auth = f"{self.redis_username}:{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
