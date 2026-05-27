"""
config.py — loads all settings from .env via pydantic-settings.
Never hardcode credentials here — use the .env file.
"""

from pydantic_settings import BaseSettings
from typing import List


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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
