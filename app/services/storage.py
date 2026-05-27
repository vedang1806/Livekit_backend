"""
app/services/storage.py — AWS S3 operations.

All boto3 calls are blocking; every public async function runs them in a
thread pool via asyncio.to_thread() so the event loop is never blocked.
"""

import asyncio
import boto3
from botocore.exceptions import ClientError

from app.config import settings

# Single S3 client — boto3 clients are thread-safe for concurrent use.
s3_client = boto3.client(
    "s3",
    region_name=settings.aws_region,
    aws_access_key_id=settings.aws_access_key,
    aws_secret_access_key=settings.aws_secret_key,
)


async def get_recording_presigned_url(session_id: str, expires_in: int = 3600) -> dict:
    """
    Return a presigned URL for the composite MP4.
    Raises FileNotFoundError if the file is not in S3 yet.
    """
    def _blocking():
        s3_key = f"TEMP/sessions/{session_id}/composite_recording.mp4"
        try:
            s3_client.head_object(Bucket=settings.s3_bucket, Key=s3_key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise FileNotFoundError(
                    f"Recording not ready yet for session '{session_id}'. "
                    "Retry after ~10–30s."
                )
            raise
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": s3_key},
            ExpiresIn=expires_in,
        )
        return {"s3_key": s3_key, "url": url, "expires_in": expires_in}

    return await asyncio.to_thread(_blocking)


async def check_composite_file(session_id: str) -> int | None:
    """Return ContentLength if the composite MP4 exists in S3, else None."""
    s3_key = f"TEMP/sessions/{session_id}/composite_recording.mp4"

    def _blocking():
        try:
            head = s3_client.head_object(Bucket=settings.s3_bucket, Key=s3_key)
            return head["ContentLength"]
        except ClientError:
            return None

    return await asyncio.to_thread(_blocking)


async def list_and_presign_session_files(session_id: str, expires_in: int) -> dict:
    """
    List all files under TEMP/sessions/{session_id}/ and return presigned URLs
    grouped by type: composite, audio[], video[].
    """
    prefix = f"TEMP/sessions/{session_id}/"

    def _blocking():
        paginator = s3_client.get_paginator("list_objects_v2")
        keys = [
            obj["Key"]
            for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]
        result: dict = {"composite": None, "audio": [], "video": []}
        for key in keys:
            tail = key[len(prefix):]
            try:
                url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": settings.s3_bucket, "Key": key},
                    ExpiresIn=expires_in,
                )
            except ClientError:
                continue

            if tail == "composite_recording.mp4":
                result["composite"] = {"key": key, "url": url}
            elif tail.startswith("audio/") and tail.endswith(".ogg"):
                result["audio"].append({"key": key, "url": url, "identity": tail[6:-4]})
            elif tail.startswith("video/") and tail.endswith(".webm"):
                result["video"].append({"key": key, "url": url, "identity": tail[6:-5]})
        return result

    return await asyncio.to_thread(_blocking)


async def list_and_presign_audio_files(session_id: str, expires_in: int) -> list[dict]:
    """Return presigned URLs for all per-participant OGG files."""
    prefix = f"TEMP/sessions/{session_id}/audio/"

    def _blocking():
        resp  = s3_client.list_objects_v2(Bucket=settings.s3_bucket, Prefix=prefix)
        files = []
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".ogg"):
                continue
            identity = key.split("/")[-1].removesuffix(".ogg")
            try:
                url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": settings.s3_bucket, "Key": key},
                    ExpiresIn=expires_in,
                )
                files.append({"identity": identity, "s3_key": key, "url": url})
            except ClientError:
                continue
        return files

    return await asyncio.to_thread(_blocking)
