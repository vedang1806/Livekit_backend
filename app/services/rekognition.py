"""
app/services/rekognition.py — HIPAA compliance face detection.

Pipeline per recording:
  1. Download composite WebM/MP4 from S3 to a temp file.
  2. Extract one frame every FRAME_INTERVAL_SECONDS using ffmpeg.
  3. Call AWS Rekognition detect_faces() on each frame (synchronous, fast).
  4. Flag any frame where detected face count > expected participant count.
  5. Return a ComplianceResult with all evidence.
"""

import asyncio
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import boto3

from app.config import settings

logger = logging.getLogger(__name__)

FRAME_INTERVAL_SECONDS = 5   # sample one frame every N seconds
MIN_FACE_CONFIDENCE    = 80  # Rekognition confidence threshold (%)


@dataclass
class FrameViolation:
    timestamp_sec: float
    frame_path:    str
    faces_detected: int
    expected:       int


@dataclass
class ComplianceResult:
    passed:          bool
    frames_analyzed: int
    max_faces:       int
    violations:      list[FrameViolation] = field(default_factory=list)
    error:           str | None = None

    def violation_summary(self) -> list[dict]:
        return [
            {
                "timestamp_sec":  v.timestamp_sec,
                "faces_detected": v.faces_detected,
                "expected":       v.expected,
            }
            for v in self.violations
        ]


def _download_from_s3(s3_key: str, dest_path: str) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key,
        aws_secret_access_key=settings.aws_secret_key,
        region_name=settings.aws_region,
    )
    s3.download_file(settings.s3_bucket, s3_key, dest_path)
    logger.info(f"Downloaded s3://{settings.s3_bucket}/{s3_key} → {dest_path}")


def _extract_frames(video_path: str, output_dir: str) -> list[tuple[str, float]]:
    """
    Use ffmpeg to extract one frame every FRAME_INTERVAL_SECONDS.
    Returns list of (frame_path, timestamp_seconds).
    """
    pattern = os.path.join(output_dir, "frame_%04d.jpg")
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-vf", f"fps=1/{FRAME_INTERVAL_SECONDS}",
        "-q:v", "2",          # high quality JPEG
        pattern,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr}")

    frames = sorted(Path(output_dir).glob("frame_*.jpg"))
    # frame_0001.jpg = 0s, frame_0002.jpg = 5s, etc.
    return [
        (str(f), (i) * FRAME_INTERVAL_SECONDS)
        for i, f in enumerate(frames)
    ]


def _count_faces_in_frame(image_path: str) -> int:
    """Call Rekognition detect_faces on a single JPEG frame."""
    rekognition = boto3.client(
        "rekognition",
        aws_access_key_id=settings.aws_access_key,
        aws_secret_access_key=settings.aws_secret_key,
        region_name=settings.aws_region,
    )
    with open(image_path, "rb") as f:
        image_bytes = f.read()

    response = rekognition.detect_faces(
        Image={"Bytes": image_bytes},
        Attributes=["DEFAULT"],
    )
    # Only count faces above confidence threshold
    faces = [
        face for face in response.get("FaceDetails", [])
        if face.get("Confidence", 0) >= MIN_FACE_CONFIDENCE
    ]
    return len(faces)


def analyze_recording(s3_key: str, expected_participant_count: int) -> ComplianceResult:
    """
    Full synchronous pipeline — intended to run inside a Celery task.
    Downloads video, samples frames, checks face counts.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        video_path  = os.path.join(tmpdir, "recording" + Path(s3_key).suffix)
        frames_dir  = os.path.join(tmpdir, "frames")
        os.makedirs(frames_dir)

        try:
            _download_from_s3(s3_key, video_path)
        except Exception as e:
            return ComplianceResult(passed=False, frames_analyzed=0, max_faces=0, error=f"S3 download failed: {e}")

        try:
            frames = _extract_frames(video_path, frames_dir)
        except Exception as e:
            return ComplianceResult(passed=False, frames_analyzed=0, max_faces=0, error=f"Frame extraction failed: {e}")

        if not frames:
            return ComplianceResult(passed=True, frames_analyzed=0, max_faces=0, error="No frames extracted — video may be empty")

        violations: list[FrameViolation] = []
        max_faces = 0

        for frame_path, ts in frames:
            try:
                count = _count_faces_in_frame(frame_path)
            except Exception as e:
                logger.warning(f"Rekognition failed on frame at {ts}s: {e}")
                continue

            max_faces = max(max_faces, count)

            if count > expected_participant_count:
                violations.append(FrameViolation(
                    timestamp_sec=ts,
                    frame_path=frame_path,
                    faces_detected=count,
                    expected=expected_participant_count,
                ))
                logger.warning(
                    f"HIPAA violation at {ts}s: {count} faces detected, expected {expected_participant_count}"
                )

        passed = len(violations) == 0
        logger.info(
            f"Compliance analysis done — frames={len(frames)} max_faces={max_faces} "
            f"violations={len(violations)} passed={passed}"
        )
        return ComplianceResult(
            passed=passed,
            frames_analyzed=len(frames),
            max_faces=max_faces,
            violations=violations,
        )
