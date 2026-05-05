"""
Video I/O utilities for basketball analysis.
"""

import cv2
import os
from pathlib import Path
from typing import Optional, Tuple


def get_video_info(video_path: str) -> dict:
    """Get basic video metadata."""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open video: {video_path}")

    info = {
        "path": video_path,
        "width": int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
        "height": int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        "fps": cap.get(cv2.CAP_PROP_FPS),
        "total_frames": int(cap.get(cv2.CAP_PROP_FRAME_COUNT)),
        "duration_sec": int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) / cap.get(cv2.CAP_PROP_FPS)
    }
    cap.release()
    return info


def format_timestamp(seconds: float) -> str:
    """Format seconds as HH:MM:SS.mmm"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}_{minutes:02d}_{secs:05.2f}".replace(".", "_")


def save_frame(
    frame,
    output_dir: str,
    timestamp_sec: float,
    label: str = "",
    quality: int = 95
) -> str:
    """
    Save a frame as JPEG.

    Returns the path to the saved file.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    timestamp_str = format_timestamp(timestamp_sec)
    label_str = f"_{label}" if label else ""
    filename = f"{timestamp_str}{label_str}.jpg"
    filepath = os.path.join(output_dir, filename)

    cv2.imwrite(filepath, frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    return filepath


def extract_frame_at(video_path: str, timestamp_sec: float):
    """Extract a single frame at a specific timestamp."""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_number = int(timestamp_sec * fps)

    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
    ret, frame = cap.read()
    cap.release()

    if not ret:
        raise ValueError(f"Could not read frame at {timestamp_sec}s")

    return frame


def extract_clip(
    video_path: str,
    output_path: str,
    start_sec: float,
    end_sec: float
) -> str:
    """
    Extract a clip from video using OpenCV.

    For better quality/speed, consider using ffmpeg directly.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    start_frame = int(start_sec * fps)
    end_frame = int(end_sec * fps)

    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    for _ in range(end_frame - start_frame):
        ret, frame = cap.read()
        if not ret:
            break
        out.write(frame)

    cap.release()
    out.release()

    return output_path
