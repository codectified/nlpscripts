"""
Motion detection for basketball footage.

Detects high-activity frames using frame differencing and optical flow.
"""

import cv2
import numpy as np
from dataclasses import dataclass
from typing import Generator, Tuple, Optional


@dataclass
class MotionFrame:
    """Represents a frame with detected motion."""
    frame_number: int
    timestamp_sec: float
    motion_score: float
    frame: np.ndarray


def compute_frame_diff(frame1: np.ndarray, frame2: np.ndarray) -> float:
    """
    Compute motion score using frame differencing.

    Returns normalized score 0-1 where higher = more motion.
    """
    # Convert to grayscale if needed
    if len(frame1.shape) == 3:
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)
    else:
        gray1, gray2 = frame1, frame2

    # Apply Gaussian blur to reduce noise
    gray1 = cv2.GaussianBlur(gray1, (21, 21), 0)
    gray2 = cv2.GaussianBlur(gray2, (21, 21), 0)

    # Compute absolute difference
    diff = cv2.absdiff(gray1, gray2)

    # Threshold to get binary motion mask
    _, thresh = cv2.threshold(diff, 25, 255, cv2.THRESH_BINARY)

    # Calculate percentage of pixels with motion
    motion_score = np.sum(thresh > 0) / thresh.size

    return motion_score


def compute_optical_flow(prev_gray: np.ndarray, curr_gray: np.ndarray) -> Tuple[float, np.ndarray]:
    """
    Compute motion using dense optical flow (Farneback method).

    Returns:
        - motion_score: average flow magnitude (higher = more motion)
        - flow: the flow field for visualization
    """
    flow = cv2.calcOpticalFlowFarneback(
        prev_gray, curr_gray,
        None,
        pyr_scale=0.5,
        levels=3,
        winsize=15,
        iterations=3,
        poly_n=5,
        poly_sigma=1.2,
        flags=0
    )

    # Compute magnitude
    magnitude = np.sqrt(flow[..., 0]**2 + flow[..., 1]**2)

    # Average magnitude as motion score
    motion_score = np.mean(magnitude)

    return motion_score, flow


def detect_motion_peaks(
    video_path: str,
    method: str = "diff",
    threshold: float = 0.05,
    min_gap_sec: float = 1.0,
    sample_rate: int = 1
) -> Generator[MotionFrame, None, None]:
    """
    Detect frames with high motion in a video.

    Args:
        video_path: Path to video file
        method: "diff" for frame differencing, "flow" for optical flow
        threshold: Motion score threshold (0-1 for diff, 0-inf for flow)
        min_gap_sec: Minimum seconds between detected frames
        sample_rate: Process every Nth frame (1 = all frames)

    Yields:
        MotionFrame objects for frames exceeding threshold
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    min_gap_frames = int(min_gap_sec * fps)

    print(f"Video: {total_frames} frames @ {fps:.1f} fps")
    print(f"Method: {method}, threshold: {threshold}")

    prev_frame = None
    prev_gray = None
    last_detection_frame = -min_gap_frames
    frame_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_count += 1

        # Skip frames based on sample rate
        if frame_count % sample_rate != 0:
            continue

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        if prev_frame is not None:
            # Compute motion score
            if method == "diff":
                motion_score = compute_frame_diff(prev_frame, frame)
            else:  # optical flow
                motion_score, _ = compute_optical_flow(prev_gray, gray)

            # Check if exceeds threshold and respects gap
            if motion_score > threshold and (frame_count - last_detection_frame) >= min_gap_frames:
                timestamp = frame_count / fps
                yield MotionFrame(
                    frame_number=frame_count,
                    timestamp_sec=timestamp,
                    motion_score=motion_score,
                    frame=frame.copy()
                )
                last_detection_frame = frame_count

        prev_frame = frame
        prev_gray = gray

    cap.release()


def analyze_motion_distribution(
    video_path: str,
    method: str = "diff",
    sample_rate: int = 5
) -> Tuple[list, float, float]:
    """
    Analyze the distribution of motion scores across a video.

    Useful for determining appropriate threshold values.

    Returns:
        - scores: list of all motion scores
        - mean: average motion score
        - std: standard deviation
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open video: {video_path}")

    scores = []
    prev_frame = None
    prev_gray = None
    frame_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_count += 1
        if frame_count % sample_rate != 0:
            continue

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        if prev_frame is not None:
            if method == "diff":
                score = compute_frame_diff(prev_frame, frame)
            else:
                score, _ = compute_optical_flow(prev_gray, gray)
            scores.append(score)

        prev_frame = frame
        prev_gray = gray

    cap.release()

    scores_arr = np.array(scores)
    return scores, float(np.mean(scores_arr)), float(np.std(scores_arr))
