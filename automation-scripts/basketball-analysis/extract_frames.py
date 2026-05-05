#!/usr/bin/env python3
"""
Basketball Frame Extractor

Extract high-action still frames from basketball footage.

Usage:
    python extract_frames.py --input video.mp4 --output ./stills/
    python extract_frames.py --input video.mp4 --analyze  # Find good threshold
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from detectors.motion import detect_motion_peaks, analyze_motion_distribution
from utils.video_io import get_video_info, save_frame, format_timestamp


def analyze_video(video_path: str, method: str = "diff", sample_rate: int = 5):
    """Analyze motion distribution to help set threshold."""
    print(f"\nAnalyzing motion distribution...")
    print(f"Video: {video_path}")

    info = get_video_info(video_path)
    print(f"Resolution: {info['width']}x{info['height']}")
    print(f"Duration: {info['duration_sec']:.1f}s ({info['total_frames']} frames @ {info['fps']:.1f} fps)")

    scores, mean, std = analyze_motion_distribution(video_path, method, sample_rate)

    print(f"\nMotion Analysis ({method} method):")
    print(f"  Samples: {len(scores)}")
    print(f"  Mean: {mean:.4f}")
    print(f"  Std:  {std:.4f}")
    print(f"  Min:  {min(scores):.4f}")
    print(f"  Max:  {max(scores):.4f}")

    # Suggest thresholds
    print(f"\nSuggested thresholds:")
    print(f"  High action (top 10%): {mean + 1.3 * std:.4f}")
    print(f"  Medium action (top 25%): {mean + 0.7 * std:.4f}")
    print(f"  Any motion (above avg): {mean:.4f}")

    return scores, mean, std


def extract_motion_frames(
    video_path: str,
    output_dir: str,
    method: str = "diff",
    threshold: float = None,
    min_gap: float = 2.0,
    sample_rate: int = 2,
    max_frames: int = 100
):
    """Extract frames with high motion."""
    print(f"\nExtracting motion frames...")

    info = get_video_info(video_path)
    print(f"Video: {info['width']}x{info['height']} @ {info['fps']:.1f} fps")
    print(f"Duration: {info['duration_sec']:.1f}s")

    # Auto-determine threshold if not provided
    if threshold is None:
        print("Determining threshold automatically...")
        _, mean, std = analyze_motion_distribution(video_path, method, sample_rate=10)
        threshold = mean + 0.7 * std  # Top ~25% of motion
        print(f"Using threshold: {threshold:.4f}")

    # Create output directory
    frames_dir = os.path.join(output_dir, "frames")
    Path(frames_dir).mkdir(parents=True, exist_ok=True)

    # Extract frames
    detections = []
    frame_count = 0

    print(f"\nScanning for motion (threshold={threshold:.4f}, gap={min_gap}s)...")

    for motion_frame in detect_motion_peaks(
        video_path,
        method=method,
        threshold=threshold,
        min_gap_sec=min_gap,
        sample_rate=sample_rate
    ):
        frame_count += 1
        if frame_count > max_frames:
            print(f"Reached max frames limit ({max_frames})")
            break

        # Save frame
        filepath = save_frame(
            motion_frame.frame,
            frames_dir,
            motion_frame.timestamp_sec,
            label="motion"
        )

        detection = {
            "frame_number": motion_frame.frame_number,
            "timestamp_sec": motion_frame.timestamp_sec,
            "timestamp_fmt": format_timestamp(motion_frame.timestamp_sec).replace("_", ":"),
            "motion_score": round(motion_frame.motion_score, 4),
            "file": os.path.basename(filepath)
        }
        detections.append(detection)

        print(f"  [{frame_count}] {detection['timestamp_fmt']} - score: {detection['motion_score']:.4f}")

    # Save metadata
    metadata = {
        "source_video": video_path,
        "extraction_time": datetime.now().isoformat(),
        "method": method,
        "threshold": threshold,
        "min_gap_sec": min_gap,
        "total_detections": len(detections),
        "detections": detections
    }

    metadata_path = os.path.join(output_dir, "detections.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nExtracted {len(detections)} frames to {frames_dir}")
    print(f"Metadata saved to {metadata_path}")

    return detections


def main():
    parser = argparse.ArgumentParser(
        description="Extract high-action frames from basketball footage"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to input video file"
    )
    parser.add_argument(
        "--output", "-o",
        default="./stills",
        help="Output directory for extracted frames (default: ./stills)"
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze video motion distribution (don't extract)"
    )
    parser.add_argument(
        "--method", "-m",
        choices=["diff", "flow"],
        default="diff",
        help="Motion detection method (default: diff)"
    )
    parser.add_argument(
        "--threshold", "-t",
        type=float,
        default=None,
        help="Motion threshold (auto-detected if not set)"
    )
    parser.add_argument(
        "--gap", "-g",
        type=float,
        default=2.0,
        help="Minimum seconds between detections (default: 2.0)"
    )
    parser.add_argument(
        "--sample-rate", "-s",
        type=int,
        default=2,
        help="Process every Nth frame (default: 2)"
    )
    parser.add_argument(
        "--max-frames",
        type=int,
        default=100,
        help="Maximum frames to extract (default: 100)"
    )

    args = parser.parse_args()

    # Validate input
    if not os.path.exists(args.input):
        print(f"Error: Video not found: {args.input}")
        sys.exit(1)

    if args.analyze:
        analyze_video(args.input, args.method)
    else:
        extract_motion_frames(
            video_path=args.input,
            output_dir=args.output,
            method=args.method,
            threshold=args.threshold,
            min_gap=args.gap,
            sample_rate=args.sample_rate,
            max_frames=args.max_frames
        )


if __name__ == "__main__":
    main()
