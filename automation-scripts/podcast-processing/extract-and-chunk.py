#!/usr/bin/env python3

"""
Extract audio from podcast videos and split into 90-minute chunks.

Usage:
    ./extract-and-chunk.py --input VIDEO_FILE --name PODCAST_NAME [--silence-detect]

Examples:
    # Extract and chunk without silence detection (simple split at 90 min)
    ./extract-and-chunk.py \
        --input /Users/omaribrahim/data/podcasts/incoming/podcast.mp4 \
        --name "my-podcast"

    # With silence detection (finds natural break points near 90 min)
    ./extract-and-chunk.py \
        --input /Users/omaribrahim/data/podcasts/incoming/podcast.mp4 \
        --name "my-podcast" \
        --silence-detect
"""

import argparse
import subprocess
import os
import sys
from datetime import datetime, timedelta
import math

PODCAST_DIR = "/Users/omaribrahim/data/podcasts"
FULL_LENGTH_DIR = os.path.join(PODCAST_DIR, "audio-extracted", "full-length")
CHUNKS_DIR = os.path.join(PODCAST_DIR, "audio-extracted", "90min-chunks")
METADATA_DIR = os.path.join(PODCAST_DIR, "metadata")

# Create directories
os.makedirs(FULL_LENGTH_DIR, exist_ok=True)
os.makedirs(CHUNKS_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)

def get_duration(file_path):
    """Get duration of audio/video file in seconds."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path
            ],
            capture_output=True,
            text=True,
            timeout=60
        )
        return float(result.stdout.strip())
    except Exception as e:
        print(f"Error getting duration: {e}")
        return None

def format_time(seconds):
    """Format seconds as HH:MM:SS."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def extract_audio(input_file, output_name):
    """Extract audio from video file to MP3."""
    output_file = os.path.join(FULL_LENGTH_DIR, f"{output_name}.mp3")

    if os.path.exists(output_file):
        print(f"✓ Audio already extracted: {output_file}")
        return output_file

    print(f"\nExtracting audio from: {os.path.basename(input_file)}")
    print(f"Output: {output_file}")

    cmd = [
        "ffmpeg",
        "-i", input_file,
        "-q:a", "0",
        "-map", "a",
        "-y",  # Overwrite output file
        output_file
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )

        if result.returncode == 0:
            file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
            duration = get_duration(output_file)
            print(f"✓ Extracted successfully ({file_size:.1f} MB, {format_time(duration)})")
            return output_file
        else:
            print(f"✗ Extraction failed: {result.stderr}")
            return None
    except subprocess.TimeoutExpired:
        print(f"✗ Extraction timeout (exceeded 1 hour)")
        return None
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def find_silence_points(audio_file, noise_threshold=-40, silence_duration=1):
    """Find silence points in audio file."""
    print(f"\nDetecting silence points...")
    print(f"(Looking for {silence_duration}+ second(s) of silence at {noise_threshold}dB)")

    cmd = [
        "ffmpeg",
        "-i", audio_file,
        "-af", f"silencedetect=n={noise_threshold}dB:d={silence_duration}",
        "-f", "null",
        "-"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            stderr=subprocess.STDOUT,
            timeout=600
        )

        silence_points = []
        for line in result.stdout.split('\n'):
            if 'silence_start' in line or 'silence_end' in line:
                # Extract timestamp
                parts = line.split()
                for i, part in enumerate(parts):
                    if part.startswith('silence_') and i + 1 < len(parts):
                        try:
                            timestamp = float(parts[i + 1])
                            silence_points.append(timestamp)
                        except:
                            pass

        if silence_points:
            print(f"Found {len(silence_points)} silence points")
            return silence_points
        else:
            print("No silence points found")
            return []
    except Exception as e:
        print(f"Warning: Silence detection failed: {e}")
        return []

def split_into_chunks(audio_file, output_name, use_silence=False):
    """Split audio into 90-minute chunks."""
    duration = get_duration(audio_file)
    if duration is None:
        return False

    chunk_duration = 90 * 60  # 90 minutes in seconds
    num_chunks = math.ceil(duration / chunk_duration)

    print(f"\nSplitting audio into {num_chunks} chunk(s)")
    print(f"Total duration: {format_time(duration)}")

    silence_points = []
    if use_silence:
        silence_points = find_silence_points(audio_file)

    successful = 0

    for chunk_num in range(1, num_chunks + 1):
        start_time = (chunk_num - 1) * chunk_duration
        end_time = min(chunk_num * chunk_duration, duration)

        # Adjust end time to silence point if available
        if use_silence and silence_points:
            # Find nearest silence point within ±30 seconds of chunk boundary
            target = end_time
            best_silence = None
            for sp in silence_points:
                if target - 30 <= sp <= target + 30:
                    if best_silence is None or abs(sp - target) < abs(best_silence - target):
                        best_silence = sp
            if best_silence:
                end_time = best_silence
                print(f"  Adjusted chunk {chunk_num} end to silence point at {format_time(best_silence)}")

        output_file = os.path.join(CHUNKS_DIR, f"{output_name}_90min_part{chunk_num}.mp3")

        if os.path.exists(output_file):
            print(f"  [{chunk_num}/{num_chunks}] Already exists: {os.path.basename(output_file)}")
            successful += 1
            continue

        print(f"  [{chunk_num}/{num_chunks}] Splitting: {format_time(start_time)} → {format_time(end_time)}")

        cmd = [
            "ffmpeg",
            "-i", audio_file,
            "-ss", str(int(start_time)),
            "-to", str(int(end_time)),
            "-c", "copy",
            "-y",
            output_file
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600
            )

            if result.returncode == 0:
                file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
                chunk_duration_actual = get_duration(output_file)
                print(f"    ✓ Created ({file_size:.1f} MB, {format_time(chunk_duration_actual)})")
                successful += 1
            else:
                print(f"    ✗ Failed: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            print(f"    ✗ Timeout")
        except Exception as e:
            print(f"    ✗ Error: {e}")

    return successful == num_chunks

def save_metadata(input_file, output_name, duration, num_chunks):
    """Save metadata about podcast processing."""
    metadata_file = os.path.join(METADATA_DIR, f"{output_name}_manifest.txt")

    file_size = os.path.getsize(input_file) / (1024 * 1024 * 1024)  # GB
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    metadata = f"""Podcast Processing Manifest
=====================================
Name: {output_name}
Date Processed: {date_str}

Source File:
  Input: {os.path.basename(input_file)}
  Size: {file_size:.2f} GB
  Duration: {format_time(duration)}

Output Files:
  Full-length MP3: {FULL_LENGTH_DIR}/{output_name}.mp3
  90-minute chunks: {num_chunks} files in {CHUNKS_DIR}/

Chunk Details:
  Each chunk: ~90 minutes (except last, may be shorter)
  Format: MP3 (stream copied, no re-encoding)

Files:
"""

    for i in range(1, num_chunks + 1):
        chunk_file = f"{output_name}_90min_part{i}.mp3"
        metadata += f"  {i}. {chunk_file}\n"

    with open(metadata_file, 'w') as f:
        f.write(metadata)

    print(f"\n✓ Metadata saved: {metadata_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Extract and chunk podcast audio",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument("--input", required=True, help="Input video/audio file path")
    parser.add_argument("--name", required=True, help="Podcast name (for output files)")
    parser.add_argument("--silence-detect", action="store_true", help="Use silence detection for smart chunk boundaries")

    args = parser.parse_args()

    # Validate input file
    if not os.path.exists(args.input):
        print(f"Error: File not found: {args.input}")
        sys.exit(1)

    print("="*60)
    print("Podcast Audio Extraction & Chunking")
    print("="*60)

    # Step 1: Extract audio
    audio_file = extract_audio(args.input, args.name)
    if not audio_file:
        print("\nError: Failed to extract audio")
        sys.exit(1)

    # Get duration for metadata
    duration = get_duration(audio_file)

    # Step 2: Split into chunks
    num_chunks = math.ceil(duration / (90 * 60))
    success = split_into_chunks(audio_file, args.name, use_silence=args.silence_detect)

    # Step 3: Save metadata
    save_metadata(args.input, args.name, duration, num_chunks)

    print("\n" + "="*60)
    if success:
        print("✓ Processing complete!")
        print(f"  Chunks saved to: {CHUNKS_DIR}/")
    else:
        print("⚠ Processing completed with some errors")
    print("="*60)

if __name__ == "__main__":
    main()
