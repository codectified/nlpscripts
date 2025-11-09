#!/usr/bin/env python3

"""
Generate basketball highlight clips from YouTube URLs and timestamps.

Usage:
    ./generate-clips.py --url VIDEO_URL --start START_TIME --end END_TIME [--date DATE]

    Or provide multiple clips in JSON format:
    ./generate-clips.py --json clips.json

Examples:
    # Single clip
    ./generate-clips.py \\
        --url "https://www.youtube.com/live/Qh6YznOLIzc" \\
        --start 8617 \\
        --end 8657

    # Batch from JSON
    ./generate-clips.py --json clips.json
"""

import argparse
import json
import subprocess
import os
from datetime import datetime
import sys

# Default output location
DATA_DIR = "/Users/omaribrahim/data/hoop-highlights"

def ensure_date_folder(date_str=None):
    """Create and return path to date-based folder."""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    date_path = os.path.join(DATA_DIR, date_str, "clips")
    os.makedirs(date_path, exist_ok=True)
    return date_path, date_str

def download_clip(url, start_time, end_time, output_dir, date_str):
    """Download a single clip using yt-dlp."""
    cmd = [
        "yt-dlp",
        url,
        "--download-sections", f"*{start_time}-{end_time}",
        "-o", f"%(title)s_{start_time}-{end_time}.%(ext)s"
    ]

    print(f"Downloading: {url} ({start_time}-{end_time} seconds)")
    print(f"Output directory: {output_dir}")

    try:
        result = subprocess.run(
            cmd,
            cwd=output_dir,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )

        if result.returncode == 0:
            print(f"✓ Downloaded successfully")
            return True
        else:
            print(f"✗ Download failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"✗ Download timeout (exceeded 1 hour)")
        return False
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False

def process_single_clip(args):
    """Process a single clip from command line arguments."""
    if not args.url or args.start is None or args.end is None:
        print("Error: --url, --start, and --end are required")
        sys.exit(1)

    output_dir, date_str = ensure_date_folder(args.date)
    success = download_clip(args.url, args.start, args.end, output_dir, date_str)

    if success:
        print(f"\nClip saved to: {output_dir}")

    return success

def process_json_clips(json_file):
    """Process multiple clips from JSON file."""
    if not os.path.exists(json_file):
        print(f"Error: File not found: {json_file}")
        sys.exit(1)

    with open(json_file, 'r') as f:
        data = json.load(f)

    clips = data.get("clips", [])
    if not clips:
        print("No clips found in JSON file")
        sys.exit(1)

    print(f"Processing {len(clips)} clips from {json_file}\n")

    successful = 0
    failed = 0

    for i, clip in enumerate(clips, 1):
        print(f"\n[{i}/{len(clips)}] ----")

        url = clip.get("url")
        start = clip.get("start")
        end = clip.get("end")
        date_str = clip.get("date")  # Optional: override date

        if not url or start is None or end is None:
            print(f"✗ Skipped: Missing url, start, or end")
            failed += 1
            continue

        output_dir, actual_date = ensure_date_folder(date_str)

        if download_clip(url, start, end, output_dir, actual_date):
            successful += 1
        else:
            failed += 1

    print(f"\n{'='*50}")
    print(f"Batch processing complete:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(clips)}")
    print(f"{'='*50}")

    return failed == 0

def main():
    parser = argparse.ArgumentParser(
        description="Download YouTube basketball highlight clips",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Single clip mode
    parser.add_argument("--url", help="YouTube video URL")
    parser.add_argument("--start", type=int, help="Start timestamp in seconds")
    parser.add_argument("--end", type=int, help="End timestamp in seconds")
    parser.add_argument("--date", help="Date folder (YYYY-MM-DD, default: today)")

    # Batch mode
    parser.add_argument("--json", help="JSON file with multiple clips")

    args = parser.parse_args()

    # Check that either single clip or batch mode is provided
    if args.json:
        success = process_json_clips(args.json)
    elif args.url is not None:
        success = process_single_clip(args)
    else:
        parser.print_help()
        print("\nError: Provide either --url (single clip) or --json (batch)")
        sys.exit(1)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
