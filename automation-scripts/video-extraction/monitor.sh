#!/usr/bin/env bash

# Monitor YouTube clip extraction progress

OUTPUT_DIR="$(cd "$(dirname "$0")" && pwd)/output"

echo "=== YouTube Clip Extraction Monitor ==="
echo "Output directory: $OUTPUT_DIR"
echo ""

# Count downloaded files (exclude .part files)
DOWNLOADED=$(ls -1 "$OUTPUT_DIR"/*.{mkv,mp4,webm} 2>/dev/null | grep -v "\.part$" | wc -l)
DOWNLOADING=$(ls -1 "$OUTPUT_DIR"/*.part 2>/dev/null | wc -l)
echo "Completed clips: $DOWNLOADED/12"
echo "Currently downloading: $DOWNLOADING clip(s)"
echo ""

# List files in directory
if [ -d "$OUTPUT_DIR" ]; then
  echo "Files in output directory:"
  ls -lh "$OUTPUT_DIR" | grep -v "^total" | grep -v "^d"
else
  echo "Output directory does not exist yet"
fi

# Check if process is still running
if pgrep -f "clips.sh" > /dev/null; then
  echo ""
  echo "Status: DOWNLOADING (script still running)"
else
  echo ""
  echo "Status: COMPLETE or STOPPED"
fi
