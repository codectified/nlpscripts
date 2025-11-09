#!/usr/bin/env bash

# Convert MKV files to MP4
# Usage: ./convert-mkv-to-mp4.sh /path/to/mkv/files

set -e

# Check if input directory is provided
if [ $# -eq 0 ]; then
  echo "Usage: $0 /path/to/mkv/directory"
  echo ""
  echo "Examples:"
  echo "  $0 /Users/omaribrahim/data/hoop-highlights/2025-11-08/clips"
  echo "  $0 /path/to/podcast/videos"
  exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="${INPUT_DIR%/*}/converted"  # Create 'converted' folder in parent directory

# Validate input directory
if [ ! -d "$INPUT_DIR" ]; then
  echo "Error: Directory not found: $INPUT_DIR"
  exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo "MKV to MP4 Converter"
echo "=========================================="
echo "Input directory:  $INPUT_DIR"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Count MKV files
MKV_COUNT=$(find "$INPUT_DIR" -maxdepth 1 -name "*.mkv" | wc -l)
if [ "$MKV_COUNT" -eq 0 ]; then
  echo "No MKV files found in: $INPUT_DIR"
  exit 1
fi

echo "Found $MKV_COUNT MKV file(s) to convert"
echo ""

CONVERTED=0
FAILED=0

# Convert each MKV file
for mkv_file in "$INPUT_DIR"/*.mkv; do
  # Get filename without extension
  filename=$(basename "$mkv_file" .mkv)
  output_file="$OUTPUT_DIR/${filename}.mp4"

  echo "Converting: $(basename "$mkv_file")"
  echo "  → $output_file"

  # Check if output already exists
  if [ -f "$output_file" ]; then
    echo "  ⚠️  Output already exists, skipping..."
    continue
  fi

  # Convert using ffmpeg (copy streams for speed, no re-encoding)
  if ffmpeg -i "$mkv_file" -c copy -y "$output_file" 2>&1 | grep -v "^\["; then
    echo "  ✓ Conversion successful"
    CONVERTED=$((CONVERTED + 1))
  else
    echo "  ✗ Conversion failed"
    FAILED=$((FAILED + 1))
  fi

  echo ""
done

echo "=========================================="
echo "Conversion Complete"
echo "=========================================="
echo "Successfully converted: $CONVERTED"
if [ "$FAILED" -gt 0 ]; then
  echo "Failed conversions: $FAILED"
fi
echo ""
echo "Files saved to: $OUTPUT_DIR"
