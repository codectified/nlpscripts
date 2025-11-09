#!/usr/bin/env bash
set -e

echo "Starting YouTube clip extraction..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/output"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

# 1
echo "Downloading clip 1..."
yt-dlp "https://www.youtube.com/live/Qh6YznOLIzc" \
  --download-sections "*8617-8657" \
  -o "%(title)s_8617-8657.%(ext)s"

# 2
echo "Downloading clip 2..."
yt-dlp "https://www.youtube.com/live/Z6t-lyhUxYU" \
  --download-sections "*643-683" \
  -o "%(title)s_643-683.%(ext)s"

# 3
echo "Downloading clip 3..."
yt-dlp "https://www.youtube.com/live/Qh6YznOLIzc" \
  --download-sections "*8582-8622" \
  -o "%(title)s_8582-8622.%(ext)s"

# 4
echo "Downloading clip 4..."
yt-dlp "https://www.youtube.com/live/Qh6YznOLIzc" \
  --download-sections "*4081-4121" \
  -o "%(title)s_4081-4121.%(ext)s"

# 5
echo "Downloading clip 5..."
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk" \
  --download-sections "*6209-6249" \
  -o "%(title)s_6209-6249.%(ext)s"

# 6
echo "Downloading clip 6..."
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk" \
  --download-sections "*5123-5163" \
  -o "%(title)s_5123-5163.%(ext)s"

# 7
echo "Downloading clip 7..."
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk" \
  --download-sections "*4978-5018" \
  -o "%(title)s_4978-5018.%(ext)s"

# 8
echo "Downloading clip 8..."
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk" \
  --download-sections "*4936-4976" \
  -o "%(title)s_4936-4976.%(ext)s"

# 9
echo "Downloading clip 9..."
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk" \
  --download-sections "*2596-2636" \
  -o "%(title)s_2596-2636.%(ext)s"

# 10
echo "Downloading clip 10..."
yt-dlp "https://www.youtube.com/live/beYpY_ME7AY" \
  --download-sections "*5972-6012" \
  -o "%(title)s_5972-6012.%(ext)s"

# 11
echo "Downloading clip 11..."
yt-dlp "https://www.youtube.com/live/Z6t-lyhUxYU" \
  --download-sections "*660-700" \
  -o "%(title)s_660-700.%(ext)s"

# 12
echo "Downloading clip 12..."
yt-dlp "https://www.youtube.com/watch?v=xW6q6Jcvgaw" \
  --download-sections "*3383-3423" \
  -o "%(title)s_3383-3423.%(ext)s"

echo "All clips downloaded successfully!"