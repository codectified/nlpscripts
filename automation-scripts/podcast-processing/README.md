# Podcast Processing - Extract & Chunk

Extract audio from podcast videos and automatically split into 90-minute chunks.

## Quick Usage

```bash
cd /Users/omaribrahim/dev/scripts/automation-scripts/podcast-processing

./extract-and-chunk.py \
  --input /Users/omaribrahim/data/podcasts/incoming/podcast-name.mp4 \
  --name "podcast-name-2025-10-18"
```

Output automatically organized:
- **Full-length MP3**: `/Users/omaribrahim/data/podcasts/audio-extracted/full-length/`
- **90-min chunks**: `/Users/omaribrahim/data/podcasts/audio-extracted/90min-chunks/`
- **Metadata**: `/Users/omaribrahim/data/podcasts/metadata/`

---

## Features

- ✓ Extracts audio from video files to MP3
- ✓ Auto-splits into 90-minute chunks
- ✓ Stream copies (no re-encoding, fast)
- ✓ Creates metadata manifest for each podcast
- ✓ Handles multiple files seamlessly
- ✓ Smart silence detection (optional)

## Script: extract-and-chunk.py

**Purpose**: Extract podcast audio and split into manageable chunks

**Parameters**:
- `--input` (required) - Path to video/audio file
- `--name` (required) - Podcast name for output files
- `--silence-detect` (optional) - Use silence detection for smart chunk boundaries

**Output Structure**:
```
/Users/omaribrahim/data/podcasts/
├── audio-extracted/
│   ├── full-length/
│   │   └── podcast-name-2025-10-18.mp3
│   └── 90min-chunks/
│       ├── podcast-name-2025-10-18_90min_part1.mp3
│       ├── podcast-name-2025-10-18_90min_part2.mp3
│       └── podcast-name-2025-10-18_90min_part3.mp3
└── metadata/
    └── podcast-name-2025-10-18_manifest.txt
```

---

## Naming Convention

Use format: `podcast-name-YYYY-MM-DD`

Examples:
- `gmt-meeting-2025-10-18`
- `conference-2025-11-11`
- `lecture-series-2025-09-15`

---

## Processing Details

### Extraction
- Input: MP4, MOV, M4A, or any video/audio format
- Output: MP3 (best quality, stream copied)
- Speed: Depends on file size (not re-encoded)

### Chunking
- **Simple mode** (default): Hard cuts every 90 minutes
- **Smart mode** (`--silence-detect`): Finds silence near 90-min boundary

### Metadata
Each podcast gets a manifest file with:
- Original file info (size, duration)
- List of all chunks created
- Processing timestamp

---

## Examples

### Extract a Google Meet recording
```bash
./extract-and-chunk.py \
  --input "/Users/omaribrahim/data/podcasts/incoming/GMT20251018-151052_Recording.m4a" \
  --name "gmt-meeting-2025-10-18"
```

### Extract with smart silence detection
```bash
./extract-and-chunk.py \
  --input "/Users/omaribrahim/data/podcasts/incoming/podcast.mp4" \
  --name "my-podcast-2025-11-11" \
  --silence-detect
```

### Extract from MOV file
```bash
./extract-and-chunk.py \
  --input "/Users/omaribrahim/Downloads/video.mov" \
  --name "video-2025-11-11"
```

---

## Silence Detection

Enable with `--silence-detect` flag.

**What it does**:
1. Analyzes audio for silence points
2. Finds natural breaks near 90-minute mark
3. Adjusts chunk boundaries ±30 seconds to silence

**Best for**:
- Podcasts with clear silence between segments
- Meetings with natural breaks
- Professional recordings with intro/outro silence

**Trade-off**:
- Adds ~2-3 minutes to processing time
- Results in more natural chunk breaks
- Not recommended for heavily compressed audio

---

## File Size Notes

Chunk file sizes vary based on:
- Audio compression (MP3 bitrate)
- Original audio quality
- Duration of chunk

**Example** (from gmt-meeting-2025-10-18):
- Part 1: 68 MB (90 minutes)
- Part 2: 27 MB (37 minutes)
- Total: 95 MB for 2:07 hours

---

## Processing Status

Check if process is still running:
```bash
ps aux | grep extract-and-chunk
```

Monitor progress:
```bash
tail -f /path/to/extraction.log
```

---

## Current Processed Podcasts

✓ **gmt-meeting-2025-10-18**
- Source: GMT20251018-151052_Recording.m4a (116 MB)
- Duration: 2 hours 7 minutes
- Chunks: 2 files (90 min + 37 min)
- Full-length: 95 MB
- Total chunks: 95 MB
- Metadata: gmt-meeting-2025-10-18_manifest.txt

---

## Next: qdi-rytx-vix Podcast Series

7 episodes ready to process:
- 2025-09-13 (321 MB)
- 2025-09-20 (485 MB)
- 2025-09-27 (521 MB)
- 2025-10-04 (690 MB)
- 2025-10-11 (424 MB)
- 2025-10-26 (455 MB)
- 2025-11-08 (660 MB)

Run same script for each episode.

---

## Troubleshooting

**ffmpeg not found**
```bash
brew install ffmpeg
```

**ffprobe not found**
```bash
brew install ffmpeg  # includes ffprobe
```

**Permission denied**
```bash
chmod +x extract-and-chunk.py
```

**File already extracted**
- Script skips if full-length MP3 already exists
- Chunks are always re-created (overwrites existing)

**Slow processing**
- Normal for large files (2+ hours)
- Uses stream copy (fast compared to re-encoding)
- Silence detection adds 2-3 minutes
