# Video Extraction Workflow

Download YouTube basketball clips with timestamps. Automatically organized by date.

## Quick Usage

### Single Clip

```bash
cd /Users/omaribrahim/dev/scripts/automation-scripts/video-extraction

./generate-clips.py \
  --url "https://www.youtube.com/live/VIDEO_ID" \
  --start 1234 \
  --end 5678
```

Clip saves to: `/Users/omaribrahim/data/hoop-highlights/YYYY-MM-DD/clips/`

### Batch Multiple Clips

Create `clips.json`:
```json
{
  "clips": [
    {"url": "https://www.youtube.com/live/ID1", "start": 100, "end": 200},
    {"url": "https://www.youtube.com/live/ID2", "start": 300, "end": 400}
  ]
}
```

Then:
```bash
./generate-clips.py --json clips.json
```

---

## Details

### Script: generate-clips.py

**Purpose**: Download specific time-range segments from YouTube videos

**Features**:
- Single clip or batch mode
- Auto-generates date folders
- Organized output location
- Easy to extend with custom dates

**Parameters**:
- `--url` - YouTube video URL
- `--start` - Start time in seconds
- `--end` - End time in seconds
- `--date` - Optional date (YYYY-MM-DD, default: today)
- `--json` - JSON file with multiple clips

**Output**:
```
/Users/omaribrahim/data/hoop-highlights/
├── 2025-11-08/
│   └── clips/
│       ├── video1_100-200.mkv
│       └── video2_300-400.mkv
├── 2025-11-09/
│   └── clips/
│       └── another_clip.mkv
```

### How to Find Timestamps

1. Open YouTube video
2. Click to the desired start time
3. Note the timestamp shown
4. Calculate: (minute × 60) + seconds = start_time

Example: 2:15 = (2 × 60) + 15 = 135 seconds

---

## Conversion to MP4

After downloading (MKV format), convert to MP4:

```bash
/Users/omaribrahim/dev/scripts/automation-scripts/video-conversion/convert-mkv-to-mp4.sh \
  /Users/omaribrahim/data/hoop-highlights/2025-11-08/clips
```

Converts all MKV files in `clips/` folder to `converted/` folder.

---

## Monitoring

Check download progress:
```bash
./monitor.sh
```

---

## Legacy: clips.sh

Old script with hardcoded clips. Still available but use `generate-clips.py` for new downloads.

---

## Dependencies

- yt-dlp: `brew install yt-dlp` or `pip install yt-dlp`
- Python 3.7+
- ffmpeg (included with yt-dlp)

## Troubleshooting

**yt-dlp not found**
```bash
brew install yt-dlp
```

**URL not working**
- Verify video is public/accessible
- Try without timestamp parameters
- Update: `brew upgrade yt-dlp`

**Permission denied**
```bash
chmod +x generate-clips.py
```
