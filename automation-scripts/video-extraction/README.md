# Video Extraction Workflow

Automated extraction of specific time-range clips from YouTube live events and videos.

## Overview

This workflow downloads specific segments from YouTube videos using timestamps. Instead of manually downloading entire videos, you provide:
1. YouTube video URL
2. Start and end timestamps
3. Desired output filename

The script then extracts only the requested segment.

## Usage

### Quick Start: Run Default Clips

```bash
./clips.sh
```

This downloads all 12 pre-configured clips defined in `clips.sh`.

### Programmatic Usage: Add New Clips

Edit `clips.sh` and add a new entry following this pattern:

```bash
# Your clip description (optional comment)
yt-dlp "https://www.youtube.com/live/VIDEO_ID?si=TIMESTAMP&t=START_TIME" \
  --download-sections "*START_TIME-END_TIME" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_START_TIME-END_TIME.%(ext)s"
```

### Parameters Explained

- `--download-sections "*START_TIME-END_TIME"`: Download only this time range (in seconds)
- `--force-keyframes-at-cuts`: Create keyframes at exact cut points for cleaner extraction
- `-S proto:https`: Prefer HTTPS protocol for downloading
- `-o "%(title)s_START_TIME-END_TIME.%(ext)s"`: Output filename with video title and timestamp range

## Input Format: Timestamps JSON (Future Enhancement)

For programmatic clip extraction, you can provide timestamps in JSON format:

```json
{
  "clips": [
    {
      "url": "https://www.youtube.com/live/Qh6YznOLIzc?si=UHTYd2uHicKVowcO&t=8617",
      "start": 8617,
      "end": 8657,
      "description": "Important segment from live event"
    },
    {
      "url": "https://www.youtube.com/live/Z6t-lyhUxYU?si=sqA7opmuneq8NH1O&t=643",
      "start": 643,
      "end": 683,
      "description": "Follow-up discussion"
    }
  ]
}
```

## Current Clips (clips.sh)

| # | Video ID | Duration | Time Range |
|---|----------|----------|-----------|
| 1 | Qh6YznOLIzc | 40s | 8617-8657 |
| 2 | Z6t-lyhUxYU | 40s | 643-683 |
| 3 | Qh6YznOLIzc | 40s | 8582-8622 |
| 4 | Qh6YznOLIzc | 40s | 4081-4121 |
| 5 | T4wcS286Bjk | 40s | 6209-6249 |
| 6 | T4wcS286Bjk | 40s | 5123-5163 |
| 7 | T4wcS286Bjk | 40s | 4978-5018 |
| 8 | T4wcS286Bjk | 40s | 4936-4976 |
| 9 | T4wcS286Bjk | 40s | 2596-2636 |
| 10 | beYpY_ME7AY | 40s | 5972-6012 |
| 11 | Z6t-lyhUxYU | 40s | 660-700 |
| 12 | xW6q6Jcvgaw | 40s | 3383-3423 |

All clips are 40 seconds long.

## Output

Downloaded video clips are saved to the `output/` directory with filenames in the format:
```
{Video Title}_{START_TIME}-{END_TIME}.{extension}
```

Example:
- `Example Live Stream_8617-8657.mp4`
- `Another Stream_643-683.mkv`

## Dependencies

- **yt-dlp**: Command-line tool for downloading videos
  - Install via pip: `pip install yt-dlp`
  - Install via Homebrew: `brew install yt-dlp`
  - Requires Python 3.7+

## Execution Notes

- Each clip typically takes 2-5 minutes to download depending on internet speed
- Total runtime for 12 clips: ~30-60 minutes
- Videos must be publicly available
- YouTube terms of service should be reviewed before mass downloading

## Error Handling

If a clip fails to download:
1. Check that the YouTube URL is still valid and public
2. Verify the timestamps fall within the video duration
3. Ensure yt-dlp is up to date: `yt-dlp --update-to nightly`
4. Check internet connectivity
5. Try downloading that specific clip in isolation

## Future Enhancements

- [ ] Python script to generate clips.sh from JSON input
- [ ] Parallel downloading of multiple clips (faster execution)
- [ ] Post-processing: trimming silence, normalizing audio levels
- [ ] Automatic subtitle generation for clips
- [ ] Clip metadata organization and indexing

## Related Files

- `clips.sh` - Main executable script with hardcoded clips
- `output/` - Directory where downloaded clips are stored (git-ignored)
- `timestamps.json` - (To be created) Example JSON input format

## Troubleshooting

### yt-dlp not found
```bash
# Install via pip
pip install yt-dlp

# Or via Homebrew (macOS)
brew install yt-dlp
```

### Permission denied on clips.sh
```bash
chmod +x clips.sh
```

### Videos failing to download
Update yt-dlp to the latest version:
```bash
yt-dlp --update-to nightly
```

## Notes

- Downloaded files are not tracked by git (see .gitignore)
- Each clip extracts only the requested time range, reducing file sizes
- Keyframe forcing ensures clean cuts at exact timestamps
