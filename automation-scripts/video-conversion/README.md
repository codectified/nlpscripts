# Video Conversion Utilities

Convert videos between formats. Currently supports MKV → MP4 conversion.

## convert-mkv-to-mp4.sh

**Purpose**: Convert MKV files to MP4 format (compatible, lossless)

**Why**:
- MKV is container format used by yt-dlp
- MP4 is more universally compatible
- Uses stream copying (no re-encoding) → fast conversion

**Usage**:
```bash
./convert-mkv-to-mp4.sh /path/to/mkv/files
```

**Examples**:
```bash
# Convert basketball highlights
./convert-mkv-to-mp4.sh /Users/omaribrahim/data/hoop-highlights/2025-11-08/clips

# Convert podcast videos
./convert-mkv-to-mp4.sh /Users/omaribrahim/data/podcasts/incoming
```

**How it Works**:
1. Scans input directory for `.mkv` files
2. Creates `converted/` folder in parent directory
3. Converts each MKV → MP4 (stream copy, no re-encoding)
4. Skips files that already have MP4 versions
5. Reports success/failure for each file

**Output Structure**:
```
clips/          → input directory with *.mkv
converted/      → output directory with *.mp4 (auto-created)
```

**Performance**:
- Conversion speed: ~10-30 seconds per 40-second clip (stream copy)
- File size: Same as input (no quality loss)
- CPU usage: Minimal (no re-encoding)

**Error Handling**:
- Validates input directory exists
- Skips files that already converted
- Reports which files failed
- Continues on errors (doesn't stop on one failure)

**Requirements**:
- ffmpeg installed: `brew install ffmpeg`

## Future Conversion Tools

- [ ] MP4 → WebM
- [ ] Batch format detection and auto-conversion
- [ ] Video quality optimization
- [ ] Audio-only extraction (MP4 → MP3)
