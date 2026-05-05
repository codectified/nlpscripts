# Basketball Analysis Tools

Automated frame extraction and shot detection for pickup basketball footage.

## Status: Planning

See [PLANNING.md](./PLANNING.md) for full brainstorm and technical approach.

## Quick Start

*Coming soon - project in planning phase*

```bash
# Activate venv
source /Users/omaribrahim/dev/scripts/openaibatches/bin/activate

# Run frame extraction (future)
python extract_frames.py --input /path/to/video.mp4 --output ./stills/
```

## Goals

1. **Motion Detection**: Extract high-action frames automatically
2. **Made Shot Detection**: Identify when ball goes through the hoop
3. **CLI First**: Simple command-line tool before any UI

## Key Insight

Fixed camera + fixed rim = consistent visual signatures for made shots. We don't need to track every shot - just detect when a ball passes through the known rim location.

## Input

- Video files from `/Users/omaribrahim/data/hoop-highlights/`

## Output

- Still frames to `hoop-highlights/YYYY-MM-DD/stills/`
- Detection metadata (timestamps, confidence scores)
