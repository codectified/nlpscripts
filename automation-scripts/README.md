# Automation Scripts Directory

A centralized repository for automation and batch processing scripts designed to make repetitive tasks easier and more efficient.

## Directory Structure

```
automation-scripts/
├── video-extraction/       # Scripts for downloading and processing video content
│   ├── clips.sh           # Main video clip extraction script
│   ├── output/            # Downloaded video files stored here
│   ├── README.md          # Detailed video extraction documentation
│   └── timestamps.json    # Input format for programmatic clip extraction
├── batch-processing/      # Batch processing utilities for large datasets
├── utilities/             # Helper scripts and common functions
├── README.md             # This file
└── AUTOMATION_LOG.md     # Central log of automation tasks and improvements
```

## Purpose

This directory serves as a central hub for:
- **Task Automation**: Scripts that automate repetitive tasks
- **Batch Processing**: Large-scale data processing operations
- **Workflow Documentation**: Clear instructions for reproducing automated workflows
- **Continuous Improvement**: Tracking which tasks can be automated next

## Key Principles

1. **Documentation First**: Every script is accompanied by clear documentation
2. **Input Standardization**: Consistent input formats (JSON, CSV) enable programmatic usage
3. **Auditability**: Comprehensive logging and execution records
4. **Reusability**: Scripts are designed to be parameterized and reusable
5. **Organization**: Clear directory structure by task type

## Quick Start

### Video Extraction
Download specific time-range clips from YouTube live events:

```bash
cd video-extraction
./clips.sh
```

See `video-extraction/README.md` for advanced usage and how to add new clips.

## Future Automation Candidates

- [ ] Automated video thumbnail extraction
- [ ] Batch subtitle processing
- [ ] Database maintenance tasks
- [ ] Log rotation and archival
- [ ] Report generation pipelines

## Contributing to This Directory

When adding new automation scripts:

1. Create a new subdirectory with a descriptive name
2. Add comprehensive README.md documentation
3. Include example input/output files
4. Document any dependencies (yt-dlp, ffmpeg, etc.)
5. Add entry to this main README
6. Update AUTOMATION_LOG.md with the new automation

## Environment & Dependencies

- **yt-dlp**: Required for video extraction scripts
  - Install: `pip install yt-dlp` or `brew install yt-dlp`
- **Python 3.7+**: For any Python-based automation scripts
- **Bash**: Standard shell scripting

## Notes

- All scripts should be idempotent where possible (safe to run multiple times)
- Output directories are git-ignored to avoid bloating the repository
- Logging should be comprehensive for debugging and auditing purposes
