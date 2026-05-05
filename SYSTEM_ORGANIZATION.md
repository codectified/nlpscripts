# System-Wide Organization Guide

**Created**: 2025-11-08
**Status**: Complete and documented

This document explains your entire system organization across data and automation.

---

## Two Repository System

### 1. Scripts Repository
**Location**: `/Users/omaribrahim/dev/scripts/`
**Purpose**: Automation scripts and tools
**Tracked**: Git (all code and documentation)

```
/Users/omaribrahim/dev/scripts/
├── automation-scripts/          ← All automation tools
│   ├── video-extraction/        (download basketball clips)
│   ├── video-conversion/        (MKV → MP4 conversion)
│   ├── podcast-processing/      (audio extraction & chunking)
│   ├── basketball-analysis/     (frame extraction & shot detection)
│   └── utilities/
├── mindroots/                   (NLP/corpus work)
├── nlp/
├── docs/
└── SYSTEM_ORGANIZATION.md       (this file)
```

**Git repo**: Yes - `/Users/omaribrahim/dev/scripts/.git`

### 2. Data Repository
**Location**: `/Users/omaribrahim/data/`
**Purpose**: Organized media and content
**Tracked**: Git (documentation only, not media files)

```
/Users/omaribrahim/data/
├── hoop-highlights/             ← Basketball clips (organized by date)
│   ├── 2025-11-08/
│   │   ├── clips/              (MKV original files)
│   │   └── converted/          (MP4 versions)
│   └── archive/
├── podcasts/                   ← Podcast processing
│   ├── incoming/               (raw podcast videos)
│   ├── processing/             (actively being worked on)
│   ├── audio-extracted/
│   │   ├── full-length/        (complete MP3)
│   │   └── 90min-chunks/       (split segments)
│   └── metadata/               (podcast info + manifest)
├── ORGANIZATION.md             (master org document)
├── QUICK_REFERENCE.md          (common commands)
└── .gitignore                  (excludes media files)
```

**Git repo**: Yes - `/Users/omaribrahim/data/.git`

---

## Key Files to Remember

### Master Documentation
- `/Users/omaribrahim/data/ORGANIZATION.md` - Complete data structure
- `/Users/omaribrahim/data/QUICK_REFERENCE.md` - Common tasks
- `/Users/omaribrahim/dev/scripts/SYSTEM_ORGANIZATION.md` - This file

### Automation Scripts
- **Video extraction**: `/Users/omaribrahim/dev/scripts/automation-scripts/video-extraction/clips.sh`
- **Format conversion**: `/Users/omaribrahim/dev/scripts/automation-scripts/video-conversion/convert-mkv-to-mp4.sh`
- **Monitor progress**: `/Users/omaribrahim/dev/scripts/automation-scripts/video-extraction/monitor.sh`

### Data References
- **Podcast manifest**: `/Users/omaribrahim/data/podcasts/metadata/PODCAST_MANIFEST.md`
- **Hoop highlights readme**: `/Users/omaribrahim/data/hoop-highlights/README.md`
- **Podcast processing**: `/Users/omaribrahim/data/podcasts/README.md`

---

## Workflows at a Glance

### Basketball Highlights

1. **Download clips from YouTube**
   ```bash
   cd /Users/omaribrahim/dev/scripts/automation-scripts/video-extraction
   # Edit clips.sh with new URLs (or use existing)
   ./clips.sh  # Downloads to /Users/omaribrahim/data/hoop-highlights/2025-11-08/clips/
   ```

2. **Convert MKV to MP4**
   ```bash
   /Users/omaribrahim/dev/scripts/automation-scripts/video-conversion/convert-mkv-to-mp4.sh \
     /Users/omaribrahim/data/hoop-highlights/2025-11-08/clips
   # Output goes to: .../2025-11-08/converted/
   ```

3. **Monitor progress**
   ```bash
   /Users/omaribrahim/dev/scripts/automation-scripts/video-extraction/monitor.sh
   ```

### Basketball Frame Analysis

**Status**: Planning phase
**Documentation**: `/Users/omaribrahim/dev/scripts/automation-scripts/basketball-analysis/PLANNING.md`

1. **Extract action frames** (future)
   ```bash
   cd /Users/omaribrahim/dev/scripts/automation-scripts/basketball-analysis
   source /Users/omaribrahim/dev/scripts/openaibatches/bin/activate
   python extract_frames.py --input /path/to/video.mp4 --output ./stills/
   ```

2. **Output location**:
   - Stills go to: `/Users/omaribrahim/data/hoop-highlights/YYYY-MM-DD/stills/`

### Podcast Processing

**All commands documented in**: `/Users/omaribrahim/data/podcasts/README.md`

1. **Extract audio from video**
   ```bash
   ffmpeg -i /Users/omaribrahim/data/podcasts/incoming/podcast-name.mp4 \
     -q:a 0 -map a \
     /Users/omaribrahim/data/podcasts/audio-extracted/full-length/podcast-name.mp3
   ```

2. **Split into 90-minute chunks**
   ```bash
   # Find silence points (optional):
   ffmpeg -i input.mp3 -af "silencedetect=n=-40dB:d=1" -f null - 2>&1 | grep silence

   # Split at specific time (5400 sec = 90 min):
   ffmpeg -i input.mp3 -ss 0 -to 5400 output_part1.mp3
   ffmpeg -i input.mp3 -ss 5400 -to 10800 output_part2.mp3
   ```

3. **Update metadata**
   ```bash
   # Document in: /Users/omaribrahim/data/podcasts/metadata/PODCAST_MANIFEST.md
   ```

---

## Naming Conventions

### Dates
- Format: `YYYY-MM-DD`
- Example: `2025-11-08`

### Folder Names
- **Simple and descriptive**: `hoop-highlights`, `podcasts`, `incoming`, `converted`
- **No vague names**: Never use `output`, `temp`, `data`, etc.

### File Names
- **Basketball clips**: `{Date} {Day} hoops_{START}-{END}.{ext}`
- **Podcast chunks**: `{podcast-name}_90min_part{N}.mp3`
- **Metadata**: `PODCAST_MANIFEST.md`, `README.md`

---

## Separation of Concerns

### Data Folder (`/Users/omaribrahim/data/`)
- ✓ Actual media files (videos, audio, podcasts)
- ✓ Documentation about the data
- ✓ Metadata and manifests
- ✗ NO automation scripts

### Scripts Folder (`/Users/omaribrahim/dev/scripts/automation-scripts/`)
- ✓ Automation and processing scripts
- ✓ Documentation on how to use scripts
- ✓ Tools and utilities
- ✗ NO large media files (output goes to /data/)

---

## Future Additions

When you add new automations:

1. **Create data folder** in `/Users/omaribrahim/data/`
   - Example: `new-automation-type/`

2. **Create script folder** in `/Users/omaribrahim/dev/scripts/automation-scripts/`
   - Example: `new-automation-processing/`

3. **Create README.md** in both locations explaining:
   - What the automation does
   - How to use it
   - Input/output locations
   - Commands to run

4. **Update documentation**:
   - `/Users/omaribrahim/data/ORGANIZATION.md`
   - `/Users/omaribrahim/data/QUICK_REFERENCE.md`
   - This file: `/Users/omaribrahim/dev/scripts/SYSTEM_ORGANIZATION.md`

5. **Commit to appropriate repo**:
   - Scripts → `/Users/omaribrahim/dev/scripts/.git`
   - Documentation → Both repos

---

## Current Status

### ✅ Completed
- [x] System-wide organization documented
- [x] Data folder structure created and organized
- [x] 12 basketball clips downloaded and stored
- [x] Video conversion script created
- [x] 11 podcasts moved from Downloads to organized structure
- [x] Podcast processing guide documented
- [x] Master documentation created (ORGANIZATION.md, QUICK_REFERENCE.md)
- [x] Both git repos initialized and documented

### ⏳ Next Steps
- [ ] Convert basketball clips to MP4 (ready to run)
- [ ] Extract audio from podcasts (manual ffmpeg commands)
- [ ] Split podcasts into 90-minute chunks (manual ffmpeg commands)
- [ ] Create podcast processing script (if doing 5+ regularly)

---

## Quick Links

| Need | Location |
|------|----------|
| How things are organized | `/Users/omaribrahim/data/ORGANIZATION.md` |
| Common commands | `/Users/omaribrahim/data/QUICK_REFERENCE.md` |
| Download basketball clips | `/Users/omaribrahim/dev/scripts/automation-scripts/video-extraction/clips.sh` |
| Convert videos | `/Users/omaribrahim/dev/scripts/automation-scripts/video-conversion/convert-mkv-to-mp4.sh` |
| Podcast processing guide | `/Users/omaribrahim/data/podcasts/README.md` |
| Podcast inventory | `/Users/omaribrahim/data/podcasts/metadata/PODCAST_MANIFEST.md` |
| Basketball highlights info | `/Users/omaribrahim/data/hoop-highlights/README.md` |
| Basketball frame analysis | `/Users/omaribrahim/dev/scripts/automation-scripts/basketball-analysis/PLANNING.md` |

---

## Philosophy

**Organization First**: Before automation, understand structure
- Folders are self-explanatory (no "output")
- Dates make files easy to find
- Documentation beats prompts (no long explanations needed)
- Separate data from code (different repos)
- Scalable: add new types without disrupting existing

**Consistency**: Same patterns for everything
- All data types follow: source → processing → output
- All scripts live in automation-scripts/
- All documentation is in README.md files
- .gitignore excludes large files everywhere

**No Repetition**: Documentation replaces prompts
- You never need to re-explain your system
- Everything is documented
- Scripts are reusable
- Add new tasks with minimal new documentation

