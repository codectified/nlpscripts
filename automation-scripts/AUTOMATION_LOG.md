# Automation Scripts - Execution & Development Log

## 2025-11-08: Initial Setup & Organization

### Completed Tasks

#### Directory Structure Creation
- Created `/automation-scripts/` root directory
- Set up subdirectories:
  - `video-extraction/` - Video clip downloading and processing
  - `batch-processing/` - For future batch processing tasks
  - `utilities/` - For shared helper functions
- Created `video-extraction/output/` for downloaded video files

#### Video Extraction Workflow Organization
- Moved `clips.sh` from `/mindroots/youtube/` to `/automation-scripts/video-extraction/`
- Made script executable
- Created comprehensive documentation:
  - Main `README.md` for automation-scripts directory
  - Detailed `README.md` for video-extraction workflow
  - This log file for tracking automation tasks

#### Documentation Created
1. **Main README.md** - Overview of entire automation-scripts directory
   - Directory structure
   - Key principles for script development
   - Dependencies and quick start guides

2. **video-extraction/README.md** - Detailed workflow documentation
   - How to use clips.sh
   - How to add new clips programmatically
   - Input/output format specifications
   - Troubleshooting guide
   - Dependencies and error handling

3. **This File (AUTOMATION_LOG.md)** - Central log of all automation efforts

### Current State

**clips.sh Status:**
- Location: `/Users/omaribrahim/dev/scripts/automation-scripts/video-extraction/clips.sh`
- Contents: 12 pre-configured video clip extraction commands
- Each clip: 40-second segment from various YouTube live streams
- Estimated total runtime: 30-60 minutes for all 12 clips

**Input Format Standardization:**
- Current: Bash script with hardcoded commands
- Recommended format for future: JSON (see video-extraction/README.md)
- Allows for programmatic generation and management

### Next Steps

1. **Execute clips.sh** - Download all 12 video clips
   - Verify successful completion
   - Document any issues or special cases

2. **Create clips.json** - Standardized input format
   - Convert existing 12 clips to JSON
   - Set up for programmatic clip generation

3. **Develop Python wrapper** - Programmatic clip downloading
   - Accept JSON input
   - Generate clips.sh or call yt-dlp directly
   - Add validation and error handling

4. **Future Automation Ideas:**
   - Batch subtitle extraction and translation
   - Video metadata extraction and organization
   - Automatic thumbnail generation
   - Audio normalization and processing
   - Database ingestion of extracted clips

## Execution History

### Session 3: Clip Timing Investigation (2025-01-21)
- **Date**: 2025-01-21
- **Tasks**: Investigating clip timing issues, cataloging highlights
- **Status**: In progress - user looking for correct game video

**Video 1 (possibly wrong video):**
- **File**: `video-extraction/2025-12-22-clips.json`
- **URL**: https://youtube.com/live/ROkDnL_xlug
- **Clips**: 14 timestamps saved
- **Status**: May be wrong video

**Video 2 - 12/2 Game 2 (possibly wrong game, right day):**
- **Files**:
  - `video-extraction/2025-12-02-game2-clips.json`
  - `video-extraction/2025-12-02-game2-clips.csv` (for Google Sheets import)
- **URL**: https://www.youtube.com/live/I38lm297-io
- **Clips**: 10 timestamps with play types and some player names
- **Status**: Waiting for confirmation / user finding right game

**CSV Format for Google Sheets:**
Columns: clip, start_time, start_sec, end_sec, duration, play_type, player, notes, youtube_link
- YouTube links go directly to timestamp for easy review
- Missing data can be filled in by collaborators

**Next Steps**: User looking for correct game from 12/2

### Session 2: December 14 Clip Downloads (2025-12-14)
- **Date**: 2025-12-14
- **Tasks**: Downloaded basketball highlight clips
- **Status**: Completed but with issues
- **Output**: 9 clips (~28MB total) in 2025-12-14/clips/
- **Issues Reported**:
  - Clips appear to be only 5 seconds each
  - Timing may be offset from intended moments
  - Possible partial corruption
- **Note**: Original timestamps not documented - need to rebuild

### Session 1: Initial Organization
- **Date**: 2025-11-08
- **Tasks**: Directory setup, documentation, script organization
- **Status**: Complete (pending execution of clips.sh)
- **Time**: ~15 minutes setup and documentation

---

## Automation Candidates (Future)

### High Priority
- [ ] **Clips from JSON input** - Create Python script to generate clips.sh from timestamps.json
- [ ] **Parallel clip downloading** - Speed up multi-clip extraction
- [ ] **Clip metadata management** - Store info about downloaded clips (date, source, content description)

### Medium Priority
- [ ] **Subtitle generation** - Automatic speech-to-text for clips
- [ ] **Audio normalization** - Standardize audio levels across multiple clips
- [ ] **Thumbnail extraction** - Get key frames from clips

### Lower Priority
- [ ] **Video format conversion** - Convert downloaded clips to specific formats
- [ ] **Quality downsampling** - Create lower-bitrate versions for archival
- [ ] **Automated tagging** - Label clips with metadata from descriptions

---

## Directory Organization Standards

### For Each Automation Task
1. Create subdirectory with descriptive name
2. Include:
   - `README.md` - Detailed documentation
   - Executable script(s)
   - `input/` directory - Example input files
   - `output/` directory - (git-ignored) output location
3. Update this log file with task details
4. Update main README.md with new automation entry

### Documentation Requirements
- Purpose statement
- Usage examples
- Input/output format specifications
- Dependency list
- Troubleshooting guide
- Future enhancement ideas

---

## Notes

- All output directories should be in `.gitignore` to keep repository clean
- Scripts should be idempotent where possible
- Comprehensive logging aids debugging and auditing
- This log serves as both execution record and task planning document
