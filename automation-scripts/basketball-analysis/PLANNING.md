# Basketball Frame Extraction & Shot Detection

**Created**: 2026-02-02
**Status**: Planning / Brainstorm Phase

---

## Project Vision

Automate extraction of meaningful still frames from pickup basketball live footage. The goal is to capture high-action moments and successful shots without manual scrubbing through hours of video.

---

## Core Ideas (Brainstorm)

### 1. Motion Intensity Detection
- Detect peaks in motion/activity to identify "action" moments
- Challenge: Action can happen far from camera (less pixel movement)
- Approach: Optical flow analysis, frame differencing

### 2. Ball-Through-Rim Detection (Primary Focus)
- **Key insight**: Fixed camera + fixed rim = consistent visual features
- Every made shot should produce a detectable signature:
  - Ball passing through specific screen coordinates
  - Net movement/deformation
  - Ball trajectory entering rim area from above
- This is more tractable than full shot detection

### 3. Shot Detection (Complex)
- Not all shots have salient trajectories (floaters, layups, close shots)
- Arc detection is unreliable for all shot types
- May be secondary goal or require ML approach

### 4. Delivery Options
- **CLI tool** (MVP): Script that processes video file → outputs stills
- **Web UI** (later): URL input, parameter tweaking, preview stills
- **Invisible UI**: Backend API connected to custom chatbot

---

## Technical Approaches

### A. Motion-Based Frame Selection

**Tools**: OpenCV, ffmpeg

**Method 1: Frame Differencing**
```python
# Compare consecutive frames, measure pixel change magnitude
diff = cv2.absdiff(frame1, frame2)
motion_score = np.sum(diff) / diff.size
```

**Method 2: Optical Flow**
```python
# Dense optical flow for motion vectors
flow = cv2.calcOpticalFlowFarneback(prev_gray, curr_gray, ...)
magnitude = np.sqrt(flow[..., 0]**2 + flow[..., 1]**2)
```

**Challenges**:
- Camera shake vs actual motion
- Distant action = less pixel movement
- Need adaptive thresholding

### B. Rim Region Detection (Ball Through Hoop)

**Premise**: With fixed camera, the rim occupies the same pixel region always.

**Setup Phase**:
1. User marks rim location in first frame (or auto-detect via circle detection)
2. Define "rim zone" bounding box
3. Define "above rim" detection zone

**Detection Logic**:
```
For each frame:
  1. Check "above rim" zone for ball-colored pixels
  2. Track ball entering rim zone from above
  3. Detect net movement (texture/edge changes in net region)
  4. Confirm: ball + downward trajectory + net disturbance = MAKE
```

**Visual Signatures of a Made Shot**:
- Ball appears in rim zone
- Net billows/deforms (edge detection changes)
- Ball exits below rim OR bounces in rim zone
- Temporal pattern: ~0.5-1 second event

### C. Ball Tracking

**Color-based Detection**:
- Basketball is orange/brown - can segment by color
- HSV color space works better than RGB
- Challenge: similar colors in environment

**Shape-based Detection**:
- Hough circle detection
- Ball size varies with distance from camera
- May need multi-scale detection

**ML-based Detection**:
- YOLO or similar object detection
- Pre-trained sports ball models exist
- More robust but heavier dependency

### D. Net Movement Detection

**Approach**:
1. Define net region below rim
2. Track edge density or texture variance over time
3. Spike in variance = net movement = potential make

```python
# Edge detection in net region
edges = cv2.Canny(net_region, 50, 150)
edge_density = np.sum(edges) / edges.size
# Spike detection over time series
```

---

## MVP Specification (CLI Tool)

### Input
- Video file path (MP4/MKV from existing hoop-highlights)
- Optional: rim region coordinates (or auto-detect)

### Output
- Directory of extracted still frames (PNG/JPG)
- Metadata file with timestamps and detection confidence
- Optional: Highlight reel (concatenated clips around detections)

### Parameters
```bash
python extract_frames.py \
  --input /path/to/video.mp4 \
  --output /path/to/stills/ \
  --mode [motion|makes|both] \
  --rim-region "x1,y1,x2,y2" \  # optional
  --threshold 0.7 \              # detection sensitivity
  --before 2 \                   # seconds before event
  --after 2                      # seconds after event
```

### Output Structure
```
stills/
├── frames/
│   ├── 00_01_23_450_motion.jpg
│   ├── 00_05_47_200_make.jpg
│   └── ...
├── clips/                       # optional short clips
│   ├── make_00_05_45.mp4
│   └── ...
└── detections.json              # timestamps + metadata
```

---

## Implementation Phases

### Phase 1: Motion Detection MVP
- [ ] Basic frame differencing
- [ ] Threshold-based frame extraction
- [ ] CLI interface
- [ ] Test on existing hoop-highlights footage

### Phase 2: Rim Region Setup
- [ ] Manual rim region input
- [ ] Auto-detection via Hough circles
- [ ] Visualizer to confirm region

### Phase 3: Made Shot Detection
- [ ] Ball color segmentation
- [ ] Rim zone monitoring
- [ ] Net movement detection
- [ ] Temporal pattern matching

### Phase 4: Refinement
- [ ] Confidence scoring
- [ ] False positive reduction
- [ ] Handle varying lighting conditions
- [ ] Distance normalization (near vs far action)

### Phase 5: UI Options
- [ ] Web interface (FastAPI + simple frontend)
- [ ] Chatbot integration (API backend)
- [ ] Parameter tuning interface

---

## Dependencies

### Core
- Python 3.x
- OpenCV (`opencv-python`)
- NumPy

### Optional
- ffmpeg (video manipulation)
- scikit-image (additional image processing)
- ultralytics (YOLO for ball detection)

### Installation
```bash
# In project venv
pip install opencv-python numpy
# Optional
pip install ultralytics scikit-image
```

---

## Data Structure

Following system conventions:

**Scripts**: `/Users/omaribrahim/dev/scripts/automation-scripts/basketball-analysis/`
```
basketball-analysis/
├── PLANNING.md           (this file)
├── README.md             (usage docs)
├── extract_frames.py     (main CLI tool)
├── detectors/
│   ├── motion.py
│   ├── ball_tracker.py
│   └── rim_detector.py
└── utils/
    ├── video_io.py
    └── visualization.py
```

**Data Output**: `/Users/omaribrahim/data/hoop-highlights/`
```
hoop-highlights/
├── YYYY-MM-DD/
│   ├── clips/            (original video)
│   ├── converted/        (MP4 versions)
│   └── stills/           (extracted frames) ← NEW
│       ├── frames/
│       ├── clips/
│       └── detections.json
```

---

## Research Questions

1. **What's the minimum viable rim detection?**
   - Can we just use a fixed bounding box user provides once per camera angle?

2. **Is ball tracking necessary for made shot detection?**
   - Or can we detect "ball-in-rim-zone + net-movement" without tracking?

3. **How consistent is the "make" signature?**
   - Need to analyze several makes to establish pattern

4. **What's the false positive rate for motion detection?**
   - People walking, camera shake, non-shot movements

5. **Do we need ML or can heuristics work?**
   - Start with heuristics, add ML if accuracy insufficient

---

## Next Steps

1. **Set up basic project structure** - Python files, venv setup
2. **Test motion detection** on existing hoop-highlights footage
3. **Manually annotate 10-20 makes** in sample video for validation
4. **Prototype rim region detection** with manual coordinates
5. **Iterate on made shot detection** algorithm

---

## References

- OpenCV Optical Flow: https://docs.opencv.org/4.x/d4/dee/tutorial_optical_flow.html
- Ball Detection in Sports: Various papers on sports analytics
- YOLO for Sports: ultralytics pretrained models

---

## Notes

- Fixed camera is a MAJOR advantage - simplifies detection significantly
- Start simple: motion peaks + rim zone presence may be enough
- User feedback loop: show detections, let user mark false positives
- Could eventually train a simple classifier on verified makes
