#!/usr/bin/env python3
import os
import json
import csv
from collections import Counter

# ─── Map Arabic pattern → simple English label ────────────────────────────────
pattern_mapping = {
    "فَعلان":     "faʿlān",
    "فَعيلٌ":    "faʿīl",
    "استفعل":    "istafʿal",
    "فَعول":     "faʿūl",
    "فُعُّول":    "fuʿūl",
    "فَعَلَ":    "faʿala (verb)",
    "أفْعَلَ":   "afʿala (verb)",
    "فَعَّلَ":   "faʿʿala (verb)",
    "فَاعَلَ":   "fāʿala (verb)",
    # …expand to cover all patterns you expect…
}

def preview_batch_outputs(directory=".", num_files=48):
    records = []
    for i in range(1, num_files+1):
        fn = os.path.join(directory, f"batch_output_{i}.jsonl")
        if not os.path.exists(fn):
            continue
        with open(fn, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                node_id = rec["custom_id"]
                pat = rec["response"]["body"]["choices"][0]["message"]["content"].strip()
                eng = pattern_mapping.get(pat, "UNKNOWN")
                records.append((node_id, pat, eng))

    # 1. frequency of each Arabic pattern
    freq = Counter(p for _, p, _ in records)
    print("\nPattern Frequencies:")
    for pat, cnt in freq.most_common():
        print(f"  {pat:<8}  ({pattern_mapping.get(pat,'?')}): {cnt}")

    # 2. write detailed CSV for inspection
    out_csv = os.path.join(directory, "patterns_preview.csv")
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as csvfile:
        w = csv.writer(csvfile)
        w.writerow(["node_id", "arabic_pattern", "english_label"])
        w.writerows(records)
    print(f"\nDetailed preview written to {out_csv}\n")

if __name__ == "__main__":
    preview_batch_outputs()