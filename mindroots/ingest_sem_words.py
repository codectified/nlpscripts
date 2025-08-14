#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from neo4j import GraphDatabase

# ── ENV ───────────────────────────────────────────────────────────────────────
load_dotenv()
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASS = os.getenv("NEO4J_PASS")
if not all([URI, USER, PASS]):
    raise EnvironmentError("Missing NEO4J_URI, NEO4J_USER, or NEO4J_PASS")

# ── LOGGING (dual: console + rotating file) ───────────────────────────────────
def setup_logger(log_file="logs/sem_word_ingest.log"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger("sem_word_ingest")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

logger = setup_logger()

# ── CSV ───────────────────────────────────────────────────────────────────────
# sem_word headers: id,category,lang,root,word,concept,meaning,root_lang
def load_sem_words(path):
    rows = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        need = {"id","category","lang","root","word","concept","meaning","root_lang"}
        missing = need - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {missing}")
        for row in reader:
            try:
                row_id = int(row["id"])
                rows[row_id] = row
            except (KeyError, ValueError):
                logger.warning(f"Skipping invalid row (bad id): {row}")
    logger.info(f"Loaded {len(rows)} rows from {path}")
    return rows

# ── Resolver ──────────────────────────────────────────────────────────────────
def resolve_sem_id(row, rows_by_id):
    """
    If root_lang is empty: row['root'] is already the sem_id (int).
    If root_lang is set: row['root'] is a ROW ID → look up that row and take its 'root' as sem_id.
    """
    try:
        root_lang_val = (row.get("root_lang") or "").strip()
        if root_lang_val:  # derived -> follow to parent row
            parent_row_id = int(row["root"])
            parent = rows_by_id.get(parent_row_id)
            if not parent:
                logger.warning(f"Parent row id {parent_row_id} not found for child id {row.get('id')}")
                return None
            return int(parent["root"])  # parent's root is the sem_id
        # base/lemma -> root is sem_id directly
        return int(row["root"])
    except (ValueError, TypeError):
        return None

# ── Ingest ────────────────────────────────────────────────────────────────────
def ingest_sem_words(csv_path, batch_size=200, sleep_seconds=0.5, create_missing_roots=True):
    rows_by_id = load_sem_words(csv_path)
    driver = GraphDatabase.driver(URI, auth=(USER, PASS))

    processed = 0
    skipped = 0
    t0 = time.time()

    root_clause = "MERGE" if create_missing_roots else "MATCH"
    cypher = f"""
    {root_clause} (r:Root {{ sem_id: $sem_id }})
    CREATE (w:Word {{
        word: $word,
        lang: toInteger($lang),
        category: toInteger($category),
        concept: $concept,
        meaning: $meaning
    }})
    MERGE (w)-[:BELONGS_TO_SEMITIC_ROOT]->(r)
    """

    with driver.session() as session:
        for idx, (row_id, row) in enumerate(rows_by_id.items(), start=1):
            sem_id = resolve_sem_id(row, rows_by_id)
            if sem_id is None:
                logger.warning(f"Skipping row {row_id} — could not resolve sem_id.")
                skipped += 1
                continue

            session.run(
                cypher,
                {
                    "sem_id": sem_id,
                    "word": row.get("word"),
                    "lang": row.get("lang"),
                    "category": row.get("category"),
                    "concept": row.get("concept"),
                    "meaning": row.get("meaning"),
                },
            )
            processed += 1

            if idx % batch_size == 0:
                logger.info(f"Processed {processed} rows — pausing {sleep_seconds}s for Aura throttling…")
                time.sleep(sleep_seconds)

    elapsed = time.time() - t0
    logger.info(f"Done. processed={processed}, skipped={skipped}, elapsed={elapsed:.2f}s")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # change csv path / batch params as needed
    ingest_sem_words("sem_words.csv", batch_size=200, sleep_seconds=0.5, create_missing_roots=True)




    