#!/usr/bin/env python3
"""
Backfill sem_lang Property for Word Nodes

This script reads sem_lang.csv to map numeric lang codes to sem_lang names
and updates all Word nodes in Neo4j where sem_lang is NULL but lang is set.
It also optionally logs mismatches where both lang and sem_lang exist but differ.

Author: Mindroots
Date: 2025-08-14
"""

import csv
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

# Neo4j connection details
URI = os.getenv('NEO4J_URI')
USER = os.getenv('NEO4J_USER')
PASSWORD = os.getenv('NEO4J_PASS')

def load_lang_mapping(csv_path):
    """Load lang code → sem_lang name mapping from CSV."""
    mapping = {}
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                code = int(row["id"])
                name = row["lang"]
                if name:
                    mapping[code] = name.strip()
            except (ValueError, KeyError):
                continue
    return mapping

def backfill_sem_lang(driver, mapping):
    """Update nodes where sem_lang is missing."""
    updates = 0
    with driver.session() as session:
        for code, name in mapping.items():
            result = session.run("""
                MATCH (w:Word)
                WHERE w.lang = $code AND (w.sem_lang IS NULL OR w.sem_lang = "")
                SET w.sem_lang = $name
                RETURN count(w) as updated
            """, code=code, name=name)
            updates += result.single()["updated"]
    return updates

def find_mismatches(driver, mapping):
    """Find nodes where lang and sem_lang disagree."""
    mismatches = []
    with driver.session() as session:
        for code, name in mapping.items():
            result = session.run("""
                MATCH (w:Word)
                WHERE w.lang = $code AND w.sem_lang IS NOT NULL AND w.sem_lang <> $name
                RETURN w.word as word, w.lang as lang, w.sem_lang as sem_lang
            """, code=code, name=name)
            mismatches.extend(result.data())
    return mismatches

def main():
    mapping = load_lang_mapping("sem_lang.csv")
    if not mapping:
        print("❌ Failed to load sem_lang.csv mapping.")
        return

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        updated_count = backfill_sem_lang(driver, mapping)
        print(f"✅ Updated sem_lang for {updated_count} Word nodes.")

        mismatches = find_mismatches(driver, mapping)
        if mismatches:
            print(f"⚠️ Found {len(mismatches)} mismatches:")
            for m in mismatches:
                print(f"  Word: {m['word']} | lang: {m['lang']} | sem_lang: {m['sem_lang']}")
        else:
            print("✅ No mismatches found.")
    finally:
        driver.close()

if __name__ == "__main__":
    main()