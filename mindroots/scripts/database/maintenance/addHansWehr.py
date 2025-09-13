"""
Hans Wehr Dictionary Integration Script
=====================================

Integrates Hans Wehr dictionary entries with existing Word nodes in the Neo4j database.
Uses the unified normalization pipeline to ensure consistent matching between Hans Wehr
entries and Lane's Lexicon Word nodes.

FEATURES:
- Unified normalization for accurate Arabic text matching
- Dual matching strategy: diacritics-stripped AND fully normalized
- Prevents overwriting existing hanswehr_entry properties
- Comprehensive logging of unmatched and corrupted entries
- Error handling for connection issues and malformed CSV data

NORMALIZATION STRATEGY:
For each Hans Wehr entry, creates both:
1. Diacritics-stripped version (matches w.arabic_no_diacritics)
2. Fully normalized version (matches w.arabic_normalized)

This dual approach maximizes matching success while maintaining text consistency.

USAGE:
    python addHansWehr.py

REQUIREMENTS:
- hanswehr_words_cleaned.csv (input file)
- Neo4j connection configured via .env file
- unified_normalization module in current-pipeline/

OUTPUT:
- unmatched_hanswehr_words.csv (entries that couldn't be matched)
- Console logging of processing progress and results
"""

import re
import unicodedata
import csv
import sys
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired
import time
from dotenv import load_dotenv
import os

# Import unified normalization module for consistent Arabic text processing
sys.path.append('/Users/omaribrahim/dev/scripts/mindroots/scripts/database/maintenance/current-pipeline')
from unified_normalization import normalize_arabic, strip_diacritics

# Load environment variables from the .env file
load_dotenv()

# Get the Neo4j connection details from the environment
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

# Validate that the credentials are set
if not all([uri, user, password]):
    raise ValueError("Missing Neo4j connection details. Ensure NEO4J_URI, NEO4J_USER, and NEO4J_PASS are set in your .env file.")

# Connect to Neo4j
driver = GraphDatabase.driver(uri, auth=(user, password))

def add_hanswehr_entry(tx, hanswehr_word, hanswehr_definition):
    """
    Match a Hans Wehr dictionary entry to existing Word nodes using unified normalization.

    MATCHING STRATEGY:
    Uses dual normalization approach to maximize matching success:
    1. Diacritics-stripped matching (w.arabic_no_diacritics)
    2. Fully normalized matching (w.arabic_normalized)

    CRITICAL FIX:
    Only updates Word nodes where hanswehr_entry IS NULL to prevent overwriting
    existing definitions (was incorrectly checking IS NOT NULL before).

    Args:
        tx: Neo4j transaction object
        hanswehr_word (str): Arabic word from Hans Wehr dictionary
        hanswehr_definition (str): Definition to add to matching Word node

    Returns:
        Neo4j record with matched word info, or None if no match found
    """
    # Apply unified normalization to Hans Wehr word for consistent matching
    normalized_word = normalize_arabic(hanswehr_word)  # Full normalization (alifs, ya, etc.)
    no_diac_word = strip_diacritics(hanswehr_word)     # Diacritics only

    # Debug logging for normalization results
    if hanswehr_word != normalized_word:
        print(f"  Normalization: '{hanswehr_word}' → '{normalized_word}'")

    query = """
    MATCH (w:Word)
    WHERE (w.arabic_no_diacritics = $no_diac_word OR w.arabic_normalized = $normalized_word)
      AND w.hanswehr_entry IS NULL  // CRITICAL: Only set definitions once
    SET w.hanswehr_entry = $hanswehr_definition
    RETURN w.arabic AS word, w.hanswehr_entry AS definition
    """
    result = tx.run(query,
                   no_diac_word=no_diac_word,
                   normalized_word=normalized_word,
                   hanswehr_definition=hanswehr_definition)
    return result.single()

def process_hanswehr_entries(hanswehr_file, unmatched_log_file):
    """
    Process Hans Wehr dictionary CSV file and integrate entries with Word nodes.

    PROCESSING WORKFLOW:
    1. Read CSV entries (word, definition)
    2. Filter out corrupted/oversized entries (>3 words)
    3. Apply unified normalization to each word
    4. Attempt matching against Word nodes
    5. Log unmatched entries for manual review

    Args:
        hanswehr_file (str): Path to Hans Wehr CSV file
        unmatched_log_file (str): Path for logging unmatched entries
    """
    row_counter = 0  # Add a counter to track the progress
    
    with open(hanswehr_file, newline='') as csvfile, \
         open(unmatched_log_file, 'w', newline='') as unmatched_file:

        hanswehr_reader = csv.DictReader(csvfile)
        fieldnames = ['word', 'definition']
        unmatched_writer = csv.DictWriter(unmatched_file, fieldnames=fieldnames)

        # Write the header for the unmatched words file
        unmatched_writer.writeheader()

        with driver.session() as session:
            for row in hanswehr_reader:
                row_counter += 1  # Track which row we're processing
                hanswehr_word = row['word']
                hanswehr_definition = row['definition']

                try:
                    # Filter out corrupted entries: likely corrupted if >3 words
                    if len(hanswehr_word.split()) > 3:
                        print(f"SKIPPED - Oversized entry at row {row_counter}: {hanswehr_word}")
                        unmatched_writer.writerow({'word': hanswehr_word, 'definition': hanswehr_definition})
                        continue

                    # Attempt to match and update using unified normalization
                    result = session.execute_write(add_hanswehr_entry, hanswehr_word, hanswehr_definition)

                    if result:
                        print(f"✅ MATCHED: '{hanswehr_word}' → Word '{result['word']}'")
                    else:
                        print(f"❌ UNMATCHED: '{hanswehr_word}' (no matching Word node found)")
                        # Log unmatched entry for manual review
                        unmatched_writer.writerow({'word': hanswehr_word, 'definition': hanswehr_definition})

                except (ServiceUnavailable, SessionExpired) as e:
                    print(f"Connection issue: {str(e)}, retrying after 10 seconds...")
                    time.sleep(10)
                except csv.Error as csv_err:
                    print(f"CSV error at row {row_counter}: {csv_err}, skipping this row.")
                    unmatched_writer.writerow({'word': hanswehr_word, 'definition': 'corrupted data'})

if __name__ == "__main__":
    print("🔄 Starting Hans Wehr dictionary integration...")
    print("📚 Using unified normalization for Arabic text matching")
    print("🎯 Matching strategy: diacritics-stripped + fully normalized")
    print("\n" + "="*60 + "\n")

    # Process Hans Wehr entries with unified normalization
    process_hanswehr_entries('hanswehr_words_cleaned.csv', 'unmatched_hanswehr_words.csv')

    print("\n" + "="*60)
    print("✅ Hans Wehr integration complete!")
    print("📄 Check unmatched_hanswehr_words.csv for entries that couldn't be matched")

    # Ensure clean database connection shutdown
    driver.close()
    print("🔌 Database connection closed.")