import time
import re
import unicodedata
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Logging ---
logger = logging.getLogger("segment_cleaner")
logger.setLevel(logging.INFO)
fh = logging.FileHandler("../logs/segment_cleaner.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# --- Cleaning function ---
def clean_arabic(text: str) -> str:
    if not text:
        return None
    # remove dirty chars
    text = text.replace("^", "").replace("#", "ئ").replace("@", "")
    # remove diacritics
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0653-\u0655\u0670]')
    text = unicodedata.normalize("NFKD", text)
    text = arabic_diacritics.sub("", text)
    # remove whitespace
    text = text.replace(" ", "")
    return text

# --- Batch update ---
def update_batch(tx, batch_size=500):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2
      AND (ci.s1_arabic IS NOT NULL 
           OR ci.s2_arabic IS NOT NULL 
           OR ci.s3_arabic IS NOT NULL 
           OR ci.s4_arabic IS NOT NULL 
           OR ci.s5_arabic IS NOT NULL 
           OR ci.s6_arabic IS NOT NULL 
           OR ci.s7_arabic IS NOT NULL)
      AND (ci.s1_arabic_clean IS NULL
           OR ci.s2_arabic_clean IS NULL
           OR ci.s3_arabic_clean IS NULL
           OR ci.s4_arabic_clean IS NULL
           OR ci.s5_arabic_clean IS NULL
           OR ci.s6_arabic_clean IS NULL
           OR ci.s7_arabic_clean IS NULL)
    RETURN ci.item_id AS item_id,
           ci.s1_arabic AS s1,
           ci.s2_arabic AS s2,
           ci.s3_arabic AS s3,
           ci.s4_arabic AS s4,
           ci.s5_arabic AS s5,
           ci.s6_arabic AS s6,
           ci.s7_arabic AS s7
    LIMIT $batch_size
    """
    result = tx.run(query, batch_size=batch_size)

    updates = []
    for record in result:
        item_id = record["item_id"]
        cleaned = {}
        for i in range(1, 8):
            seg = record.get(f"s{i}")
            if seg:
                cleaned[f"s{i}_arabic_clean"] = clean_arabic(seg)
        if cleaned:
            updates.append((item_id, cleaned))

    # Apply updates
    for item_id, cleaned in updates:
        set_clause = ", ".join([f"ci.{k} = ${k}" for k in cleaned.keys()])
        params = {"item_id": item_id, **cleaned}
        tx.run(f"""
            MATCH (ci:CorpusItem {{corpus_id: 2, item_id: $item_id}})
            SET {set_clause}
        """, **params)
        segment_info = ", ".join([f"{k}: '{v}'" for k, v in cleaned.items()])
        logger.info(f"✅ Updated item {item_id} → {segment_info}")

    return len(updates)

# --- Main loop ---
def main():
    logger.info("🔵 Starting segment cleanup (Corpus 2)...")
    total = 0
    batch = 0
    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_batch)
                if updated == 0:
                    logger.warning("⚠️ No more updates found. Stopping.")
                    break
                total += updated
                batch += 1
                logger.info(f"Batch {batch}: {updated} items updated (Total: {total})")
                time.sleep(0.2)  # Aura safety
    finally:
        driver.close()

    logger.info(f"🎉 Done. Cleaned segments for {total} CorpusItems.")

if __name__ == "__main__":
    main()