import re
import unicodedata
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import pyarabic.trans as trans

# Load env vars
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Normalization functions ---
def strip_diacritics(text: str) -> str:
    """Remove Arabic diacritics."""
    if text is None:
        return None
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0670]')  # includes harakat + dagger alif
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

def normalize_arabic(text: str) -> str:
    """Normalize Arabic text for orthography consistency."""
    if text is None:
        return None

    text = strip_diacritics(text)

    # Normalize common variants
    text = re.sub(r'[أإآٱ]', 'ا', text)  # all alif variants → ا
    text = re.sub(r'ى', 'ي', text)       # alif maqsura → ya
    text = re.sub(r'ة', 'ه', text)       # ta marbuta → ha
    text = re.sub(r'ؤ', 'و', text)       # hamza on waw → waw
    text = re.sub(r'ئ', 'ي', text)       # hamza on ya → ya

    return text

def arabic_to_bw(text: str) -> str:
    """Convert Arabic text to Buckwalter transliteration."""
    if text:
        return trans.convert(text, 'arabic', 'tim')
    return None

# --- Neo4j Update ---
def update_words(tx, batch_size=500):
    result = tx.run("""
        MATCH (w:Word)
        WHERE w.arabic IS NOT NULL
          AND (w.arabic_normalized IS NULL OR w.bw_arabic IS NULL)
        RETURN elementId(w) AS eid, w.arabic AS arabic
        LIMIT $batch_size
    """, batch_size=batch_size)

    updates = []
    for record in result:
        eid = record["eid"]
        arabic = record["arabic"]

        norm_val = normalize_arabic(arabic)
        bw_val = arabic_to_bw(arabic)

        updates.append((eid, norm_val, bw_val))

    for eid, norm_val, bw_val in updates:
        tx.run("""
            MATCH (w) WHERE elementId(w) = $eid
            SET w.arabic_normalized = $norm_val,
                w.bw_arabic = $bw_val
        """, eid=eid, norm_val=norm_val, bw_val=bw_val)

    return len(updates)

# --- Main process ---
def main():
    print("🔵 Starting backfill for Word nodes (arabic_normalized + bw_arabic)...")
    total = 0
    batch = 0
    try:
        while True:
            with driver.session() as session:
                updated = session.write_transaction(update_words)
                if updated == 0:
                    break
                total += updated
                batch += 1
                print(f"✅ Batch {batch}: Updated {updated} Word nodes (Total: {total})")
                time.sleep(0.2)  # throttle for Aura
    finally:
        driver.close()
    print(f"🎉 Done. Updated {total} Word nodes.")

if __name__ == "__main__":
    main()