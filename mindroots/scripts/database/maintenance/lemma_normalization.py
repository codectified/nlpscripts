import re
import unicodedata
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load env vars
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Normalization functions ---
def strip_diacritics(text: str) -> str:
    """Remove Arabic diacritics from text."""
    if text is None:
        return None
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0670]')  # harakat + dagger alif
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

def normalize_arabic(text: str) -> str:
    """Normalize Arabic text to canonical form."""
    if text is None:
        return None

    text = strip_diacritics(text)

    # Normalize common variants
    text = re.sub(r'[أإآ]', 'ا', text)   # all alif forms → ا
    text = re.sub(r'ى', 'ي', text)       # alif maqsura → ya
    text = re.sub(r'ة', 'ه', text)       # ta marbuta → ha
    text = re.sub(r'ؤ', 'و', text)       # hamza on waw → waw
    text = re.sub(r'ئ', 'ي', text)       # hamza on ya → ya
    text = re.sub(r'ٱ', 'ا', text)       # alif wasla → alif

    return text

# --- Neo4j Update ---
def update_lemmas(tx, batch_size=500):
    result = tx.run("""
        MATCH (ci:CorpusItem {corpus_id: 2})
        WHERE ci.lemma IS NOT NULL AND ci.lemma_normalized IS NULL
        RETURN elementId(ci) AS eid, ci.lemma AS lemma
        LIMIT $batch_size
    """, batch_size=batch_size)

    updates = []
    for record in result:
        eid = record["eid"]
        lemma = record["lemma"]
        normalized = normalize_arabic(lemma)
        if normalized:
            updates.append((eid, normalized))

    for eid, normalized in updates:
        tx.run("""
            MATCH (ci) WHERE elementId(ci) = $eid
            SET ci.lemma_normalized = $normalized
        """, eid=eid, normalized=normalized)

    return len(updates)

# --- Main process ---
def main():
    print("🔵 Starting lemma normalization for Corpus 2...")
    total = 0
    batch = 0
    try:
        while True:
            with driver.session() as session:
                updated = session.write_transaction(update_lemmas)
                if updated == 0:
                    break
                total += updated
                batch += 1
                print(f"✅ Batch {batch}: Updated {updated} lemmas (Total: {total})")
                time.sleep(0.2)  # throttle to avoid Aura load
    finally:
        driver.close()
    print(f"🎉 Done. Normalized {total} lemmas in total.")

if __name__ == "__main__":
    main()