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

# --- Normalization helpers ---
def strip_diacritics(text: str) -> str:
    if text is None:
        return None
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0670]')  # harakat + dagger alif
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

def normalize_arabic(text: str) -> str:
    if text is None:
        return None
    text = strip_diacritics(text)
    text = re.sub(r'[أإآٱ]', 'ا', text)  # unify alifs
    text = re.sub(r'ى', 'ي', text)       # alif maqsura → ya
    text = re.sub(r'ة', 'ه', text)       # ta marbuta → ha
    text = re.sub(r'ؤ', 'و', text)       # hamza on waw → waw
    text = re.sub(r'ئ', 'ي', text)       # hamza on ya → ya
    return text

def bw_to_arabic(bw: str) -> str:
    if bw:
        return trans.convert(bw, 'tim', 'arabic')
    return None

# --- Neo4j Update ---
def update_corpusitems(tx, batch_size=500):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2
      AND ci.lemma_norm_seg IS NULL
      AND (ci.s1_root IS NOT NULL OR ci.s2_root IS NOT NULL OR
           ci.s3_root IS NOT NULL OR ci.s4_root IS NOT NULL OR
           ci.s5_root IS NOT NULL)
    RETURN ci.item_id AS item_id,
           ci.s1_root AS s1_root, ci.s1_lemma AS s1_lemma,
           ci.s2_root AS s2_root, ci.s2_lemma AS s2_lemma,
           ci.s3_root AS s3_root, ci.s3_lemma AS s3_lemma,
           ci.s4_root AS s4_root, ci.s4_lemma AS s4_lemma,
           ci.s5_root AS s5_root, ci.s5_lemma AS s5_lemma
    LIMIT $batch_size
    """
    result = tx.run(query, batch_size=batch_size)

    updates = []
    for record in result:
        item_id = record["item_id"]

        lemma_ar = None

        # Check each segment s1–s5 in order
        for i in range(1, 6):
            root = record.get(f"s{i}_root")
            lemma = record.get(f"s{i}_lemma")
            if root and lemma:
                lemma_ar = bw_to_arabic(lemma)
                break

        if lemma_ar:
            norm_val = normalize_arabic(lemma_ar)
            updates.append((item_id, norm_val))

    for item_id, norm_val in updates:
        tx.run("""
            MATCH (ci:CorpusItem {corpus_id: 2, item_id: $item_id})
            SET ci.lemma_norm_seg = $norm_val
        """, item_id=item_id, norm_val=norm_val)

    return len(updates)

# --- Main process ---
def main():
    print("🔵 Starting lemma normalization for CorpusItems (s1–s5 → lemma_norm_seg)...")
    total = 0
    batch = 0
    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_corpusitems)
                if updated == 0:
                    break
                total += updated
                batch += 1
                print(f"✅ Batch {batch}: Updated {updated} items (Total: {total})")
                time.sleep(0.2)  # throttle for Aura
    finally:
        driver.close()
    print(f"🎉 Done. Updated {total} CorpusItems.")

if __name__ == "__main__":
    main()