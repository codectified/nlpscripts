"""
Normalize CorpusItem lemmas with layered normalization:
- lemma (Arabic - from Buckwalter conversion)
- lemma_norm (normalized)
- lemma_no_fem (conservative normalization with feminine markers removed)
"""

import os, re, unicodedata, time, sys
from dotenv import load_dotenv
from neo4j import GraphDatabase
from camel_tools.utils.charmap import CharMapper
from camel_tools.utils.transliterate import Transliterator

# Import unified normalization module
sys.path.append('/Users/omaribrahim/dev/scripts/mindroots/scripts/database/maintenance/current-pipeline')
from unified_normalization import buckwalter_to_arabic, normalize_arabic, create_normalization_layers

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

# Normalization functions are now imported from unified_normalization module

def update_lemmas(tx, batch_size=500):
    """Update CorpusItem lemmas in batches using unified normalization"""
    # Remove lemma_no_fem as per instructions
    q = """
    MATCH (ci:CorpusItem {corpus_id: 2})
    WHERE (ci.s1_lemma IS NOT NULL OR ci.s2_lemma IS NOT NULL OR
           ci.s3_lemma IS NOT NULL OR ci.s4_lemma IS NOT NULL OR ci.s5_lemma IS NOT NULL)
      AND ci.lemma_norm IS NULL
    RETURN elementId(ci) AS cid,
           ci.s1_lemma AS s1_lemma,
           ci.s2_lemma AS s2_lemma,
           ci.s3_lemma AS s3_lemma,
           ci.s4_lemma AS s4_lemma,
           ci.s5_lemma AS s5_lemma
    LIMIT $batch_size
    """
    rows = list(tx.run(q, batch_size=batch_size))
    updates = []

    for row in rows:
        cid = row["cid"]

        # Process each segment lemma
        lemmas = []
        for i in range(1, 6):
            lemma_bw = row.get(f"s{i}_lemma")
            if lemma_bw:
                # Convert Buckwalter to Arabic using unified function
                lemma_ar = buckwalter_to_arabic(lemma_bw)
                lemmas.append(lemma_ar or "")

        # Use the first non-empty lemma for the main lemma property
        main_lemma = next((l for l in lemmas if l), None)

        if main_lemma:
            # Use unified normalization
            lemma_norm = normalize_arabic(main_lemma)
            updates.append((cid, main_lemma, lemma_norm))

    # Apply updates
    for cid, lemma, lemma_norm in updates:
        tx.run("""
            MATCH (ci:CorpusItem) WHERE elementId(ci) = $cid
            SET ci.lemma = $lemma,
                ci.lemma_norm = $lemma_norm
        """, cid=cid, lemma=lemma, lemma_norm=lemma_norm)

    return len(updates)

def main():
    print("=5 Normalizing CorpusItem lemmas...")
    total = 0
    while True:
        with driver.session() as session:
            updated = session.execute_write(update_lemmas)
            if updated == 0:
                break
            total += updated
            print(f" Updated {total} corpus items so far")
            time.sleep(0.2)
    driver.close()
    print(f"🎉 Done. Normalized {total} corpus items using unified normalization pipeline.")

if __name__ == "__main__":
    main()