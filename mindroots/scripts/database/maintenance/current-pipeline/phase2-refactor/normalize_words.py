"""
Normalize Word nodes with layered normalization:
- arabic_no_diacritics
- arabic_normalized
- arabic_no_fem (conservative normalization with feminine markers removed)
"""

import os, re, unicodedata, time, sys
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Import unified normalization module
sys.path.append('/Users/omaribrahim/dev/scripts/mindroots/scripts/database/maintenance/current-pipeline')
from unified_normalization import normalize_arabic, strip_diacritics, create_normalization_layers

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

# Normalization functions are now imported from unified_normalization module

def update_words(tx, batch_size=500):
    # Remove arabic_no_fem as per instructions (too noisy)
    q = "MATCH (w:Word) WHERE w.arabic_normalized IS NULL RETURN elementId(w) AS wid, w.arabic AS arabic LIMIT $batch_size"
    rows = list(tx.run(q, batch_size=batch_size))
    updates = []
    for row in rows:
        wid = row["wid"]
        ar = row["arabic"]
        if not ar:
            continue

        # Use unified normalization
        layers = create_normalization_layers(ar)
        no_diac = layers['no_diacritics']
        normalized = layers['normalized']

        updates.append((wid, no_diac, normalized))

    for wid, no_diac, normalized in updates:
        tx.run("""
            MATCH (w:Word) WHERE elementId(w) = $wid
            SET w.arabic_no_diacritics = $no_diac,
                w.arabic_normalized = $normalized
        """, wid=wid, no_diac=no_diac, normalized=normalized)
    return len(updates)

def main():
    print("🔵 Normalizing Word nodes...")
    total = 0
    while True:
        with driver.session() as session:
            updated = session.execute_write(update_words)
            if updated == 0:
                break
            total += updated
            print(f"✅ Updated {total} words so far")
            time.sleep(0.2)
    driver.close()
    print(f"🎉 Done. Normalized {total} words using unified normalization pipeline.")

if __name__ == "__main__":
    main()