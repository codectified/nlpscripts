"""
Rebuild sX_arabic from Buckwalter forms, and rebuild full_arabic / sem.
Handles special symbols ^, #, @.
"""

import os, time, re, logging
from dotenv import load_dotenv
from neo4j import GraphDatabase
from camel_tools.utils.charmap import CharMapper
from camel_tools.utils.transliterate import Transliterator

# Setup
load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

# Transliterator
bw2ar = Transliterator(CharMapper.builtin_mapper("bw2ar"))

def custom_bw_to_arabic(bw: str) -> str:
    if not bw:
        return None
    # Handle special cases
    bw = bw.replace("^", "A")  # madda alif → آ
    bw = bw.replace("#", "}")  # force hamza on ya seat → ئ
    bw = bw.replace("@", "A")  # long alif → ا
    return bw2ar.transliterate(bw)

def rebuild_batch(tx, batch_size=500):
    q = """
    MATCH (ci:CorpusItem {corpus_id: 2})
    RETURN ci.item_id AS item_id,
           ci.s1_form AS s1_form,
           ci.s2_form AS s2_form,
           ci.s3_form AS s3_form,
           ci.s4_form AS s4_form,
           ci.s5_form AS s5_form
    LIMIT $batch_size
    """
    rows = list(tx.run(q, batch_size=batch_size))
    updates = []

    for row in rows:
        item_id = row["item_id"]
        segments = []
        props = {}
        for i in range(1, 6):
            form = row.get(f"s{i}_form")
            if form:
                arabic = custom_bw_to_arabic(form)
                props[f"s{i}_arabic"] = arabic
                segments.append(arabic or "")
        if segments:
            full = "".join(segments)
            props["full_arabic"] = full
            props["sem"] = full
        updates.append((item_id, props))

    for item_id, props in updates:
        set_clause = ", ".join([f"ci.{k} = ${k}" for k in props])
        tx.run(f"""
            MATCH (ci:CorpusItem {{corpus_id: 2, item_id: $item_id}})
            SET {set_clause}
        """, item_id=item_id, **props)

    return len(updates)

def main():
    print("🔵 Rebuilding sX_arabic and full_arabic/sem...")
    total = 0
    while True:
        with driver.session() as session:
            updated = session.execute_write(rebuild_batch)
            if updated == 0:
                break
            total += updated
            print(f"✅ Updated {total} corpus items so far")
            time.sleep(0.2)
    driver.close()
    print(f"🎉 Done. Rebuilt {total} corpus items.")

if __name__ == "__main__":
    main()