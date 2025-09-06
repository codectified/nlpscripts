"""
Rebuild sX_arabic from Buckwalter forms, and rebuild full_arabic / sem.
Handles special symbols #, @ (lets camel-tools handle ^ naturally in A^ sequences).
Creates multiple normalization layers:
- full_arabic_no_diac (diacritics stripped, preserving articles)
- full_arabic_no_fem (diacritics stripped + feminine markers removed)
"""

import os, time, re, unicodedata
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
    # Handle special cases that camel-tools doesn't handle correctly
    # Handle A^ → آ sequence (camel-tools expects | for madda alif, not A^)
    bw = re.sub(r"A\^", "آ", bw)  # collapse A^ sequence into madda alif
    bw = bw.replace("#", "}")     # force hamza on ya seat → ئ  
    bw = bw.replace("@", "")      # ignore @ symbol (remove entirely)
    bw = bw.replace("{", "A")     # waṣla alif → plain alif (for camel-tools)
    
    result = bw2ar.transliterate(bw)
    
    # Debug: log any unresolved ^ characters (indicates malformed Buckwalter)
    if "^" in result:
        print(f"WARNING: Unresolved ^ in Buckwalter '{bw}' → result: '{result}'")
    
    return result

def strip_diacritics_only(text):
    """Strip diacritics but preserve articles and other structure"""
    if not text: 
        return None
    # Strip diacritics including madda and hamza marks  
    diacs = re.compile(r'[\u064B-\u0655\u0670]')  # Include U+0653-U+0655 (madda, hamza above/below)
    text = unicodedata.normalize('NFKD', text)
    text = diacs.sub('', text)
    # Normalize waṣla alif to regular alif
    text = text.replace('ٱ', 'ا')  # U+0671 → U+0627
    return text

def rebuild_batch(tx, batch_size=500):
    q = """
    MATCH (ci:CorpusItem {corpus_id: 2})
    WHERE ci.full_arabic_no_diac IS NULL
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
            # Add diacritics-only stripped version (preserves articles)
            props["full_arabic_no_diac"] = strip_diacritics_only(full)
            # Add feminine marker removal layer (conservative normalization)
            props["full_arabic_no_fem"] = strip_diacritics_only(full).replace('ة', '') if full else None
        updates.append((item_id, props))

    for item_id, props in updates:
        set_clause = ", ".join([f"ci.{k} = ${k}" for k in props])
        tx.run(f"""
            MATCH (ci:CorpusItem {{corpus_id: 2, item_id: $item_id}})
            SET {set_clause}
        """, item_id=item_id, **props)

    return len(updates)

def main():
    print("🔵 Rebuilding sX_arabic, full_arabic/sem, full_arabic_no_diac, and full_arabic_no_fem...")
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