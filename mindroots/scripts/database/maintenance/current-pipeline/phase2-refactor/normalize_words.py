"""
Normalize Word nodes with layered normalization:
- arabic_no_diacritics
- arabic_normalized
- arabic_no_fem (conservative normalization with feminine markers removed)
"""

import os, re, unicodedata, time
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def strip_diacritics(text):
    """Strip all diacritics including madda"""
    if not text: 
        return None
    # Strip diacritics including madda and hamza marks  
    diacs = re.compile(r'[\u064B-\u0655\u0670]')  # Include U+0653-U+0655 (madda, hamza above/below)
    text = unicodedata.normalize('NFKD', text)
    return diacs.sub('', text)

def normalize_arabic(text, conservative=False):
    """
    Normalize Arabic text with optional conservative mode
    
    Standard normalization:
    - Strip diacritics (including madda)
    - Normalize alifs (أ, إ, آ, ٱ → ا)
    - Normalize ya (ى → ي)
    - Keep ta marbuta (ة stays as is)
    - Normalize hamza seats (ؤ → و, ئ → ي)
    
    Conservative mode (conservative=True):
    - Same as above, plus ة → ه
    """
    if not text: 
        return None
    
    text = strip_diacritics(text)
    
    # Normalize alifs - madda and hamza alifs are decomposed by NFKD and diacritics removed
    text = text.replace('ٱ', 'ا')  # alif wasla (not decomposed by NFKD)
    
    # Normalize ya
    text = text.replace('ى', 'ي')  # ya alif maqsura
    
    # Conservative mode: remove feminine markers completely
    if conservative:
        text = text.replace('ة', 'ه')  # ta marbuta → ha
    
    # Normalize hamza seats - these are handled by NFKD + diacritic removal
    # No additional processing needed as hamza marks are stripped
    
    return text

def update_words(tx, batch_size=500):
    q = "MATCH (w:Word) RETURN elementId(w) AS wid, w.arabic AS arabic LIMIT $batch_size"
    rows = list(tx.run(q, batch_size=batch_size))
    updates = []
    for row in rows:
        wid = row["wid"]
        ar = row["arabic"]
        if not ar: continue
        no_diac = strip_diacritics(ar)
        norm = normalize_arabic(ar, conservative=False)
        no_fem = normalize_arabic(ar, conservative=True)
        updates.append((wid, no_diac, norm, no_fem))
    for wid, no_diac, norm, no_fem in updates:
        tx.run("""
            MATCH (w:Word) WHERE elementId(w) = $wid
            SET w.arabic_no_diacritics = $no_diac,
                w.arabic_normalized = $norm,
                w.arabic_no_fem = $no_fem
        """, wid=wid, no_diac=no_diac, norm=norm, no_fem=no_fem)
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
    print(f"🎉 Done. Normalized {total} words.")

if __name__ == "__main__":
    main()