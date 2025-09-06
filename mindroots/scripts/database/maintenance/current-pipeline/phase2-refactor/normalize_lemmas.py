"""
Normalize CorpusItem lemmas with layered normalization:
- lemma (Arabic - from Buckwalter conversion)
- lemma_norm (normalized)
- lemma_no_fem (conservative normalization with feminine markers removed)
"""

import os, re, unicodedata, time
from dotenv import load_dotenv
from neo4j import GraphDatabase
from camel_tools.utils.charmap import CharMapper
from camel_tools.utils.transliterate import Transliterator

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

# Transliterator for Buckwalter to Arabic
bw2ar = Transliterator(CharMapper.builtin_mapper("bw2ar"))

def buckwalter_to_arabic(bw: str) -> str:
    """Convert Buckwalter transliteration to Arabic"""
    if not bw:
        return None
    # Handle special cases that camel-tools doesn't handle correctly
    # Handle alif madda (آ) cases before transliteration
    bw = re.sub(r"A\^", "آ", bw)  # collapse A^ → آ
    bw = bw.replace("^", "آ")     # catch any remaining stray ^ → آ
    bw = bw.replace("#", "}")     # force hamza on ya seat → ئ
    bw = bw.replace("@", "")      # ignore @ symbol (remove entirely)
    bw = bw.replace("{", "A")     # waṣla alif → plain alif (for camel-tools)
    
    result = bw2ar.transliterate(bw)
    
    # Debug: log any unresolved ^ characters (indicates malformed Buckwalter)
    if "^" in result:
        print(f"WARNING: Unresolved ^ in Buckwalter '{bw}' → result: '{result}'")
    
    return result

def strip_diacritics(text):
    """Strip all diacritics including madda and hamza marks"""
    if not text: 
        return None
    # Strip diacritics including madda and hamza marks  
    diacs = re.compile(r'[\u064B-\u0655\u0670]')  # Include U+0653-U+0655 (madda, hamza above/below)
    text = unicodedata.normalize('NFKD', text)
    text = diacs.sub('', text)
    # Normalize waṣla alif to regular alif
    text = text.replace('ٱ', 'ا')  # U+0671 → U+0627
    return text

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
    - Same as above, plus remove ة entirely
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
        text = text.replace('ة', '')  # ta marbuta → remove entirely
    
    # Normalize hamza seats - these are handled by NFKD + diacritic removal
    # No additional processing needed as hamza marks are stripped
    
    return text

def update_lemmas(tx, batch_size=500):
    """Update CorpusItem lemmas in batches"""
    q = """
    MATCH (ci:CorpusItem {corpus_id: 2})
    WHERE (ci.s1_lemma IS NOT NULL OR ci.s2_lemma IS NOT NULL OR 
           ci.s3_lemma IS NOT NULL OR ci.s4_lemma IS NOT NULL OR ci.s5_lemma IS NOT NULL)
      AND ci.lemma_no_fem IS NULL
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
                # Convert Buckwalter to Arabic
                lemma_ar = buckwalter_to_arabic(lemma_bw)
                lemmas.append(lemma_ar or "")
        
        # Use the first non-empty lemma for the main lemma property
        main_lemma = next((l for l in lemmas if l), None)
        
        if main_lemma:
            lemma_norm = normalize_arabic(main_lemma, conservative=False)
            lemma_no_fem = normalize_arabic(main_lemma, conservative=True)
            
            updates.append((cid, main_lemma, lemma_norm, lemma_no_fem))
    
    # Apply updates
    for cid, lemma, lemma_norm, lemma_no_fem in updates:
        tx.run("""
            MATCH (ci:CorpusItem) WHERE elementId(ci) = $cid
            SET ci.lemma = $lemma,
                ci.lemma_norm = $lemma_norm,
                ci.lemma_no_fem = $lemma_no_fem
        """, cid=cid, lemma=lemma, lemma_norm=lemma_norm, lemma_no_fem=lemma_no_fem)
    
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
    print(f"<� Done. Normalized {total} corpus items.")

if __name__ == "__main__":
    main()