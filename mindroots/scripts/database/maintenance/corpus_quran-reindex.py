import os
import time
import re
import unicodedata
import logging
from dotenv import load_dotenv
from neo4j import GraphDatabase
from camel_tools.utils.charmap import CharMapper
from camel_tools.utils.transliterate import Transliterator

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Logging ---
logger = logging.getLogger("reindexer")
logger.setLevel(logging.INFO)

fh = logging.FileHandler("reindex_corpus2.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# --- Transliteration + Normalization ---
bw2ar = Transliterator(CharMapper.builtin_mapper("bw2ar"))

def bw_to_arabic(bw: str) -> str:
    if not bw:
        return None
    try:
        return bw2ar.transliterate(bw)
    except Exception:
        return None

def strip_diacritics(text: str) -> str:
    if not text:
        return None
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0670]')
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

def normalize_arabic(text: str) -> str:
    if not text:
        return None
    text = strip_diacritics(text)
    text = re.sub(r'[أإآٱ]', 'ا', text)
    text = re.sub(r'ى', 'ي', text)
    text = re.sub(r'ة', 'ه', text)
    text = re.sub(r'ؤ', 'و', text)
    text = re.sub(r'ئ', 'ي', text)
    return text

# --- Helpers ---
def parse_location(loc: str):
    """Parse (s:a:w:s) location → ints"""
    s, a, w, seg = loc.strip("()").split(":")
    return int(s), int(a), int(w), int(seg)

# --- Neo4j Write ---
def write_batch(tx, batch):
    for row in batch:
        surah, ayah, word, seg, form, tag, feats = row

        # Extract lemma + root from features string
        lemma_match = re.search(r"LEM:([^|]+)", feats)
        root_match = re.search(r"ROOT:([^|]+)", feats)
        lemma_bw = lemma_match.group(1) if lemma_match else None
        root_bw = root_match.group(1) if root_match else None

        lemma_ar = bw_to_arabic(lemma_bw) if lemma_bw else None
        lemma_norm = normalize_arabic(lemma_ar) if lemma_ar else None
        root_ar = bw_to_arabic(root_bw) if root_bw else None

        item_id = f"{surah}:{ayah}:{word}"
        seg_form = f"s{seg}_form"
        seg_arabic = f"s{seg}_arabic"
        seg_lemma = f"s{seg}_lemma"
        seg_lemma_norm = f"s{seg}_lemma_norm"
        seg_root = f"s{seg}_root"

        tx.run(f"""
            MERGE (c:Corpus {{corpus_id: 2}})
            MERGE (s:Surah {{corpus_id: 2, surah_id: $surah}})
              ON CREATE SET s.node_type = "Surah"
            MERGE (a:Ayah {{corpus_id: 2, surah_id: $surah, ayah_id: $ayah}})
              ON CREATE SET a.node_type = "Ayah"
            MERGE (c)-[:HAS_SURAH]->(s)
            MERGE (s)-[:HAS_AYAH]->(a)
            MERGE (ci:CorpusItem {{corpus_id: 2, item_id: $item_id}})
              ON CREATE SET ci.node_type = "CorpusItem"
            MERGE (a)-[:HAS_ITEM]->(ci)
            SET ci.`{seg_form}` = $form,
                ci.`{seg_arabic}` = $form_ar,
                ci.`{seg_lemma}` = $lemma_bw,
                ci.`{seg_lemma_norm}` = $lemma_norm,
                ci.`{seg_root}` = $root_bw,
                ci.root = CASE WHEN $root_ar IS NOT NULL THEN $root_ar ELSE ci.root END
        """, surah=surah, ayah=ayah, item_id=item_id,
             form=form, form_ar=bw_to_arabic(form),
             lemma_bw=lemma_bw, lemma_norm=lemma_norm,
             root_bw=root_bw, root_ar=root_ar)

# --- Main ---
def reindex_tsv(tsv_path, batch_size=500, throttle=0.1):
    total = 0
    batch = []
    with open(tsv_path, "r", encoding="utf-8") as f, driver.session() as session:
        for line in f:
            if not line.strip() or line.startswith("LOCATION"):
                continue
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            loc, form, tag = parts[:3]
            feats = parts[3] if len(parts) > 3 else ""
            surah, ayah, word, seg = parse_location(loc)
            batch.append((surah, ayah, word, seg, form, tag, feats))

            if len(batch) >= batch_size:
                session.execute_write(write_batch, batch)
                total += len(batch)
                logger.info(f"✅ Processed batch, total {total}")
                batch = []
                time.sleep(throttle)

        if batch:
            session.execute_write(write_batch, batch)
            total += len(batch)
            logger.info(f"✅ Final batch processed, total {total}")

    logger.info(f"🎉 Done. Total rows processed: {total}")

# --- Run ---
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python reindex_corpus2.py <path_to_tsv>")
    else:
        reindex_tsv(sys.argv[1])