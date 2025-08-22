import csv
import os
import time
import logging
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console

# Load environment variables
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

# Rich console logging
console = Console()

# Optional file-based logging
logging.basicConfig(
    filename='update_log.txt',
    filemode='a',
    format='%(asctime)s - %(message)s',
    level=logging.INFO
)

# Neo4j driver
driver = GraphDatabase.driver(uri, auth=(user, password))

TSV_PATH = "quranic-corpus-morphology-0.4.tsv"

# --- Helpers ---

def parse_location(loc_str):
    parts = loc_str.strip("()").split(":")
    return tuple(map(int, parts))  # returns: sura, aya, word, segment

def extract_feature_value(features, key):
    for feat in features.split("|"):
        if feat.startswith(key + ":"):
            return feat.split(":", 1)[1]
    return None

def parse_features(features, segment_index):
    prefix = f"s{segment_index}_"
    return {
        prefix + "lemma": extract_feature_value(features, "LEM"),
        prefix + "root": extract_feature_value(features, "ROOT"),
        prefix + "part_of_speech": extract_feature_value(features, "POS"),
        prefix + "gender": "masculine" if "M" in features else "feminine" if "F" in features else None,
        prefix + "number": "singular" if "S" in features else "plural" if "P" in features else None,
        prefix + "case": "genitive" if "GEN" in features else "nominative" if "NOM" in features else "accusative" if "ACC" in features else None
    }

def load_segmented_tsv(path):
    entries = []
    with open(path, "r", encoding="utf-8") as f:
        for _ in range(56):
            next(f)
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            sura, aya, word_pos, segment_index = parse_location(row["LOCATION"])
            prefix = f"s{segment_index}_"

            entry = {
                "sura": sura,
                "aya": aya,
                "word_position": word_pos,
                "segment_index": segment_index,
                prefix + "LOCATION": row["LOCATION"],
                prefix + "FORM": row["FORM"],
                prefix + "TAG": row["TAG"],
                prefix + "FEATURES": row["FEATURES"]
            }

            entry.update(parse_features(row["FEATURES"], segment_index))
            entries.append(entry)
    return entries

# --- Database Update Logic ---

def update_nodes(tx, entries, dry_run=True):
    count = 0
    WRITE_DELAY = 0.3
    BATCH_SIZE = 50
    BATCH_DELAY = 2

    for entry in entries:
        sura, aya, word_pos = entry["sura"], entry["aya"], entry["word_position"]
        result = tx.run("""
            MATCH (ci:CorpusItem {
                corpus_id: 2,
                sura_index: $sura,
                aya_index: $aya,
                word_position: $word_position
            })
            RETURN elementId(ci) AS eid, properties(ci) AS props
        """, sura=sura, aya=aya, word_position=word_pos).single()

        if not result:
            console.log(f"[yellow]No node found for {sura}:{aya}:{word_pos}")
            continue

        element_id = result["eid"]
        props = result["props"]

        updates = {
            k: v for k, v in entry.items()
            if k not in ("sura", "aya", "word_position") and v is not None and props.get(k) != v
        }

        if not updates:
            continue

        count += 1
        console.print(f"\n[bold cyan]Node {element_id} ({sura}:{aya}:{word_pos}):")
        for k, v in updates.items():
            old = props.get(k)
            console.print(f"  [red]{k}[/red]: '{old}' → '{v}'")

        logging.info(f"Updating node {element_id} ({sura}:{aya}:{word_pos}) with {len(updates)} fields")

        if not dry_run:
            try:
                set_clause = ", ".join([f"ci.`{k}` = ${k}" for k in updates])
                params = {"eid": element_id, **updates}

                tx.run(f"""
                    MATCH (ci:CorpusItem)
                    WHERE elementId(ci) = $eid
                    SET {set_clause}
                """, **params)

                console.log(f"[green]✔ Updated node {element_id}")
            except Exception as e:
                console.log(f"[red]❌ Failed to update node {element_id}: {e}")
                logging.error(f"Failed to update node {element_id}: {e}")

            time.sleep(WRITE_DELAY)
            if count % BATCH_SIZE == 0:
                time.sleep(BATCH_DELAY)

    console.log(f"[bold green]✅ Done. Updated {count} nodes.")


# --- Main Entrypoint ---

def main():
    entries = load_segmented_tsv(TSV_PATH)
    dry = console.input("[bold yellow]Dry run? (Y/n): ").strip().lower() != "n"
    console.log(f"[blue]Running in {'dry' if dry else 'write'} mode...")

    with driver.session() as session:
        session.execute_write(update_nodes, entries, dry_run=dry)  # ✅ FIXED HERE

    driver.close()
    console.log("[bold red]Connection closed.")

if __name__ == "__main__":
    main()
    main()