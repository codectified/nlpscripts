import csv
import os
import time
from collections import defaultdict
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console

# Load environment variables
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

# Rich console for pretty logging
console = Console()
driver = GraphDatabase.driver(uri, auth=(user, password))

TSV_PATH = "quranic-corpus-morphology-0.4.tsv"

# --- Helpers ---

def parse_location(loc_str):
    parts = loc_str.strip("()").split(":")
    return tuple(map(int, parts))  # returns all 4: sura, aya, word, segment

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

            # Build entry with just prefixed keys
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

            # Add parsed, prefixed features
            entry.update(parse_features(row["FEATURES"], segment_index))

            entries.append(entry)
    return entries


# --- DB UPDATE LOGIC ---

def update_nodes(tx, entries, dry_run=True):
    count = 0
    WRITE_DELAY = 0.3     # 0.3s between writes ≈ ~3.3 writes/sec
    BATCH_SIZE = 50
    BATCH_DELAY = 2       # 2s pause every 50 writes
    for entry in entries:
        sura, aya, word_pos = entry["sura"], entry["aya"], entry["word_position"]
        result = tx.run("""
            MATCH (ci:CorpusItem {
                corpus_id: 2,
                sura_index: $sura,
                aya_index: $aya,
                word_position: $word_position
            })
            RETURN id(ci) AS id, properties(ci) AS props
        """, sura=sura, aya=aya, word_position=word_pos).single()

        if not result:
            console.log(f"[yellow]No node found for {sura}:{aya}:{word_pos}")
            continue

        node_id = result["id"]
        props = result["props"]
        updates = {}

        for k, v in entry.items():
            if k in ("sura", "aya", "word_position"):
                continue
            if props.get(k) != v:
                updates[k] = v

        if updates:
            count += 1
            console.print(f"\n[bold cyan]Node {node_id} ({sura}:{aya}:{word_pos}):")
            for k, v in updates.items():
                old = props.get(k)
                console.print(f"  [red]{k}[/red]: '{old}' → '{v}'")

            if not dry_run:
                tx.run(f"""
                    MATCH (ci) WHERE id(ci) = $id
                    SET {', '.join([f'ci.{k} = ${k}' for k in updates])}
                """, id=node_id, **updates)
            if not dry_run:
                time.sleep(WRITE_DELAY)
                if count % BATCH_SIZE == 0:
                    time.sleep(BATCH_DELAY)

    console.log(f"[green]Done. Updated {count} nodes.")

# --- MAIN ---

def main():
    entries = load_segmented_tsv(TSV_PATH)

    dry = console.input("[bold yellow]Dry run? (Y/n): ").strip().lower() != "n"
    console.log(f"[blue]Running in {'dry' if dry else 'write'} mode...")

    with driver.session() as session:
        session.write_transaction(update_nodes, entries, dry_run=dry)

    driver.close()

if __name__ == "__main__":
    main()