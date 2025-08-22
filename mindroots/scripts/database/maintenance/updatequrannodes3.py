import csv
import os
import time
from collections import defaultdict
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console

# Load env vars
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

console = Console()
driver = GraphDatabase.driver(uri, auth=(user, password))

TSV_PATH = "quranic-corpus-morphology-0.4.tsv"
CORPUS_ID = 2
TEST_LIMIT = None  # Set to None for full run

# --- Parse Helpers ---

def parse_location(loc_str):
    return tuple(map(int, loc_str.strip("()").split(":")))  # sura, aya, word, segment

def extract_feature_value(features, key):
    for feat in features.split("|"):
        if feat.startswith(f"{key}:"):
            return feat.split(":", 1)[1]
    return None

def parse_features(features, segment_index):
    prefix = f"s{segment_index}_"
    features_list = features.split("|")

    return {
        prefix + "lemma": extract_feature_value(features, "LEM"),
        prefix + "root": extract_feature_value(features, "ROOT"),
        prefix + "part_of_speech": extract_feature_value(features, "POS"),
        prefix + "gender": (
            "masculine" if "M" in features_list else
            "feminine" if "F" in features_list else None
        ),
        prefix + "number": (
            "singular" if "S" in features_list else
            "plural" if "P" in features_list else None
        ),
        prefix + "case": (
            "genitive" if "GEN" in features_list else
            "nominative" if "NOM" in features_list else
            "accusative" if "ACC" in features_list else None
        )
    }

def load_and_group_segments(tsv_path, limit=None):
    grouped = defaultdict(list)
    with open(tsv_path, "r", encoding="utf-8") as f:
        for _ in range(56): next(f)
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            sura, aya, word_pos, segment_index = parse_location(row["LOCATION"])
            prefix = f"s{segment_index}_"
            key = (sura, aya, word_pos)

            segment = {
                prefix + "LOCATION": row["LOCATION"],
                prefix + "FORM": row["FORM"],
                prefix + "TAG": row["TAG"],
                prefix + "FEATURES": row["FEATURES"],
                prefix + "segment_index": segment_index
            }
            segment.update(parse_features(row["FEATURES"], segment_index))
            grouped[key].append(segment)

    # ✅ Apply the limit *after* full grouping
    if limit is not None:
        grouped = dict(list(grouped.items())[:limit])

    return grouped


# --- DB Insertion Logic ---

def ingest_nodes(session, grouped_segments):
    count = 0

    for (sura, aya, word_pos), segments in grouped_segments.items():
        match_clause = {
            "corpus_id": CORPUS_ID,
            "sura_index": sura,
            "aya_index": aya,
            "word_position": word_pos
        }

        props = {k: v for segment in segments for k, v in segment.items()}

        query = f"""
        MERGE (ci:CorpusItem {{
            corpus_id: $corpus_id,
            sura_index: $sura_index,
            aya_index: $aya_index,
            word_position: $word_position
        }})
        SET {', '.join([f'ci.`{k}` = ${k}' for k in props])}
        """

        try:
            session.execute_write(
                lambda tx: tx.run(query, **match_clause, **props)
            )
            console.log(f"[green]✔ MERGED node ({sura}:{aya}:{word_pos})")
        except Exception as e:
            console.log(f"[red]❌ Failed to merge ({sura}:{aya}:{word_pos}): {e}")

        count += 1
        time.sleep(0.05)  # Aura-safe delay

    console.log(f"[bold green]✅ Done. Merged {count} CorpusItem nodes.")
# --- Entry Point ---

def main():
    console.log("[blue]Loading and grouping segments from TSV...")
    grouped = load_and_group_segments(TSV_PATH, limit=TEST_LIMIT)
    console.log(f"[cyan]Prepared {len(grouped)} unique words.")

    confirm = console.input("[bold yellow]Proceed to ingest with MERGE? (y/N): ").strip().lower()
    if confirm != "y":
        console.log("[red]Aborted.")
        return

    with driver.session() as session:
        ingest_nodes(session, grouped)

    driver.close()
    console.log("[bold red]Connection closed.")

if __name__ == "__main__":
    main()