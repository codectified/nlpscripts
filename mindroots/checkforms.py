import csv, json, os
from collections import Counter, defaultdict
from dotenv import load_dotenv

# ─── Load the full word list to map ID → word ──────────────────────────────
def load_word_lookup(csv_file="clean_defs.csv"):
    lookup = {}
    with open(csv_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lookup[row["entry_id_xml"]] = row["word"]
    return lookup

# ─── Parse batch output files and gather counts + links ─────────────────────
def parse_llm_outputs(batch_dir=".", num_batches=50, word_lookup=None):
    form_counts = Counter()
    form_examples = {}
    word_links = []

    # (wazn, form) → form_id
    form_id_map = {}
    form_index = 1

    for i in range(1, num_batches + 1):
        file_path = os.path.join(batch_dir, f"batch_output_{i}.jsonl")
        if not os.path.exists(file_path):
            continue
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    entry_id = data["custom_id"]
                    msg = data["response"]["body"]["choices"][0]["message"]["content"]
                    parsed = json.loads(msg.strip())

                    wazn = parsed["wazn"]
                    form = parsed["form"]

                    if wazn == "NA" or form == "NA":
                        continue

                    key = (wazn, form)
                    form_counts[key] += 1

                    # Store the first example word seen
                    if key not in form_examples and entry_id in word_lookup:
                        form_examples[key] = word_lookup[entry_id]

                    # Assign a unique form_id
                    if key not in form_id_map:
                        form_id_map[key] = f"M{form_index}"
                        form_index += 1

                    # Link word to form_id
                    word_links.append((entry_id, form_id_map[key]))

                except Exception as e:
                    print(f"Error parsing entry in batch {i}: {e}")

    return form_counts, form_examples, form_id_map, word_links

# ─── Write results to CSVs ─────────────────────────────────────────────────
def write_outputs(form_counts, form_examples, form_id_map, word_links):
    with open("form_mapping_with_counts.csv", "w", newline='', encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["form_id", "wazn", "form", "count", "example_word"])
        for (wazn, form), count in form_counts.most_common():
            form_id = form_id_map[(wazn, form)]
            example_word = form_examples.get((wazn, form), "")
            writer.writerow([form_id, wazn, form, count, example_word])
    print(f"✓ Created {len(form_counts)} form entries in 'form_mapping_with_counts.csv'")

    with open("word_to_form_links.csv", "w", newline='', encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["entry_id", "form_id"])
        for entry_id, form_id in word_links:
            writer.writerow([entry_id, form_id])
    print(f"✓ Mapped {len(word_links)} word nodes to form_ids in 'word_to_form_links.csv'")

# ─── Entrypoint ────────────────────────────────────────────────────────────
def main():
    load_dotenv()
    word_lookup = load_word_lookup("clean_defs.csv")
    form_counts, form_examples, form_id_map, word_links = parse_llm_outputs(word_lookup=word_lookup)
    write_outputs(form_counts, form_examples, form_id_map, word_links)

if __name__ == "__main__":
    main()