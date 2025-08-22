import csv
import json
import os

def map_batch_output_to_wazn(csv_file_path, output_dir, output_csv, batch_count=45):
    # Define which original fields to keep (exclude definitions or anything unnecessary)
    fields_to_keep = ['entry_id_xml', 'word', 'arabic', 'english']  # Add/remove as needed

    # Load the original word list
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        csv_rows = list(reader)

    # Index rows by entry_id_xml for fast lookup
    csv_index = {row['entry_id_xml']: row for row in csv_rows}

    # Output fieldnames: keep only selected + wazn + form
    fieldnames = fields_to_keep + ['wazn', 'form']
    with open(output_csv, 'w', newline='', encoding='utf-8') as mappedfile:
        writer = csv.DictWriter(mappedfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, batch_count + 1):
            batch_filename = f"batch_output_{i}.jsonl"
            batch_path = os.path.join(output_dir, batch_filename)

            if not os.path.exists(batch_path):
                print(f"⚠️ Missing batch file: {batch_filename}")
                continue

            with open(batch_path, 'r', encoding='utf-8') as batchfile:
                for line in batchfile:
                    try:
                        response = json.loads(line)
                        content = response["response"]["body"]["choices"][0]["message"]["content"].strip()
                        parsed = json.loads(content)
                        entry_id = parsed["id"]
                        wazn = parsed.get("wazn", "")
                        form = parsed.get("form", "")
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"Skipping malformed line in {batch_filename}: {e}")
                        continue

                    original_row = csv_index.get(entry_id)
                    if not original_row:
                        print(f"⚠️ ID {entry_id} not found in CSV.")
                        continue

                    # Construct output row with only relevant fields
                    mapped_row = {field: original_row.get(field, '') for field in fields_to_keep}
                    mapped_row.update({"wazn": wazn, "form": form})
                    writer.writerow(mapped_row)

    print(f"✅ Final output written to '{output_csv}' without definitions.")

# Example usage
map_batch_output_to_wazn(
    csv_file_path="clean_defs.csv",
    output_dir="./",
    output_csv="compact_wazn_output.csv",
    batch_count=45
)