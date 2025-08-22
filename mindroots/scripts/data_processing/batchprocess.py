import csv
import json
import os

def map_batch_output_to_csv(csv_file_path, output_dir, batch_files, output_csv):
    # Read the original CSV file
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        csv_rows = list(reader)  # Store all rows for index-based mapping

    # Open the final output CSV file
    with open(output_csv, 'w', newline='', encoding='utf-8') as mappedfile:
        fieldnames = ['word', 'entry_id_xml', 'english', 'spanish', 'urdu', 'transliteration']
        writer = csv.DictWriter(mappedfile, fieldnames=fieldnames)
        writer.writeheader()

        current_index = 0  # Track where we are in `csv_rows`

        for batch_file in batch_files:
            batch_output_path = os.path.join(output_dir, batch_file)

            # Read the batch output JSONL file
            with open(batch_output_path, 'r', encoding='utf-8') as batchfile:
                batch_lines = batchfile.readlines()

            for line in batch_lines:
                # Ensure we don't exceed CSV row count
                if current_index >= len(csv_rows):
                    print(f"Warning: More batch output entries than CSV rows! Stopping at {current_index}.")
                    break

                csv_row = csv_rows[current_index]
                result = json.loads(line)
                
                # Extract LLM response (should be a JSON string)
                try:
                    gpt_output = json.loads(result['response']['body']['choices'][0]['message']['content'].strip())
                except json.JSONDecodeError:
                    print(f"Skipping malformed entry at index {current_index}")
                    continue

                # Create a mapped row
                mapped_row = {
                    'word': csv_row['word'],
                    'entry_id_xml': csv_row['entry_id_xml'],
                    'english': gpt_output.get('english', ''),
                    'spanish': gpt_output.get('spanish', ''),
                    'urdu': gpt_output.get('urdu', ''),
                    'transliteration': gpt_output.get('transliteration', '')
                }

                # Write the mapped row to the output CSV file
                writer.writerow(mapped_row)

                # Increment index
                current_index += 1

    print(f"Final mapped output saved to '{output_csv}'.")

# Example usage
csv_file_path = "clean_defs.csv"  # Master CSV with 55,000 words
output_dir = "./"  # Directory where batch_output files are located
batch_files = [  # List of batch output files
    "batch_chunk1_output.jsonl",
    "batch_chunk2_output.jsonl",
    "batch_chunk3_output.jsonl",
    "batch_chunk4_output.jsonl",
    "batch_chunk5_output.jsonl"
]
output_csv = "final_translations.csv"  # Final output CSV

map_batch_output_to_csv(csv_file_path, output_dir, batch_files, output_csv)