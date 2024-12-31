import csv
import json
import os

def map_batch_output_to_csv(csv_file_path, output_dir, num_batches):
    # Read the original CSV file
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        csv_rows = list(reader)
    
    # Process each batch output file
    for batch_num in range(1, num_batches + 1):
        batch_output_file = os.path.join(output_dir, f"batch_output_{batch_num}.jsonl")
        output_mapped_file = os.path.join(output_dir, f"mapped_output_{batch_num}.csv")

        with open(batch_output_file, 'r', encoding='utf-8') as batchfile:
            batch_lines = batchfile.readlines()

        # Create a new file to store the mapped output for this batch
        with open(output_mapped_file, 'w', newline='', encoding='utf-8') as mappedfile:
            fieldnames = ['word', 'entry_id_xml', 'type']
            writer = csv.DictWriter(mappedfile, fieldnames=fieldnames)
            writer.writeheader()

            # Calculate the starting index based on the batch number
            start_idx = (batch_num - 1) * len(batch_lines)

            for i, line in enumerate(batch_lines):
                csv_row = csv_rows[start_idx + i]
                result = json.loads(line)
                gpt_output = result['response']['body']['choices'][0]['message']['content'].strip()

                # Create a new row with only the required fields
                mapped_row = {
                    'word': csv_row['word'],
                    'entry_id_xml': csv_row['entry_id_xml'],
                    'type': gpt_output
                }

                # Write the mapped row to the output CSV file
                writer.writerow(mapped_row)

        print(f"Mapped output saved to '{output_mapped_file}'")

# Example usage
csv_file_path = "clean_defs.csv"
output_dir = "./"  # Directory where batch_output files are located
num_batches = 10  # Number of batch output files

map_batch_output_to_csv(csv_file_path, output_dir, num_batches)