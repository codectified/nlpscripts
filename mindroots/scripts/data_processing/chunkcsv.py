import csv

def chunk_csv_with_all_columns(input_file, output_prefix, chunk_size=10000):
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        headers = next(reader)  # Read the headers
        
        chunk = []
        invalid_rows = []  # To store problematic rows
        
        for i, row in enumerate(reader, start=1):
            # Validate row: Ensure the number of columns matches the headers
            if len(row) != len(headers):
                invalid_rows.append((i, "Row length does not match headers", row))
                continue

            # Validation: Check specific required fields (adjust as needed)
            row_data = dict(zip(headers, row))
            if not row_data.get('word'):  # Ensure 'word' is not empty
                invalid_rows.append((i, "Missing 'word' value", row))
                continue
            if not row_data.get('definitions_xml'):  # Ensure 'definitions_xml' is not empty
                invalid_rows.append((i, "Missing 'definitions_xml' value", row))
                continue

            # Add the valid row to the current chunk
            chunk.append(row)

            # Write chunk to file when chunk size is reached
            if i % chunk_size == 0:
                chunk_file = f"{output_prefix}_{i // chunk_size}.csv"
                with open(chunk_file, 'w', encoding='utf-8', newline='') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(headers)  # Write headers
                    writer.writerows(chunk)  # Write rows
                print(f"Created chunk file: {chunk_file}")
                chunk = []  # Reset chunk
        
        # Write remaining rows
        if chunk:
            chunk_file = f"{output_prefix}_{(i // chunk_size) + 1}.csv"
            with open(chunk_file, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(headers)
                writer.writerows(chunk)
            print(f"Created final chunk file: {chunk_file}")

        # Log invalid rows
        if invalid_rows:
            with open(f"{output_prefix}_invalid_rows.log", 'w', encoding='utf-8') as log_file:
                for line_number, error, row in invalid_rows:
                    log_file.write(f"Line {line_number}: {error} | Row: {row}\n")
            print(f"Logged {len(invalid_rows)} invalid rows to '{output_prefix}_invalid_rows.log'.")

# Example usage
chunk_csv_with_all_columns('clean_defs.csv', 'chunk', chunk_size=10000)