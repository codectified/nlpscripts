import csv

TSV_PATH = "quranic-corpus-morphology-0.4.tsv"

def scan_location_fields(path):
    valid = 0
    invalid = 0
    empty = 0
    line_number = 56  # we skip 56 lines before starting

    with open(path, "r", encoding="utf-8") as f:
        # Skip the copyright block (first 56 lines)
        for _ in range(56):
            next(f)

        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            line_number += 1
            location = row.get("LOCATION", "").strip()

            if not location:
                empty += 1
                print(f"[Empty] Line {line_number} is missing LOCATION field.")
                continue

            parts = location.strip("()").split(":")
            if len(parts) != 4:
                invalid += 1
                print(f"[Invalid] Line {line_number}: LOCATION = '{location}' → {len(parts)} parts")
            else:
                valid += 1

    print("\n--- Scan Complete ---")
    print(f"Valid LOCATION rows: {valid}")
    print(f"Invalid LOCATION rows: {invalid}")
    print(f"Empty LOCATION rows: {empty}")

if __name__ == "__main__":
    scan_location_fields(TSV_PATH)