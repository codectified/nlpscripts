import pandas as pd

# Path to your full CSV file
input_csv = "entry.csv"  # ← replace this with your actual file path
output_csv = "lanes_filtered.csv"

# Columns you want to extract
columns = [
    "id",
    "root",
    "broot",
    "word",
    "bword",
    "itype",
    "nodeid",
    "bareword",
    "headword"
]

# Load and extract the specified columns
df = pd.read_csv(input_csv, usecols=columns)

# Write to a new CSV file
df.to_csv(output_csv, index=False)

print(f"Filtered CSV written to {output_csv}")