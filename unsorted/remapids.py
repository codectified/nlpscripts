import csv

def preprocess_root(root):
    # Add dashes between each letter
    r1, r2, r3 = list(root)
    return f"{r1}-{r2}-{r3}", r1, r2, r3

with open('new_roots.csv', 'r', encoding='utf-8') as infile, open('processed_new_roots.csv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    writer.writerow(['id', 'الجذور', 'r3', 'r2', 'r1'])  # Write headers
    
    for row in reader:
        if row[0] == 'id':  # Skip the header
            continue
        processed_root, r1, r2, r3 = preprocess_root(row[1])
        writer.writerow([row[0], processed_root, r3, r2, r1])

print("Processed new roots CSV.")