def remove_corrupt_lines(input_file, output_file, ranges_to_remove):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        line_number = 0
        for line in infile:
            line_number += 1
            # Skip lines that fall within the corrupt ranges
            if any(start <= line_number <= end for start, end in ranges_to_remove):
                continue
            outfile.write(line)

# Specify the ranges of corrupted lines
corrupted_ranges = [(10539, 299235), (299732, 373200)]

# Run the function to clean the file
remove_corrupt_lines('hanswehr_words.csv', 'hanswehr_words_cleaned.csv', corrupted_ranges)

print("Corrupt lines removed. Cleaned file saved as 'hanswehr_words_cleaned.csv'")