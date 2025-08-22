import re
import unicodedata
import csv
import sys
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired
import time
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the Neo4j connection details from the environment
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

# Validate that the credentials are set
if not all([uri, user, password]):
    raise ValueError("Missing Neo4j connection details. Ensure NEO4J_URI, NEO4J_USER, and NEO4J_PASS are set in your .env file.")

# Connect to Neo4j
driver = GraphDatabase.driver(uri, auth=(user, password))

# Function to match a word node, check for the Hans Wehr entry, and add it if not present
def add_hanswehr_entry(tx, arabic_no_diacritics, hanswehr_definition):
    query = """
    MATCH (w:Word {arabic_no_diacritics: $arabic_no_diacritics})
    WHERE (w.hanswehr_entry) IS NOT NULL  // Ensure we don't overwrite existing entries
    SET w.hanswehr_entry = $hanswehr_definition
    RETURN w.arabic AS word, w.hanswehr_entry AS definition
    """
    result = tx.run(query, arabic_no_diacritics=arabic_no_diacritics, hanswehr_definition=hanswehr_definition)
    return result.single()

# Function to process and add Hans Wehr entries, logging unmatched or corrupt words
def process_hanswehr_entries(hanswehr_file, unmatched_log_file):
    row_counter = 0  # Add a counter to track the progress
    
    with open(hanswehr_file, newline='') as csvfile, \
         open(unmatched_log_file, 'w', newline='') as unmatched_file:

        hanswehr_reader = csv.DictReader(csvfile)
        fieldnames = ['word', 'definition']
        unmatched_writer = csv.DictWriter(unmatched_file, fieldnames=fieldnames)

        # Write the header for the unmatched words file
        unmatched_writer.writeheader()

        with driver.session() as session:
            for row in hanswehr_reader:
                row_counter += 1  # Track which row we're processing
                hanswehr_word = row['word']
                hanswehr_definition = row['definition']

                try:
                    # Check if the word entry is more than 2 or 3 words (likely a corrupted entry)
                    if len(hanswehr_word.split()) > 3:  # Assuming max 3 words is reasonable
                        print(f"Corrupt or oversized word entry found at row {row_counter}: {hanswehr_word}")
                        unmatched_writer.writerow({'word': hanswehr_word, 'definition': hanswehr_definition})
                        continue

                    # Match and update the word in the Neo4j database
                    result = session.write_transaction(add_hanswehr_entry, hanswehr_word, hanswehr_definition)

                    if result:
                        print(f"Updated word {result['word']} with Hans Wehr definition.")
                    else:
                        print(f"No match found for Hans Wehr word: {hanswehr_word}")
                        # Log unmatched word to the CSV file
                        unmatched_writer.writerow({'word': hanswehr_word, 'definition': hanswehr_definition})

                except (ServiceUnavailable, SessionExpired) as e:
                    print(f"Connection issue: {str(e)}, retrying after 10 seconds...")
                    time.sleep(10)
                except csv.Error as csv_err:
                    print(f"CSV error at row {row_counter}: {csv_err}, skipping this row.")
                    unmatched_writer.writerow({'word': hanswehr_word, 'definition': 'corrupted data'})

# Run the process and log unmatched words
process_hanswehr_entries('hanswehr_words_cleaned.csv', 'unmatched_hanswehr_words.csv')

# Close the driver connection when done
driver.close()