import csv
from neo4j import GraphDatabase
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

# Function to update the new English label while preserving the old one
def update_word_node(tx, entry_id, english_new, spanish, urdu, transliteration):
    query = """
    MATCH (w:Word {entry_id: $entry_id})
    SET w.english_2 = $english_new,
        w.spanish = $spanish,
        w.urdu = $urdu,
        w.transliteration = $transliteration
    """
    tx.run(query, entry_id=entry_id, english_new=english_new, 
           spanish=spanish, urdu=urdu, transliteration=transliteration)

# Function to process **final_translations.csv** and update Neo4j with logging
def update_neo4j_from_csv(csv_file_path):
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with driver.session() as session:
            for i, row in enumerate(reader, start=1):
                entry_id = row['entry_id_xml']
                english_new = row['english']
                spanish = row['spanish']
                urdu = row['urdu']
                transliteration = row['transliteration']

                session.execute_write(update_word_node, entry_id, english_new, spanish, urdu, transliteration)

                # Log progress every 1,000 words
                if i % 1000 == 0:
                    print(f"Processed {i} words from '{csv_file_path}'")

    print(f"Finished processing and updating nodes from '{csv_file_path}'")

# Main function to process final_translations.csv
def main():
    final_csv = "final_translations.csv"  # Single output file from batch processing
    update_neo4j_from_csv(final_csv)

if __name__ == "__main__":
    main()

# Close the Neo4j driver
driver.close()