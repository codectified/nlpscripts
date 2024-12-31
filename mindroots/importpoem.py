import time
import re
import unicodedata
from neo4j import GraphDatabase
import csv

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


# Function to strip diacritics including shadda and other common Arabic marks
def strip_diacritics(text):
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0653-\u0655]')
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

# Function to create CorpusItem node
def create_corpus_item(tx, word_data, item_id):
    result = tx.run("""
        MERGE (ci:CorpusItem {item_id: $item_id, corpus_id: $corpus_id})
        ON CREATE SET 
            ci.arabic = $arabic_word, 
            ci.lemma = $lemma, 
            ci.wazn = $wazn, 
            ci.part_of_speech = $pos, 
            ci.gender = $gender, 
            ci.number = $number, 
            ci.case = $case, 
            ci.prefix = $prefix, 
            ci.suffix = $suffix, 
            ci.line_number = $line_number, 
            ci.word_position = $word_position
        RETURN ci
        """, 
        arabic_word=word_data['arabic_word'], lemma=word_data['lemma'], wazn=word_data['wazn'], 
        pos=word_data['pos'], gender=word_data['gender'], number=word_data['number'], 
        case=word_data['case'], prefix=word_data['prefix'], suffix=word_data['suffix'], 
        line_number=word_data['line_number'], word_position=word_data['word_position'], 
        item_id=item_id, corpus_id=3  # Explicitly pass the corpus_id
    )
    node = result.single()
    if node:
        print(f"Created or retrieved CorpusItem: {node['ci']}")

# Function to link CorpusItem to Word node if a match is found
def link_to_word(tx, lemma_no_diacritics, item_id):
    word_node = tx.run("""
        MATCH (w:Word {arabic_no_diacritics: $lemma_no_diacritics})
        RETURN w
        """, lemma_no_diacritics=lemma_no_diacritics).single()
    
    if word_node:
        tx.run("""
            MATCH (ci:CorpusItem {item_id: $item_id})
            MATCH (w:Word {arabic_no_diacritics: $lemma_no_diacritics})
            MERGE (ci)-[:HAS_WORD]->(w)
            """, item_id=item_id, lemma_no_diacritics=lemma_no_diacritics)
        print(f"Linked CorpusItem with item_id {item_id} to Word node.")
    else:
        print(f"Word node for lemma '{lemma_no_diacritics}' not found for CorpusItem with item_id {item_id}.")

# Function to safely extract classification parts
def get_classification_part(index, classification_parts):
    if len(classification_parts) > index:
        part = classification_parts[index].split(": ")
        if len(part) > 1 and any(char.isalpha() for char in part[1]):
            return part[1]
    return ''

# Function to process CSV and import data to Neo4j
def process_csv_and_import_to_neo4j(csv_file, unmatched_log, max_rows=None, batch_size=100, delay_seconds=2):
    item_id = 1
    line_number_map = {}
    line_counter = 1

    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        retries = 3  # Retry up to 3 times in case of connectivity issues
        for row in reader:
            if max_rows and count >= max_rows:
                break

            classification_parts = row['classification'].split(",")
            original_line_number = int(row['line_number'])
            if original_line_number not in line_number_map:
                line_number_map[original_line_number] = line_counter
                line_counter += 1

            word_data = {
                'word': row['word'],
                'lemma': get_classification_part(1, classification_parts),
                'arabic_word': row['word'],
                'wazn': get_classification_part(2, classification_parts),
                'pos': get_classification_part(3, classification_parts),
                'gender': get_classification_part(4, classification_parts),
                'number': get_classification_part(5, classification_parts),
                'case': get_classification_part(6, classification_parts),
                'prefix': get_classification_part(7, classification_parts),
                'suffix': get_classification_part(8, classification_parts),
                'line_number': line_number_map[original_line_number],
                'word_position': row['word_position']
            }
            
            lemma_no_diacritics = strip_diacritics(word_data['lemma'])

            # Retry logic for handling connection or write issues
            for attempt in range(retries):
                try:
                    # Open session, execute corpus item creation and linking
                    with driver.session() as session:
                        session.execute_write(create_corpus_item, word_data, item_id)
                        print(f"Created CorpusItem for word '{word_data['word']}' with item_id {item_id}")
                        if lemma_no_diacritics:
                            session.execute_write(link_to_word, lemma_no_diacritics, item_id)
                    break  # If successful, exit retry loop
                except Exception as e:
                    unmatched_log.write(f"Error processing word '{word_data['word']}': {e}\n")
                    print(f"Attempt {attempt + 1} failed for word '{word_data['word']}': {e}")
                    if attempt < retries - 1:
                        time.sleep(5)  # Wait before retrying
                    else:
                        print(f"Skipping word '{word_data['word']}' after {retries} failed attempts.")

            item_id += 1
            count += 1

            if count % batch_size == 0:
                print(f"Processed {count} rows, sleeping for {delay_seconds} seconds to avoid overloading the server...")
                time.sleep(delay_seconds)
                    
    print(f"Finished processing {count} rows.")

# Main function
def main():
    csv_file = "poem_output_for_graphdb.csv"
    with open('unmatched_words.log', 'a') as unmatched_log:
        max_rows = 1000  # Process only the first 1000 rows for testing
        process_csv_and_import_to_neo4j(csv_file, unmatched_log)

# Run main
if __name__ == "__main__":
    main()