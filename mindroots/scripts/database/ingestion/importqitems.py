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
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0653-\u0655]')  # Full range including Shadda
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

# Function to add corpus item and link to the word in the graph
def add_corpus_item_and_link_to_word(tx, word_data, item_id, unmatched_log):
    word = word_data.get('word')
    lemma = word_data.get('lemma')
    arabic_word = word_data.get('arabic_word')
    wazn = word_data.get('wazn', '')
    pos = word_data.get('pos', '')
    gender = word_data.get('gender', '')
    number = word_data.get('number', '')
    case = word_data.get('case', '')
    prefix = word_data.get('prefix', '')
    suffix = word_data.get('suffix', '')
    sura_index = word_data.get('sura_index')
    aya_index = word_data.get('aya_index')

    # Strip diacritics from the lemma and Arabic word for matching
    lemma_no_diacritics = strip_diacritics(lemma)
    
    # Ensure lemma_no_diacritics is not empty
    if not lemma_no_diacritics:
        unmatched_log.write(f"Could not strip diacritics for lemma '{lemma}'\n")
        return

    # Use MERGE to avoid creating duplicate CorpusItem nodes
    result = tx.run("""
        MERGE (ci:CorpusItem {item_id: $item_id})
        ON CREATE SET 
            ci.arabic = $arabic_word, 
            ci.corpus_id = 2, 
            ci.lemma = $lemma, 
            ci.wazn = $wazn, 
            ci.part_of_speech = $pos, 
            ci.gender = $gender, 
            ci.number = $number, 
            ci.case = $case, 
            ci.prefix = $prefix, 
            ci.suffix = $suffix, 
            ci.sura_index = $sura_index, 
            ci.aya_index = $aya_index
        RETURN ci
        """, 
        arabic_word=arabic_word, lemma=lemma, wazn=wazn, pos=pos, gender=gender, 
        number=number, case=case, prefix=prefix, suffix=suffix,
        sura_index=sura_index, aya_index=aya_index, item_id=item_id
    )
    
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
        print(f"Linked CorpusItem for word '{word}' to Word node '{lemma}' with item_id {item_id}")
    else:
        print(f"Word node for lemma '{lemma}' not found.")
        unmatched_log.write(f"Unmatched word '{word}' with lemma '{lemma}'\n")




# Function to safely extract classification parts
def get_classification_part(index, classification_parts):
    """
    Safely extract a part from classification_parts at a given index.
    Only return the value if it's non-empty and contains at least one letter.
    """
    if len(classification_parts) > index:
        part = classification_parts[index].split(": ")
        if len(part) > 1 and any(char.isalpha() for char in part[1]):
            return part[1]
    return ''  # Return empty string if no valid value is found

def process_csv_and_import_to_neo4j(csv_file, unmatched_log, max_rows=None, batch_size=100, delay_seconds=2):
    item_id = 1  # Initialize the item ID counter
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with driver.session() as session:
            count = 0  # Track the number of processed rows
            for row in reader:
                if max_rows and count >= max_rows:
                    break

                classification_parts = row['classification'].split(",")

                word_data = {
                    'word': row['word'],
                    'lemma': get_classification_part(1, classification_parts),  # Extract lemma safely
                    'arabic_word': row['word'],  # CorpusItem Arabic label
                    'wazn': get_classification_part(2, classification_parts),  # Extract wazn safely
                    'pos': get_classification_part(3, classification_parts),  # Part of speech safely
                    'gender': get_classification_part(4, classification_parts),  # Gender safely
                    'number': get_classification_part(5, classification_parts),  # Number safely
                    'case': get_classification_part(6, classification_parts),  # Case safely
                    'prefix': get_classification_part(7, classification_parts),  # Prefix safely
                    'suffix': get_classification_part(8, classification_parts),  # Suffix safely
                    'sura_index': row['sura_index'],  # Surah index
                    'aya_index': row['aya_index']  # Ayah index
                }
                
                # Run transaction to add CorpusItem and link to Word
                try:
                    session.execute_write(add_corpus_item_and_link_to_word, word_data, item_id, unmatched_log)
                    item_id += 1  # Increment the item_id after processing each word
                except Exception as e:
                    print(f"Error processing word '{word_data['word']}': {e}")
                
                count += 1

                # Implement rate limiting
                if count % batch_size == 0:
                    print(f"Processed {count} rows, sleeping for {delay_seconds} seconds to avoid overloading the server...")
                    time.sleep(delay_seconds)
                    
    print(f"Finished processing {count} rows.")
    
def main():
    csv_file = "output_for_graphdb.csv"  # Replace with your CSV file
    
    # Open the log file to store unmatched words
    with open('unmatched_words.log', 'a') as unmatched_log:
        # Set the maximum number of rows to process for testing
        max_rows = 1000  # Adjust this to process only the first 1000 rows for testing
        
        # Call the function to process the CSV and import data into Neo4j
        process_csv_and_import_to_neo4j(csv_file, unmatched_log)

if __name__ == "__main__":
    main()