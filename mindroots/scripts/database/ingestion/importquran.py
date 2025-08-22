import xml.etree.ElementTree as ET
import pandas as pd
from neo4j import GraphDatabase
import Levenshtein  # pip install python-Levenshtein
import time
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

# Parse the XML file
tree = ET.parse('quran-simple.xml')  # Adjusted the file name as per your input
root = tree.getroot()


# Cache for unmatched words
unmatched_words = []

# Load all Word nodes into a dictionary for quick lookup
def load_word_nodes(tx):
    query = "MATCH (w:Word) RETURN w.word_id AS word_id, w.arabic AS arabic"
    result = tx.run(query)
    word_dict = {}
    for record in result:
        word_dict[record['arabic']] = record['word_id']
    return word_dict

# Function to build a DataFrame from the XML file
def build_dataframe():
    data = []
    for sura in root.findall('sura'):
        sura_index = sura.get('index')
        for aya in sura.findall('aya'):
            aya_index = aya.get('index')
            text = aya.get('text')
            words = text.split()
            for word in words:
                data.append((sura_index, aya_index, word, text))
    return pd.DataFrame(data, columns=['sura', 'aya', 'word', 'verse'])

# Function to find a word in the cache using fuzzy matching
def find_word(word, word_dict):
    closest_word_id = None
    closest_distance = float('inf')
    threshold = 2  # Adjust this threshold based on tolerance

    for db_word, word_id in word_dict.items():
        distance = Levenshtein.distance(word, db_word)
        
        if distance < closest_distance and distance <= threshold:
            closest_distance = distance
            closest_word_id = word_id

    if closest_word_id is not None:
        print(f"Matched: '{word}' with word_id: {closest_word_id} (Distance: {closest_distance})")
        return closest_word_id
    else:
        print(f"No match found for '{word}'.")
        unmatched_words.append((word, closest_distance))
        return None

def create_corpus_item(tx, word, sura, aya, item_id):
    query = """
    MERGE (ci:CorpusItem {item_id: $item_id, arabic: $word, corpus_id: 2, sura: $sura, aya: $aya})
    RETURN ci
    """
    result = tx.run(query, word=word, sura=sura, aya=aya, item_id=item_id)
    record = result.single()

    if record:
        return record["ci"]
    else:
        print(f"Debug Info - No record returned for corpus item creation: word='{word}', sura='{sura}', aya='{aya}', item_id='{item_id}'")
        raise ValueError(f"Failed to create the corpus item for word '{word}' in Sura {sura}, Aya {aya}.")

def link_corpus_item_to_word(tx, item_id, word_id):
    query = """
    MATCH (ci:CorpusItem {item_id: $item_id})
    MATCH (w:Word {word_id: $word_id})
    MERGE (ci)-[:HAS_WORD]->(w)
    """
    result = tx.run(query, item_id=item_id, word_id=word_id)
    summary = result.consume()
    if summary and hasattr(summary.counters, 'relationships_created'):
        if summary.counters.relationships_created == 0:
            print(f"Warning: No relationship was created for corpus item with item_id '{item_id}' and word with word_id '{word_id}'.")

def log_unmatched_words_to_csv(filename='unmatched_words.csv'):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['word', 'closest_distance'])
        writer.writerows(unmatched_words)

def process_dataframe(driver, df):
    global item_counter  # Use the global variable

    # Load all Word nodes into memory
    with driver.session() as session:
        word_dict = session.read_transaction(load_word_nodes)

    for _, row in df.iterrows():
        sura = row['sura']
        aya = row['aya']
        word = row['word']
        verse_text = row['verse']

        with driver.session() as session:
            # Find the word node and get its word_id
            word_id = find_word(word, word_dict)

            # Create the corpus item
            try:
                corpus_item = session.write_transaction(create_corpus_item, word, sura, aya, item_counter)
                item_id = corpus_item['item_id']  # Access the item_id directly from the properties
                item_counter += 1

                # If a word_id is found, link the corpus item to the word node
                if word_id:
                    session.write_transaction(link_corpus_item_to_word, item_id, word_id)

                time.sleep(0.1)  # Rate limiting
            except ValueError as e:
                print(e)

# Initialize item_counter
item_counter = 1

# Build the DataFrame and process it
df = build_dataframe()
process_dataframe(driver, df)

# Log unmatched words to a CSV file
log_unmatched_words_to_csv()

# Close the driver after use
driver.close()