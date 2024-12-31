import re
import unicodedata
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

# Function to strip Arabic diacritics from a string
def strip_diacritics(text):
    arabic_diacritics = re.compile(r'[\u064B-\u0652]')  # Arabic diacritics range
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)


# Function to update each word node with the stripped diacritic property
def update_word_node(tx, word_id, arabic_no_diacritics):
    query = """
    MATCH (w:Word {word_id: $word_id})
    SET w.arabic_no_diacritics = $arabic_no_diacritics
    RETURN w.arabic AS original, w.arabic_no_diacritics AS stripped
    """
    result = tx.run(query, word_id=word_id, arabic_no_diacritics=arabic_no_diacritics)
    return result.single()

# Function to process and update word nodes in batches
def process_words(batch_size=500, sleep_duration=10):
    start_from = 0  # Keeps track of the node where the script left off

    while True:
        try:
            with driver.session() as session:
                # Fetch word nodes in batches, skipping already processed nodes
                result = session.run("""
                    MATCH (w:Word) 
                    WHERE w.arabic IS NOT NULL AND w.arabic_no_diacritics IS NULL 
                    RETURN w.word_id AS word_id, w.arabic AS arabic
                    SKIP $start_from LIMIT $batch_size
                """, start_from=start_from, batch_size=batch_size)

                count = 0  # Keep track of how many nodes were processed in the current batch
                
                for record in result:
                    word_id = record['word_id']
                    arabic_word = record['arabic']
                    
                    # Strip the diacritics from the arabic property
                    stripped_word = strip_diacritics(arabic_word)
                    
                    # Update the word node with the new property arabic_no_diacritics
                    updated_node = session.write_transaction(update_word_node, word_id, stripped_word)
                    
                    print(f"Updated node {word_id}: {updated_node['original']} -> {updated_node['stripped']}")
                    count += 1

                # Update the start_from counter
                start_from += count

                # If no more records were processed, we are done
                if count == 0:
                    print("All nodes processed.")
                    break

                # Sleep for a while before processing the next batch to avoid rate-limiting issues
                print(f"Processed {count} nodes, sleeping for {sleep_duration} seconds...")
                time.sleep(sleep_duration)
        
        except (ServiceUnavailable, SessionExpired) as e:
            print(f"Connection dropped, retrying... {str(e)}")
            time.sleep(10)  # Wait before retrying the connection

# Run the process
process_words(batch_size=500, sleep_duration=10)  # Process 500 nodes at a time, with a 10-second delay between batches

# Close the driver connection when done
driver.close()