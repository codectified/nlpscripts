import csv
import os
import time  # Import time for pausing

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

def update_word_classifications(tx, classifications, pause_after=500, pause_duration=10):
    """
    Update Neo4j Word nodes with classification and subclass properties, pausing after every N rows.

    Args:
    - tx: Neo4j transaction.
    - classifications: A dictionary mapping words to their classification and subclass.
      Example: { "word1": {"classification": "Concrete", "subclass": "MAA"}, ... }
    - pause_after: Number of rows to process before pausing.
    - pause_duration: Time in seconds to pause after processing pause_after rows.
    """
    query = """
    MATCH (w:Word)
    WHERE w.arabic = $word
    SET w.classification = $classification_value, w.subclass = $subclass_value
    RETURN w.arabic AS word, w.classification AS classification, w.subclass AS subclass
    """
    counter = 0

    for word, properties in classifications.items():
        classification_value = properties.get("classification")
        subclass_value = properties.get("subclass")
        tx.run(query, word=word, classification_value=classification_value, subclass_value=subclass_value)
        print(f"Updated Word: {word}, Classification: {classification_value}, Subclass: {subclass_value}")

        # Increment counter and check if a pause is needed
        counter += 1
        if counter % pause_after == 0:
            print(f"Processed {counter} rows. Pausing for {pause_duration} seconds...")
            time.sleep(pause_duration)


def read_classifications_from_mapped_files(mapped_dir, num_files):
    """
    Read word classifications from multiple mapped output files.

    Args:
    - mapped_dir: Directory containing mapped output files.
    - num_files: Number of mapped output files to process.

    Returns:
    - A dictionary mapping words to their classification and subclass.
    """
    classifications = {}
    for i in range(1, num_files + 1):
        mapped_file = os.path.join(mapped_dir, f"mapped_output_{i}.csv")
        with open(mapped_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                word = row['word']
                type_field = row['type']
                
                # Split the 'type' field into classification and subclass
                classification_value, subclass_value = type_field.split("; ")
                classifications[word] = {
                    "classification": classification_value if classification_value != "NA" else None,
                    "subclass": subclass_value if subclass_value != "NA" else None
                }
    return classifications


# Main function
mapped_dir = "./"  # Directory containing the mapped output files
num_files = 10  # Number of mapped output files to process

# Read classifications from all mapped output files
classifications = read_classifications_from_mapped_files(mapped_dir, num_files)

# Update the Neo4j database with pausing
pause_after = 500  # Pause after every 500 rows
pause_duration = 10  # Pause duration in seconds

with driver.session() as session:
    session.execute_write(update_word_classifications, classifications, pause_after, pause_duration)

driver.close()