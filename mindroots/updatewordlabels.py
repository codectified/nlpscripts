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

# Function to update the type property of Word nodes
def update_word_node(tx, entry_id, type_value):
    query = """
    MATCH (w:Word {entry_id: $entry_id})
    SET w.type = $type_value
    """
    tx.run(query, entry_id=entry_id, type_value=type_value)

# Function to process each CSV file and update Neo4j with logging
def update_neo4j_from_csv(csv_file_path):
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with driver.session() as session:
            for i, row in enumerate(reader, start=1):
                entry_id = row['entry_id_xml']
                type_value = row['type']
                session.execute_write(update_word_node, entry_id, type_value)

                # Log progress every 1000 words
                if i % 1000 == 0:
                    print(f"Processed {i} words from '{csv_file_path}'")

    print(f"Finished processing and updating nodes from '{csv_file_path}'")

# Main function to loop through all the CSV files
def main():
    output_dir = "./"  # Directory where your mapped_output CSV files are located
    num_batches = 10  # Number of batch output files

    for batch_num in range(1, num_batches + 1):
        csv_file_path = f"{output_dir}/mapped_output_{batch_num}.csv"
        update_neo4j_from_csv(csv_file_path)

if __name__ == "__main__":
    main()

# Close the Neo4j driver
driver.close()