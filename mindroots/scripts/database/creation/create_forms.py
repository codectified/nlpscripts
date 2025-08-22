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

# Test mode flag
test_mode = False  # Set to True for testing, False for actual database updates

# Form mapping dictionary
form_mapping = {
    "المصدر": 1,
    "اسم_الفاعل": 2,
    "اسم_المفعول": 3,
    "اسم_المكان": 4,
    "اسم_الحال": 5,
    "اسم_الآلة": 6,
    "اسم_ذات": 7,
    "اسم_المبالغة": 8,
    "أسم_العلة": 9
}

def update_word_nodes_in_batch(tx, batch):
    if test_mode:
        print(f"Test Mode: Executing a batch of {len(batch)} queries")
    for entry_id, form_id in batch:
        query = f"""
        MATCH (w:Word {{entry_id: '{entry_id}'}})
        MATCH (f:Form {{form_id: {form_id}}})
        MERGE (w)-[:HAS_FORM]->(f)
        """
        if test_mode:
            print(query)
        else:
            try:
                tx.run(query)
            except Exception as e:
                print(f"Error executing query for word_id {entry_id} and form_id {form_id}: {e}")


# Function to process each CSV file and update Neo4j with batching and logging
def update_neo4j_from_csv(csv_file_path, batch_size=100):
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        batch = []
        with driver.session() as session:
            for i, row in enumerate(reader, start=1):
                entry_id = row['entry_id_xml']
                type_value = row['type'].replace(' ', '_')
                form_id = form_mapping.get(type_value)

                if form_id:
                    batch.append((entry_id, form_id))

                if len(batch) >= batch_size:
                    print(f"Processing batch of {len(batch)} items at record {i}")
                    session.execute_write(update_word_nodes_in_batch, batch)
                    batch = []

                # Log progress every 1000 words
                if i % 1000 == 0:
                    print(f"Processed {i} words from '{csv_file_path}'")

            # Process remaining batch
            if batch:
                print(f"Processing final batch of {len(batch)} items")
                session.execute_write(update_word_nodes_in_batch, batch)

    print(f"Finished processing and updating nodes from '{csv_file_path}'")

# Main function to loop through all the CSV files
def main():
    output_dir = "./"  # Directory where your mapped_output CSV files are located
    num_batches = 48  # Number of batch output files

    for batch_num in range(1, num_batches + 1):
        csv_file_path = f"{output_dir}/mapped_output_{batch_num}.csv"
        print(f"Starting processing for file: {csv_file_path}")
        update_neo4j_from_csv(csv_file_path)

if __name__ == "__main__":
    main()

# Close the Neo4j driver
driver.close()