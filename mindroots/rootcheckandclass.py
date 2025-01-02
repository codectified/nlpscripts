from py2neo import Graph
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the Neo4j connection details from the environment
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

class RootUpdater:

    def __init__(self, uri, user, password):
        # Connect to the Neo4j database
        self.graph = Graph(uri, auth=(user, password))
        self.total_processed = 0

    def update_roots(self):
        print("Fetching root nodes from the database...")
        roots = self.graph.run("MATCH (r:Root) RETURN r.root_id AS id, r.arabic AS arabic").data()
        print(f"Found {len(roots)} root nodes to process.\n")

        for record in roots:
            root_id = record['id']
            arabic = record['arabic']
            
            # Split into letters and reverse for RTL order
            letters = arabic.split('-')[::-1]  # Reverse for RTL
            unique_letters = list(dict.fromkeys(letters))  # Remove duplicates for classification
            
            # Assign r1, r2, r3, ... based on RTL order
            r_properties = {f"r{i+1}": letter for i, letter in enumerate(letters)}
            
            # Determine the root type and its corresponding ID
            root_type, type_id_field, type_id = self.get_root_type_and_id(unique_letters)

            # Update the root node in the database
            self.graph.run(
                f"""
                MATCH (r:Root {{root_id: $root_id}})
                SET r += $properties,
                    r.root_type = $root_type,
                    r.{type_id_field} = $type_id
                """,
                root_id=root_id,
                properties=r_properties,
                root_type=root_type,
                type_id=type_id
            )

            # Log the updated properties
            updated_properties = {
                "root_id": root_id,
                "arabic": arabic,
                "r_values": r_properties,
                "root_type": root_type,
                type_id_field: type_id
            }
            print(f"Updated Root {root_id}: {updated_properties}")

            # Increment the count
            self.total_processed += 1

        print("\nProcessing completed!")

    def get_root_type_and_id(self, unique_letters):
        count = len(unique_letters)
        if count == 2:  # Geminate
            return 'Geminate', 'Geminate_ID', 25  # Example ID (assign dynamically or based on logic)
        elif count == 3:  # Triliteral
            return 'Triliteral', 'Triliteral_ID', 35  # Example ID
        elif count == 4:  # Quadriliteral
            return 'Quadriliteral', 'Quadriliteral_ID', 45  # Example ID
        elif count == 5:  # Quintiliteral
            return 'Quintiliteral', 'Quintiliteral_ID', 55  # Example ID
        elif count == 7:  # Septiliteral
            return 'Septiliteral', 'Septiliteral_ID', 75  # Example ID
        else:  # Beyond Septiliteral
            return 'Unknown', 'Unknown_ID', None

    def print_summary(self):
        print("\n--- Summary Report ---")
        print(f"Total roots processed: {self.total_processed}")
        print("----------------------\n")


# Usage
if __name__ == "__main__":
    if not uri or not user or not password:
        raise ValueError("Neo4j connection details are not set in environment variables.")

    updater = RootUpdater(uri, user, password)
    updater.update_roots()
    updater.print_summary()
    print("Root nodes have been successfully updated.")