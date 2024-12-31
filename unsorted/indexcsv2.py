import requests
import pandas as pd
import json

def index_csv_to_elastic(connection_details, index_name, csv_path, mapping):
    elastic_host = connection_details['host']
    username = connection_details['username']
    password = connection_details['password']

    # URL for index creation
    url_create = f"{elastic_host}/{index_name}"

    # Headers for JSON content
    headers = {'Content-Type': 'application/json'}

    # Delete the index if it exists
    requests.delete(url_create, auth=(username, password))

    # Body for index creation with mappings
    body_create = json.dumps({"mappings": mapping})

    # Create the index with the specified mapping
    requests.put(url_create, headers=headers, data=body_create, auth=(username, password))

    # Read the CSV file from the local path
    df = pd.read_csv(csv_path)

    # Iterate through the DataFrame and index each row
    for index, row in df.iterrows():
        # URL for document indexing
        url_index = f"{url_create}/_doc/{index}"
        # Body for document indexing
        body_index = json.dumps(row.to_dict())
        # Index the document
        requests.post(url_index, headers=headers, data=body_index, auth=(username, password))

    print(f"Indexed CSV from {csv_path} into {index_name} on {elastic_host}")

# Example usage
connection_details = {
    "host": "http://localhost:9200",
    "username": "elastic",
    "password": "changeme"
}
index_name = "my_index"
csv_path = "../sysco_brand_analytics.csv"
mapping = {
    "properties": {
        "brand_name": {
          "type": "keyword"
        },
        "number_of_searches": {
          "type": "long"
        },
        "submitted_keyword": {
          "type": "keyword"
        }
    }
}

index_csv_to_elastic(connection_details, index_name, csv_path, mapping)
