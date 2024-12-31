import os
import pandas as pd
import ast
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

def index_csv_to_elastic(connection_details, index_name, csv_path, mapping):
    elastic_host = connection_details['host']
    username = connection_details['username']
    password = connection_details['password']
    verify_ssl = connection_details['verify_ssl']
    ssl_show_warn= connection_details['ssl_warning']

    # Create Elasticsearch client
    es = Elasticsearch(hosts=[elastic_host], basic_auth=(username, password), verify_certs=verify_ssl, ssl_show_warn=ssl_show_warn)

    # Delete the index if it exists
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    # Create the index with the specified mapping
    es.indices.create(index=index_name, mappings=mapping)

    # Read the CSV file from the local path
    df = pd.read_csv(csv_path)

    # Iterate through the DataFrame and index each row
    for index, row in df.iterrows():
        # Index the document
        es.index(index=index_name, document = row.to_dict(), id=index)

    print(f"Indexed CSV from {csv_path} into {index_name} on {elastic_host}")


load_dotenv() # Load environment variables from .env file

connection_details = {
    "host": os.getenv("ELASTIC_HOST"),
    "username": os.getenv("ELASTIC_USERNAME"),
    "password": os.getenv("ELASTIC_PASSWORD"),
    "verify_ssl": ast.literal_eval(os.getenv("VERIFY_SSL")),
    "ssl_warning": ast.literal_eval(os.getenv("SSL_SHOW_WARNING"))
}

csv_path = os.getenv("CSV_PATH")
index_name = "sysco_brand_analytics"

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
