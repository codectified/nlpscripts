import os
import json
import datetime
from elasticsearch import Elasticsearch

def create_index_with_mapping(es, index_name):
    mapping = {
        "mappings": {
            "properties": {
                "conversation_start": {
                    "type": "date"
                },
                "messages": {
                    "type": "nested",
                    "properties": {
                        "content": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "timestamp_ms": {
                            "type": "long"
                        },
                        "sender_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "participants": {
                    "properties": {
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)

def index_conversations(base_url, index_name, username, password, conversations):
    es = Elasticsearch([base_url], http_auth=(username, password), verify_certs=False, ssl_show_warn=False)
    
    # Create index with mapping
    if not es.indices.exists(index=index_name):
        create_index_with_mapping(es, index_name)

    for index, (conversation_id, conversation_data) in enumerate(conversations.items()):

        doc_id = index + 1

        participant_names = [participant["name"] for participant in conversation_data["participants"]]
        conversation_start = min(message["timestamp_ms"] for message in conversation_data["messages"])
        conversation_start_dt = datetime.datetime.fromtimestamp(conversation_start / 1000.0)
        conversation = {
            "participants": conversation_data["participants"],
            "messages": conversation_data["messages"],
            "conversation_start": conversation_start_dt.isoformat()
        }

        for message in conversation["messages"]:
            timestamp_ms = message["timestamp_ms"]
            timestamp_dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0)
            message["year"] = timestamp_dt.year
            message["month"] = timestamp_dt.month
            message["day"] = timestamp_dt.day
            message["hour"] = timestamp_dt.hour
            message["day_of_week"] = timestamp_dt.strftime("%A")

        index_data = {
            "index": index_name,
            "id": doc_id,
            "body": conversation
        }

        response = es.index(**index_data)

        if response["result"] == "created":
            print(f"Document indexed successfully: Conversation {doc_id}")
        else:
            print(f"Failed to index document: Conversation {doc_id}")
            print(response)

def main():
    index_name = "fb-messages"
    base_url = "https://localhost:9200"
    username = "elastic"
    password = "changeme"
    json_directory = "../fb/fb-jsons"

    json_files = [os.path.join(json_directory, file) for file in os.listdir(json_directory) if file.endswith(".json")]

    conversations = {}

    for json_file in json_files:
        with open(json_file, "r") as file:
            data = json.load(file)

        if "participants" not in data:
            continue

        participants = data["participants"]
        messages = data["messages"]

        participant_names = [participant["name"] for participant in participants]
        conversation_id = ",".join(sorted(participant_names))

        if conversation_id in conversations:
            conversations[conversation_id]["messages"].extend(messages)
        else:
            conversations[conversation_id] = {
                "participants": participants,
                "messages": messages
            }

    index_conversations(base_url, index_name, username, password, conversations)

if __name__ == "__main__":
    main()
