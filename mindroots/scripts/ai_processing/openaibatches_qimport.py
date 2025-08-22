import os
import csv
import json
import time
import xml.etree.ElementTree as ET
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the OpenAI API key from the environment
api_key = os.getenv('OPENAI_API_KEY')

if not api_key:
    raise ValueError("OpenAI API key is missing. Set it in your .env file or environment variables.")

# Instantiate the OpenAI client with the API key
client = OpenAI(api_key=api_key)

# Step 1: Parse the Quranic XML file
def parse_quran_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    words_data = []
    for sura in root.findall('sura'):
        sura_index = sura.get('index')
        sura_name = sura.get('name')
        for aya in sura.findall('aya'):
            aya_index = aya.get('index')
            aya_text = aya.get('text')
            # Split the aya text into words
            words = aya_text.split()
            for word_position, word in enumerate(words, start=1):
                # Build the location string in the format sura:aya:word_position
                location = f"{sura_index}:{aya_index}:{word_position}"
                words_data.append({
                    "word": word,
                    "sura_index": sura_index,
                    "aya_index": aya_index,
                    "word_position": word_position,
                    "location": location,
                    "sura_name": sura_name
                })
    return words_data

# Step 2: Create batch input file for OpenAI Batch API
def create_batch_input_file(words_data, file_name="batchinput.jsonl", start_idx=0, batch_size=1000):
    with open(file_name, 'w', encoding='utf-8') as f:
        for idx, word_data in enumerate(words_data[start_idx:start_idx + batch_size]):
            word = word_data['word']
            batch_request = {
    "custom_id": f"request-{start_idx + idx}",
    "method": "POST",
    "url": "/v1/chat/completions",
    "body": {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": 
                "You are a helpful assistant trained to identify the grammatical nature of Arabic words. "
                "Your task is to extract important grammatical information about the word provided. "
                "Please respond with the following format: "
                "'word: <word>, lemma: <lemma>, morphological pattern (wazn): <wazn>, part of speech: <POS>, gender: <gender>, number: <number>, "
                "verb tense: <tense>, verb mood: <mood>, case: <case>, prefix: <prefix1>, suffix: <suffix1>'. "
                "For verbs, provide the 3rd person masculine singular form. For nouns, remove articles like 'ال'. "
                "For particles, separate them as prefixes or suffixes where applicable. Include the classical morphological pattern (wazn) such as 'Fa'ala' (فَعَلَ) for verbs and nouns."
            },
            {"role": "user", "content": f"Please provide the lemma, morphological pattern (wazn), and grammatical information for the following Arabic word: '{word}'."}
        ]
    }
}
            f.write(json.dumps(batch_request) + "\n")
    print(f"Batch input file '{file_name}' created with {len(words_data[start_idx:start_idx + batch_size])} entries.")

# Step 3: Upload the batch file to OpenAI
def upload_batch_file(file_name="batchinput.jsonl"):
    with open(file_name, 'rb') as f:
        batch_input_file = client.files.create(file=f, purpose="batch")
    print(f"File '{file_name}' uploaded with ID {batch_input_file.id}")
    return batch_input_file.id

# Step 4: Create the batch and process requests
def create_batch(batch_input_file_id):
    batch = client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "Processing Arabic words batch"}
    )
    print(f"Batch created with ID {batch.id}.")
    return batch.id

# Step 5: Poll for batch completion status
def poll_batch_status(batch_id):
    status = None
    while status != "completed":
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        print(f"Batch {batch_id} is currently {status}.")
        if status == "failed" or status == "cancelled":
            raise Exception(f"Batch {batch_id} failed or was cancelled.")
        time.sleep(60)  # Wait for 1 minute before checking again
    print(f"Batch {batch_id} completed.")
    return batch.output_file_id

# Step 6: Retrieve the results of the batch
def retrieve_batch_results(output_file_id, output_file_name="batch_output.jsonl"):
    result = client.files.content(output_file_id)
    with open(output_file_name, 'w', encoding='utf-8') as f:
        f.write(result.text)
    print(f"Batch results saved to '{output_file_name}'.")

# Step 7: Process the results and generate a CSV for graph database linking
def process_batch_results(output_file_name="batch_output.jsonl", words_data=None, append=False):
    csv_output = []
    with open(output_file_name, 'r', encoding='utf-8') as f:
        for line in f:
            result = json.loads(line)
            custom_id = result['custom_id']
            classification = result['response']['body']['choices'][0]['message']['content'].strip()

            # The custom_id is "request-{idx}", so we'll split by "-" and get the index
            request_index = int(custom_id.split("-")[1])

            # Retrieve the corresponding word data from words_data using the index
            word_data = words_data[request_index]

            # Output in CSV-compatible format
            csv_output.append([
                word_data['word'],  # Arabic word
                word_data['sura_index'],  # Sura index
                word_data['aya_index'],  # Aya index
                classification  # Lemma, wazn, prefixes, and suffixes classification
            ])

            # Print the Arabic word alongside its classification
            print(f"Arabic Word: {word_data['word']} | Classification: {classification}")
    
    # Write results to a CSV file for importing to the graph database
    # Open in append mode if append=True, otherwise write normally (for the first batch)
    mode = 'a' if append else 'w'
    with open('output_for_graphdb.csv', mode, newline='', encoding='utf-8') as csvfile:
        fieldnames = ['word', 'sura_index', 'aya_index', 'classification']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header only if we're writing the first batch (when mode == 'w')
        if mode == 'w':
            writer.writeheader()

        for row in csv_output:
            writer.writerow({
                'word': row[0],
                'sura_index': row[1],
                'aya_index': row[2],
                'classification': row[3]
            })

    print("Results processed and appended to output_for_graphdb.csv.")


def main():
    print("Choose an option:")
    print("1. Process existing batch results")
    print("2. Run full pipeline (send requests to OpenAI)")

    choice = input("Enter the number of the option you want to execute: ")

    if choice == "1":
        # Process existing batch results
        words_data = parse_quran_xml("quran-simple.xml")
        
        # Adjust this to handle all 16 batch files
        num_batches = 16  # Number of batch output files
        for batch_idx in range(1, num_batches + 1):
            output_file_name = f"batch_output_{batch_idx}.jsonl"
            append = batch_idx > 1  # Append after the first file
            process_batch_results(output_file_name, words_data, append=append)
    
    elif choice == "2":
        # Run the full pipeline
        words_data = parse_quran_xml("quran-simple.xml")
        total_words = len(words_data)
        
        # Set the batch size for processing
        batch_size = 5000  # Adjust based on needs
        
        # Process in batches
        for start_idx in range(0, total_words, batch_size):
            batch_file_name = f"batchinput_{start_idx//batch_size + 1}.jsonl"
            create_batch_input_file(words_data, file_name=batch_file_name, start_idx=start_idx, batch_size=batch_size)
            batch_input_file_id = upload_batch_file(file_name=batch_file_name)
            batch_id = create_batch(batch_input_file_id)
            output_file_id = poll_batch_status(batch_id)
            retrieve_batch_results(output_file_id, output_file_name=f"batch_output_{start_idx//batch_size + 1}.jsonl")
            
            # Process and append results after each batch
            append = start_idx != 0  # Only append after the first batch
            process_batch_results(output_file_name=f"batch_output_{start_idx//batch_size + 1}.jsonl", words_data=words_data, append=append)
    
    else:
        print("Invalid choice. Please run the script again.")

if __name__ == "__main__":
    main()