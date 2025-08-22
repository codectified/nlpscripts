from py2neo import Graph
import re
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
graph = Graph(uri, auth=(user, password))


# Function to remove specific diacritics and the article "ال"
def refine_strip_diacritics_and_article(arabic_text):
    # Remove the article "ال" or "ٱل"
    arabic_text = re.sub(r'^(ٱ?ل)', '', arabic_text)
    
    # Remove the first two diacritics after the article, if present
    arabic_text = re.sub(r'^[ًٌٍَُِّْ]{0,2}', '', arabic_text)
    
    # Remove Shadda (ّ) if present on the first letter after article removal
    arabic_text = re.sub(r'^(ّ)', '', arabic_text)
    
    # Remove the last diacritic from the string
    arabic_text = re.sub(r'[ًٌٍَُِّْ]$', '', arabic_text)
    
    return arabic_text

# Fetch the specific corpus item with item_id = 22
corpus_item = graph.run("MATCH (item:CorpusItem {item_id: 22, corpus_id: 1}) RETURN item.item_id AS id, item.arabic AS arabic").data()

# Fetch all word nodes
word_nodes = graph.run("MATCH (word:Word) RETURN word.word_id AS id, word.arabic AS arabic").data()

# Log result to CSV
with open('corpus_item_22_refined_output.csv', 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['CorpusItemID', 'StrippedCorpusItemArabic', 'WordID', 'StrippedWordArabic', 'OriginalWordArabic', 'OriginalCorpusItemArabic'])
    
    for word_node in word_nodes:
        if corpus_item:
            original_corpus_arabic = corpus_item[0]['arabic']
            stripped_corpus_arabic = refine_strip_diacritics_and_article(original_corpus_arabic)
        
            original_word_arabic = word_node['arabic']
            stripped_word_arabic = refine_strip_diacritics_and_article(original_word_arabic)
            
            if stripped_corpus_arabic == stripped_word_arabic:
                # If match, log to CSV
                csvwriter.writerow([corpus_item[0]['id'], stripped_corpus_arabic, word_node['id'], stripped_word_arabic, original_word_arabic, original_corpus_arabic])
                print(f"Match found! Corpus Item ID: {corpus_item[0]['id']} matches Word ID: {word_node['id']}")
                print(f"Original Corpus Item Arabic: {original_corpus_arabic}")
                print(f"Original Word Arabic: {original_word_arabic}")
                print(f"Stripped Corpus Item Arabic: {stripped_corpus_arabic}")
                print(f"Stripped Word Arabic: {stripped_word_arabic}")
            else:
                print(f"No match for: {original_word_arabic}")

print("Processing complete. Results saved to corpus_item_22_refined_output.csv.")