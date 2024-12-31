import csv
import xml.etree.ElementTree as ET

def extract_properties_from_xml(xml_string):
    try:
        # Parse the XML string
        root = ET.fromstring(xml_string)
        
        entry_id = root.attrib.get('id', '')
        key = root.attrib.get('key', '')
        entry_type = root.attrib.get('type', '')
        
        forms = []
        definitions = []
        
        for form in root.findall(".//form/orth"):
            forms.append(form.text.strip() if form.text else "")
        
        for definition in root.itertext():
            definitions.append(definition.strip() if definition else "")
        
        # Filter out None or empty values before joining
        forms = [f for f in forms if f]
        definitions = [d for d in definitions if d]
        
        return entry_id, key, entry_type, "; ".join(forms), "; ".join(definitions)
    
    except ET.ParseError:
        return None, None, None, None, None  # Return None values in case of a parse error

def process_csv(input_csv, output_csv):
    with open(input_csv, 'r', encoding='utf-8') as infile, open(output_csv, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ['word', 'entry_id_xml', 'key_xml', 'type_xml', 'forms_xml', 'definitions_xml']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in reader:
            # Extract the properties from the XML
            entry_id_xml, key_xml, type_xml, forms_xml, definitions_xml = extract_properties_from_xml(row['xml'])
            
            # Write the extracted data to the output CSV
            writer.writerow({
                'word': row['word'],
                'entry_id_xml': entry_id_xml,
                'key_xml': key_xml,
                'type_xml': type_xml,
                'forms_xml': forms_xml,
                'definitions_xml': definitions_xml
            })

# Usage
process_csv('entry.csv', 'output_with_extracted_columns.csv')

print("CSV processing complete. The word and extracted XML data have been saved to the output CSV.")