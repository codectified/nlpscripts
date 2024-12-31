import os
import markdownify
from bs4 import BeautifulSoup

def convert_html_to_markdown(html_content):
    return markdownify.markdownify(html_content, heading_style="ATX")

def save_markdown_file(title, content, output_dir):
    filename = f"{title.replace(' ', '_').replace('/', '_')}.md"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(content)

def main(input_xml, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(input_xml, 'r', encoding='utf-8') as file:
        content = file.read()
        soup = BeautifulSoup(content, 'xml')
        items = soup.find_all('item')

        for item in items:
            title = item.find('title').text
            description = item.find('encoded').text
            markdown_content = convert_html_to_markdown(description)
            save_markdown_file(title, markdown_content, output_dir)

if __name__ == "__main__":
    input_xml = 'wp_posts.xml'  # Replace with your XML file path
    output_dir = 'output'  # Replace with your desired output directory
    main(input_xml, output_dir)
