import os

# Replace '../toc.txt' with the path to your text file
text_file_path = '../toc.txt'

parent_directory = 'BookStructure'  # Name of the parent directory
current_chapter_dir = None

def sanitize_title(title):
    # Remove or replace problematic characters
    return title.replace(',', '').replace('/', ' or ').strip()

# Create the parent directory if it doesn't exist
if not os.path.exists(parent_directory):
    os.makedirs(parent_directory)
    print(f"Created parent directory: {parent_directory}")

with open(text_file_path, 'r') as file:
    for line in file:
        title = sanitize_title(line)

        # Skip empty lines
        if not title:
            continue

        if title.startswith('-'):
            # This line is a subsection
            if current_chapter_dir is None:
                print("Error: Subsection encountered before any chapter. Check the file format.")
                break

            subsection_title = sanitize_title(title[1:])  # Remove the dash and sanitize

            # Write subsection text file in the current chapter directory
            subsection_file_path = os.path.join(parent_directory, current_chapter_dir, subsection_title + '.txt')
            with open(subsection_file_path, 'w') as subsection_file:
                subsection_file.write(subsection_title)
            print(f"Created subsection file: {subsection_file_path}")

        else:
            # This line is a chapter
            chapter_title = sanitize_title(title)
            current_chapter_dir = chapter_title

            # Create chapter directory inside the parent directory
            chapter_dir_path = os.path.join(parent_directory, chapter_title)
            if not os.path.exists(chapter_dir_path):
                os.makedirs(chapter_dir_path)
                print(f"Created chapter directory: {chapter_dir_path}")

            with open(os.path.join(chapter_dir_path, chapter_title + '.txt'), 'w') as chapter_file:
                chapter_file.write(chapter_title)
            print(f"Created chapter file: {os.path.join(chapter_dir_path, chapter_title + '.txt')}")
