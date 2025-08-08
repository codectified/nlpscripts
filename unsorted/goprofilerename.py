import os
import re

def rename_gopro_videos(directory):
    if not os.path.exists(directory):
        print(f"❌ Error: Directory '{directory}' not found.")
        return

    print(f"📂 Scanning directory: {directory}\n")

    # **Regex pattern to match GX[XX][YYYYY].MP4**
    pattern = re.compile(r"^GX(\d{2})(\d{5})\.MP4$", re.IGNORECASE)

    grouped_files = {}

    print("📜 Raw Files Found:")
    files = os.listdir(directory)
    
    for f in files:
        # **Print file in HEX to detect hidden characters**
        hex_name = " ".join(hex(ord(c)) for c in f)
        print(f"  - [{f}] (HEX: {hex_name})")

    print("\n🔍 Matching files:")
    for filename in files:
        cleaned_filename = filename.strip().replace("\r", "").replace("\n", "")

        # **Convert to lowercase before regex matching**
        cleaned_filename = cleaned_filename.upper()

        match = pattern.match(cleaned_filename)
        
        if match:
            prefix, session = match.groups()
            session_number = int(session)
            if session_number not in grouped_files:
                grouped_files[session_number] = []
            grouped_files[session_number].append((prefix, filename))
            print(f"  ✅ Matched: [{filename}] → Prefix: {prefix}, Session: {session_number}")
        else:
            print(f"  ❌ Not Matched: [{filename}] (Cleaned: [{cleaned_filename}])")

    if not grouped_files:
        print("\n⚠️ No files matched. Check filename structure.")
        return

    print("\n📂 Renaming files...\n")

    for session_number, files in sorted(grouped_files.items()):
        files.sort()
        for part, (prefix, filename) in enumerate(files, start=1):
            new_filename = f"Game {session_number} Part {part}.mp4"
            old_path = os.path.join(directory, filename)
            new_path = os.path.join(directory, new_filename)

            print(f"🔄 Renaming: [{filename}] → [{new_filename}]")
            os.rename(old_path, new_path)

    print("\n✅ Renaming complete!")

# **Run the script**
rename_gopro_videos("/Volumes/Untitled/DCIM/100GOPRO")