import os
import json
import re
from collections import defaultdict
import pymupdf

# Define paths
DATA_DIR = "data"
UNSTRUCTURED_DIR = os.path.join(DATA_DIR, "unstructured")
OUTPUT_FILE = os.path.join(DATA_DIR, "parsed", "parsed_bios.jsonl")

def parse_unstructured_data():
    """Parses all .txt and .pdf files in the unstructured data directory."""
    user_bios = defaultdict(lambda: {"bio": "", "sources": []})

    if not os.path.exists(UNSTRUCTURED_DIR):
        print(f"Directory not found: {UNSTRUCTURED_DIR}")
        # Create directory to prevent script failure
        os.makedirs(UNSTRUCTURED_DIR)
        print(f"Created directory: {UNSTRUCTURED_DIR}")
        return 0

    user_dirs = [d for d in os.listdir(UNSTRUCTURED_DIR) if os.path.isdir(os.path.join(UNSTRUCTURED_DIR, d))]

    if not user_dirs:
        print("No user directories found in unstructured data folder.")
        return 0

    print(f"Found {len(user_dirs)} user directories to process.")

    for user_id in user_dirs:
        user_dir_path = os.path.join(UNSTRUCTURED_DIR, user_id)
        
        for filename in os.listdir(user_dir_path):
            # Skip hidden files and non-text/pdf files
            if filename.startswith('.') or not (filename.endswith('.txt') or filename.endswith('.pdf')):
                continue

            filepath = os.path.join(user_dir_path, filename)
            content = ""
            source, _ = os.path.splitext(filename)

            if filename.endswith(".txt"):
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
            elif filename.endswith(".pdf"):
                try:
                    with pymupdf.open(filepath) as doc:
                        content = "".join(page.get_text() for page in doc)
                except Exception as e:
                    print(f"Error processing PDF {filepath}: {e}")
                    continue
            
            if content:
                user_bios[user_id]['bio'] += content + "\n\n"
                user_bios[user_id]['sources'].append(source)

    # Ensure output directory exists
    output_dir = os.path.dirname(OUTPUT_FILE)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Write to a .jsonl file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for user_id, data in user_bios.items():
            record = {"user_id": user_id, "bio": data["bio"].strip(), "sources": data["sources"]}
            f.write(json.dumps(record) + '\n')
    
    return len(user_bios)

if __name__ == "__main__":
    parsed_count = parse_unstructured_data()
    
    if parsed_count > 0:
        print(f"Successfully parsed {parsed_count} user(s) and saved to {OUTPUT_FILE}")
        print("\nSample output:")
        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                print(f.readline().strip())
    else:
        print("No new files were found to parse in the unstructured directory.")
