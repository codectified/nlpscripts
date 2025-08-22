#!/usr/bin/env python3
"""
Script to analyze and reconstruct Semitic roots from the SQL dump CSV files.

This script demonstrates how the radical IDs in sem_root.csv correspond to 
positions in the Arabic alphabet defined in sem_lang.csv.
"""

import csv
from typing import Dict, List, Optional

def load_arabic_alphabet() -> Dict[int, str]:
    """Load the Arabic alphabet mapping from sem_lang.csv."""
    with open('sem_lang.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['lang'] == 'Arabic':
                arabic_chars = row['script'].split(',')
                # Create mapping from index to character
                return {i: char for i, char in enumerate(arabic_chars)}
    raise ValueError("Arabic language not found in sem_lang.csv")

def reconstruct_root(rad1: int, rad2: int, rad3: int, rad4: Optional[int], 
                    alphabet: Dict[int, str]) -> str:
    """Reconstruct an Arabic root from radical IDs."""
    root_chars = []
    
    for rad_id in [rad1, rad2, rad3]:
        if rad_id in alphabet:
            root_chars.append(alphabet[rad_id])
        else:
            root_chars.append(f"?{rad_id}")
    
    # Add 4th radical if present
    if rad4 is not None and rad4 != '':
        if int(rad4) in alphabet:
            root_chars.append(alphabet[int(rad4)])
        else:
            root_chars.append(f"?{rad4}")
    
    return '-'.join(root_chars)

def analyze_roots():
    """Analyze the sem_root.csv file and reconstruct Arabic roots."""
    print("Loading Arabic alphabet...")
    alphabet = load_arabic_alphabet()
    
    print(f"Loaded {len(alphabet)} Arabic characters")
    print("First 10 characters:")
    for i in range(min(10, len(alphabet))):
        print(f"  {i}: {alphabet[i]}")
    print()
    
    print("Analyzing roots from sem_root.csv...")
    
    with open('sem_root.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        total_roots = 0
        reconstructed_roots = []
        
        for row in reader:
            total_roots += 1
            root_id = row['id']
            rad1 = int(row['rad1'])
            rad2 = int(row['rad2'])
            rad3 = int(row['rad3'])
            rad4 = row['rad4'] if row['rad4'] else None
            concept = row['concept']
            
            # Reconstruct the root
            reconstructed_root = reconstruct_root(rad1, rad2, rad3, rad4, alphabet)
            
            reconstructed_roots.append({
                'id': root_id,
                'root': reconstructed_root,
                'concept': concept,
                'rad_ids': (rad1, rad2, rad3, rad4)
            })
            
            # Print first 20 examples
            if total_roots <= 20:
                print(f"Root {root_id}: {reconstructed_root} (concept: {concept})")
                print(f"  Radical IDs: {rad1}, {rad2}, {rad3}" + 
                      (f", {rad4}" if rad4 else ""))
        
        print(f"\nTotal roots processed: {total_roots}")
        
        # Print some statistics
        trilateral_count = sum(1 for r in reconstructed_roots if r['rad_ids'][3] is None)
        quadrilateral_count = sum(1 for r in reconstructed_roots if r['rad_ids'][3] is not None)
        
        print(f"Trilateral roots: {trilateral_count}")
        print(f"Quadrilateral roots: {quadrilateral_count}")
        
        return reconstructed_roots

def verify_with_words():
    """Cross-reference roots with actual words to verify reconstruction."""
    print("\nVerifying reconstruction with actual words...")
    
    # Load some word examples
    word_examples = {}
    with open('sem_word.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            root_id = int(row['root'])
            if root_id not in word_examples:
                word_examples[root_id] = []
            if len(word_examples[root_id]) < 3:  # Keep first 3 examples per root
                word_examples[root_id].append({
                    'word': row['word'],
                    'meaning': row['meaning'],
                    'lang': row['lang']
                })
    
    # Load reconstructed roots
    alphabet = load_arabic_alphabet()
    
    print("Examples of reconstructed roots with their words:")
    with open('sem_root.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for i, row in enumerate(reader):
            if i >= 10:  # Only show first 10
                break
                
            root_id = int(row['id'])
            rad1, rad2, rad3 = int(row['rad1']), int(row['rad2']), int(row['rad3'])
            rad4 = row['rad4'] if row['rad4'] else None
            
            reconstructed_root = reconstruct_root(rad1, rad2, rad3, rad4, alphabet)
            
            print(f"\nRoot {root_id}: {reconstructed_root} (concept: {row['concept']})")
            
            if root_id in word_examples:
                for word in word_examples[root_id]:
                    lang_name = {1: 'Arabic', 2: 'Hebrew', 3: 'Sabaic'}.get(int(word['lang']), f"Lang {word['lang']}")
                    print(f"  {word['word']} ({lang_name}): {word['meaning']}")

if __name__ == "__main__":
    print("Semitic Roots Analysis")
    print("=" * 50)
    
    try:
        roots = analyze_roots()
        verify_with_words()
        
        print(f"\n✅ Successfully analyzed {len(roots)} roots!")
        print("\nThe radical IDs in sem_root.csv correspond to positions in the Arabic alphabet")
        print("as defined in sem_lang.csv. You can now index this data by reconstructing")
        print("the actual root characters from the radical ID positions.")
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Make sure you're running this script from the mindroots directory")
        print("with all the sem_*.csv files present.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")