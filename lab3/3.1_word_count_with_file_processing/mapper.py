#!/usr/bin/env python3
import sys
import re

def clean_word(word):
    # Remove punctuation and convert to lowercase
    return re.sub(r'[^\w]', '', word.lower())

for line in sys.stdin:
    try:
        line = line.strip()
        words = line.split()
        for word in words:
            clean = clean_word(word)
            if clean:  # Only emit non-empty words
                print(f"{clean}\t1")
    except Exception as e:
        # Log errors to stderr
        sys.stderr.write(f"Error processing line: {e}\n")
        continue