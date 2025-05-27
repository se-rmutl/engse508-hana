#!/usr/bin/env python3
import sys
import re
import string

def clean_text(text):
    # Remove punctuation and convert to lowercase
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

def generate_ngrams(words, n):
    ngrams = []
    for i in range(len(words) - n + 1):
        ngram = ' '.join(words[i:i+n])
        ngrams.append(ngram)
    return ngrams

# Configure n-gram size (can be passed as parameter)
N = 3  # trigrams

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue
            
        # Clean and tokenize
        clean_line = clean_text(line)
        words = clean_line.split()
        
        if len(words) >= N:
            ngrams = generate_ngrams(words, N)
            for ngram in ngrams:
                print(f"{ngram}\t1")
    except:
        continue