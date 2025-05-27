#!/usr/bin/env python3
import sys

# Minimum frequency threshold
MIN_FREQUENCY = 5

current_ngram = None
current_count = 0

for line in sys.stdin:
    try:
        line = line.strip()
        ngram, count = line.split('\t')
        count = int(count)
        
        if current_ngram == ngram:
            current_count += count
        else:
            if current_ngram and current_count >= MIN_FREQUENCY:
                print(f"{current_ngram}\t{current_count}")
            current_ngram = ngram
            current_count = count
    except:
        continue

if current_ngram and current_count >= MIN_FREQUENCY:
    print(f"{current_ngram}\t{current_count}")