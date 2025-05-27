#!/usr/bin/env python3
import sys
from collections import defaultdict

word_counts = defaultdict(int)

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue
        word, count = line.split('\t')
        word_counts[word] += int(count)
    except ValueError:
        sys.stderr.write(f"Invalid line format: {line}\n")
        continue

# Output sorted results
for word in sorted(word_counts.keys()):
    print(f"{word}\t{word_counts[word]}")