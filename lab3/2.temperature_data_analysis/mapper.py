#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        date, temperature = line.split('\t')
        year = date.split('-')[0]
        print(f"{year}\t{temperature}")
    except ValueError:
        continue