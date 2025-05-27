#!/usr/bin/env python3
import sys

current_key = None
current_sum = 0

for line in sys.stdin:
    try:
        line = line.strip()
        key, value = line.split('\t')
        value = int(value)
        
        if current_key == key:
            current_sum += value
        else:
            if current_key:
                category, name = current_key.split('_', 1)
                print(f"{category}\t{name}\t{current_sum}")
            current_key = key
            current_sum = value
    except:
        continue

if current_key:
    category, name = current_key.split('_', 1)
    print(f"{category}\t{name}\t{current_sum}")