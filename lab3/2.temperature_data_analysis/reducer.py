#!/usr/bin/env python3
import sys

current_year = None
max_temp = float('-inf')

for line in sys.stdin:
    try:
        line = line.strip()
        year, temp = line.split('\t')
        temp = int(temp)
        
        if current_year == year:
            max_temp = max(max_temp, temp)
        else:
            if current_year:
                print(f"{current_year}\t{max_temp}")
            current_year = year
            max_temp = temp
    except ValueError:
        continue

if current_year:
    print(f"{current_year}\t{max_temp}")