#!/usr/bin/env python3
import sys
from collections import defaultdict

def main():
    """
    Reducer for e-commerce sales data
    Aggregates revenue by key and calculates statistics
    """
    current_key = None
    current_sum = 0
    current_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        key, value = line.split('\t', 1)
        value = float(value)
        
        if current_key == key:
            current_sum += value
            current_count += 1
        else:
            if current_key:
                # Output aggregated results
                avg_value = current_sum / current_count if current_count > 0 else 0
                print(f"{current_key}\t{current_sum:.2f}\t{current_count}\t{avg_value:.2f}")
            
            current_key = key
            current_sum = value
            current_count = 1
    
    # Output the last group
    if current_key:
        avg_value = current_sum / current_count if current_count > 0 else 0
        print(f"{current_key}\t{current_sum:.2f}\t{current_count}\t{avg_value:.2f}")

if __name__ == "__main__":
    main()