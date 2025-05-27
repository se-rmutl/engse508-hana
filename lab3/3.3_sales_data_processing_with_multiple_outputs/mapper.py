#!/usr/bin/env python3
import sys
import csv

# Skip header
header_skipped = False

for line in sys.stdin:
    if not header_skipped:
        header_skipped = True
        continue
    
    try:
        row = next(csv.reader([line.strip()]))
        if len(row) == 5:
            date, product, region, quantity, price = row
            total_sale = int(quantity) * int(price)
            
            # Emit region-based aggregation
            print(f"region_{region}\t{total_sale}")
            
            # Emit product-based aggregation
            print(f"product_{product}\t{total_sale}")
    except:
        continue