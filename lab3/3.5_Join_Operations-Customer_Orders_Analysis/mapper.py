#!/usr/bin/env python3
import sys
import csv
import os

filename = os.environ.get('mapreduce_map_input_file', '')

for line in sys.stdin:
    try:
        row = next(csv.reader([line.strip()]))
        
        if 'customer' in filename.lower():
            # Customer data: customer_id, name, tier, country
            if len(row) == 4 and row[0] != 'customer_id':
                customer_id, name, tier, country = row
                print(f"{customer_id}\tcustomer\t{name}\t{tier}\t{country}")
        
        elif 'order' in filename.lower():
            # Order data: order_id, customer_id, amount, date
            if len(row) == 4 and row[0] != 'order_id':
                order_id, customer_id, amount, date = row
                print(f"{customer_id}\torder\t{order_id}\t{amount}\t{date}")
    except:
        continue