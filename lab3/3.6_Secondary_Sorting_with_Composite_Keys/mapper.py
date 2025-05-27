#!/usr/bin/env python3
import sys
import csv

for line in sys.stdin:
    try:
        row = next(csv.reader([line.strip()]))
        if len(row) == 5 and row[0] != 'date':
            date, symbol, time, price, volume = row
            
            # Create composite key: symbol_date, and natural key: time
            # This allows sorting by symbol, then date, then time
            composite_key = f"{symbol}_{date}"
            print(f"{composite_key}\t{time}\t{price}\t{volume}")
    except:
        continue