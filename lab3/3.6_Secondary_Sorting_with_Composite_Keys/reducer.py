#!/usr/bin/env python3
import sys
from datetime import datetime

current_key = None
daily_prices = []

def process_daily_data(key, prices):
    if not prices:
        return
    
    symbol, date = key.split('_')
    prices.sort(key=lambda x: x[0])  # Sort by time
    
    high_price = max(float(p[1]) for p in prices)
    low_price = min(float(p[1]) for p in prices)
    open_price = float(prices[0][1])  # First price of day
    close_price = float(prices[-1][1])  # Last price of day
    total_volume = sum(int(p[2]) for p in prices)
    
    print(f"{symbol}\t{date}\t{open_price}\t{high_price}\t{low_price}\t{close_price}\t{total_volume}")

for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split('\t')
        key = parts[0]
        time = parts[1]
        price = parts[2]
        volume = parts[3]
        
        if current_key == key:
            daily_prices.append((time, price, volume))
        else:
            if current_key:
                process_daily_data(current_key, daily_prices)
            current_key = key
            daily_prices = [(time, price, volume)]
    except:
        continue

if current_key:
    process_daily_data(current_key, daily_prices)