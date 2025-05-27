#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

def main():
    """
    Mapper for e-commerce sales data
    Emits: (category_month, price*quantity), (region_month, price*quantity), (customer_id, price*quantity)
    """
    reader = csv.reader(sys.stdin)
    next(reader)  # Skip header
    
    for row in reader:
        try:
            if len(row) < 8:
                continue
                
            transaction_id, customer_id, product_id, category, price, quantity, timestamp, region = row
            
            # Parse timestamp
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            month_year = dt.strftime('%Y-%m')
            
            # Calculate revenue
            revenue = float(price) * int(quantity)
            
            # Emit different key-value pairs for different analyses
            # 1. Category-wise monthly revenue
            print(f"category_{category}_{month_year}\t{revenue}")
            
            # 2. Region-wise monthly revenue
            print(f"region_{region}_{month_year}\t{revenue}")
            
            # 3. Customer total spending
            print(f"customer_{customer_id}\t{revenue}")
            
            # 4. Product performance
            print(f"product_{product_id}\t{revenue}")
            
        except (ValueError, IndexError) as e:
            # Skip malformed records
            continue

if __name__ == "__main__":
    main()