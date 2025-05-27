#!/usr/bin/env python3
import csv
import random
from datetime import datetime, timedelta
import uuid

def generate_ecommerce_data(num_records=1000000, filename='ecommerce_data.csv'):
    """Generate synthetic e-commerce data for testing"""
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty', 'Toys']
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['transaction_id', 'customer_id', 'product_id', 'category', 
                        'price', 'quantity', 'timestamp', 'region'])
        
        start_date = datetime(2023, 1, 1)
        
        for i in range(num_records):
            transaction_id = str(uuid.uuid4())
            customer_id = f"CUST_{random.randint(1, 100000)}"
            product_id = f"PROD_{random.randint(1, 50000)}"
            category = random.choice(categories)
            price = round(random.uniform(10, 500), 2)
            quantity = random.randint(1, 5)
            timestamp = start_date + timedelta(days=random.randint(0, 365))
            region = random.choice(regions)
            
            writer.writerow([transaction_id, customer_id, product_id, category,
                           price, quantity, timestamp.strftime('%Y-%m-%d %H:%M:%S'), region])
    
    print(f"Generated {num_records} records in {filename}")

if __name__ == "__main__":
    generate_ecommerce_data(1000000)  # Generate 1M records