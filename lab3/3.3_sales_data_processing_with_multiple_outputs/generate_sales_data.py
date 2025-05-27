import random
import csv

products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse']
regions = ['North', 'South', 'East', 'West']

with open('sales_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Date', 'Product', 'Region', 'Quantity', 'Price'])
    
    for i in range(10000):
        date = f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        product = random.choice(products)
        region = random.choice(regions)
        quantity = random.randint(1, 100)
        price = random.randint(100, 2000)
        writer.writerow([date, product, region, quantity, price])