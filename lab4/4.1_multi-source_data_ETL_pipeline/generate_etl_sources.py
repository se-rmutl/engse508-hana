#!/usr/bin/env python3
import json
import csv
import xml.etree.ElementTree as ET
import random
from datetime import datetime, timedelta
import uuid

# Generate Customer Demographics (CSV)
def generate_customer_csv():
    customers = []
    for i in range(5000):
        customer = {
            'customer_id': f'CUST_{i+1:06d}',
            'first_name': random.choice(['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank']),
            'last_name': random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']),
            'email': f'user{i+1}@email.com',
            'phone': f'+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}',
            'birth_date': (datetime(1950, 1, 1) + timedelta(days=random.randint(0, 25000))).strftime('%Y-%m-%d'),
            'gender': random.choice(['M', 'F', 'O']),
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']),
            'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA']),
            'zip_code': f'{random.randint(10000, 99999)}',
            'registration_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
            'customer_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])
        }
        customers.append(customer)
    
    with open('customers.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

# Generate Purchase History (JSON)
def generate_purchases_json():
    purchases = []
    for i in range(15000):
        purchase = {
            'transaction_id': str(uuid.uuid4()),
            'customer_id': f'CUST_{random.randint(1, 5000):06d}',
            'purchase_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1200))).isoformat(),
            'items': [
                {
                    'product_id': f'PROD_{random.randint(1, 1000):04d}',
                    'quantity': random.randint(1, 5),
                    'unit_price': round(random.uniform(10.99, 299.99), 2),
                    'discount_percent': random.choice([0, 5, 10, 15, 20])
                } for _ in range(random.randint(1, 4))
            ],
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'cash']),
            'shipping_address': {
                'street': f'{random.randint(100, 9999)} {random.choice(["Main", "Oak", "Pine", "Cedar"])} St',
                'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
                'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
                'zip_code': f'{random.randint(10000, 99999)}'
            },
            'total_amount': 0,  # Will be calculated
            'tax_amount': 0,
            'shipping_cost': round(random.uniform(5.99, 19.99), 2),
            'order_status': random.choice(['completed', 'pending', 'cancelled', 'refunded'])
        }
        
        # Calculate totals
        subtotal = sum(item['quantity'] * item['unit_price'] * (1 - item['discount_percent']/100) 
                      for item in purchase['items'])
        purchase['total_amount'] = round(subtotal + purchase['shipping_cost'], 2)
        purchase['tax_amount'] = round(subtotal * 0.08, 2)  # 8% tax
        
        purchases.append(purchase)
    
    with open('purchases.json', 'w', encoding='utf-8') as f:
        for purchase in purchases:
            f.write(json.dumps(purchase) + '\n')

# Generate Product Catalog (XML)
def generate_products_xml():
    root = ET.Element('product_catalog')
    root.set('generated_date', datetime.now().isoformat())
    root.set('version', '1.0')
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
    
    for i in range(1000):
        product = ET.SubElement(root, 'product')
        product.set('id', f'PROD_{i+1:04d}')
        
        ET.SubElement(product, 'name').text = f'Product {i+1}'
        ET.SubElement(product, 'description').text = f'High quality product in {random.choice(categories)} category'
        ET.SubElement(product, 'category').text = random.choice(categories)
        ET.SubElement(product, 'brand').text = random.choice(brands)
        ET.SubElement(product, 'price').text = str(round(random.uniform(9.99, 499.99), 2))
        ET.SubElement(product, 'cost').text = str(round(random.uniform(5.99, 299.99), 2))
        ET.SubElement(product, 'weight_kg').text = str(round(random.uniform(0.1, 10.0), 2))
        ET.SubElement(product, 'dimensions').text = f'{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)}'
        ET.SubElement(product, 'stock_quantity').text = str(random.randint(0, 1000))
        ET.SubElement(product, 'supplier_id').text = f'SUP_{random.randint(1, 50):03d}'
        ET.SubElement(product, 'created_date').text = (datetime(2019, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
        ET.SubElement(product, 'is_active').text = str(random.choice([True, False])).lower()
    
    tree = ET.ElementTree(root)
    tree.write('products.xml', encoding='utf-8', xml_declaration=True)

if __name__ == '__main__':
    print("Generating multi-format source data...")
    generate_customer_csv()
    generate_purchases_json()
    generate_products_xml()
    print("Data generation completed!")