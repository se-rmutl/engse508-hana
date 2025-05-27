# LAB4: Advanced Hadoop MapReduce ETL Labs
Extract, Transform, Load with Real-World Data Integration

## 4.1 Multi-Source Data ETL Pipeline
Difficulty: Advanced
Time: 3-4 hours
Focus: Extract data from multiple sources, transform formats, and load into unified schema

### Objective
Build a comprehensive ETL pipeline that processes data from multiple sources (CSV, JSON, XML), performs data cleansing and transformation, and outputs standardized records for analytics.

### Business Scenario
A retail company needs to integrate customer data from:

Legacy CSV files (customer demographics)
Modern JSON API responses (purchase history)
XML supplier feeds (product information)

Step-by-Step Instructions

### Generate Multi-Format Source Data: generate_etl_sources.py:
```
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
```

### ETL Mapper - Extract and Transform (mapper.py):
```
#!/usr/bin/env python3
import sys
import json
import csv
import xml.etree.ElementTree as ET
import os
import re
from datetime import datetime
from io import StringIO

def clean_phone(phone):
    """Standardize phone number format"""
    digits = re.sub(r'[^\d]', '', phone)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return phone

def clean_email(email):
    """Validate and clean email"""
    email = email.lower().strip()
    if '@' in email and '.' in email.split('@')[1]:
        return email
    return None

def standardize_date(date_str):
    """Convert various date formats to YYYY-MM-DD"""
    try:
        # Handle ISO format
        if 'T' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '')).strftime('%Y-%m-%d')
        # Handle standard date format
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    except:
        return None

def get_file_type():
    """Determine file type from input filename"""
    filename = os.environ.get('mapreduce_map_input_file', '').lower()
    if 'customer' in filename or filename.endswith('.csv'):
        return 'csv'
    elif 'purchase' in filename or filename.endswith('.json'):
        return 'json'
    elif 'product' in filename or filename.endswith('.xml'):
        return 'xml'
    return 'unknown'

def process_csv_line(line):
    """Process customer CSV data"""
    try:
        reader = csv.DictReader(StringIO(line))
        row = next(reader)
        
        # Transform and validate customer data
        customer_record = {
            'record_type': 'customer',
            'customer_id': row.get('customer_id', '').strip(),
            'full_name': f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
            'email': clean_email(row.get('email', '')),
            'phone': clean_phone(row.get('phone', '')),
            'birth_date': standardize_date(row.get('birth_date', '')),
            'gender': row.get('gender', '').upper(),
            'address': f"{row.get('city', '')}, {row.get('state', '')} {row.get('zip_code', '')}",
            'registration_date': standardize_date(row.get('registration_date', '')),
            'customer_tier': row.get('customer_tier', '').title(),
            'data_quality_score': 0
        }
        
        # Calculate data quality score
        quality_score = 0
        if customer_record['customer_id']: quality_score += 20
        if customer_record['email']: quality_score += 20
        if customer_record['phone']: quality_score += 15
        if customer_record['birth_date']: quality_score += 15
        if customer_record['gender'] in ['M', 'F', 'O']: quality_score += 10
        if customer_record['address'].count(',') == 1: quality_score += 10
        if customer_record['registration_date']: quality_score += 10
        
        customer_record['data_quality_score'] = quality_score
        
        if quality_score >= 60:  # Only emit high-quality records
            output = '\t'.join([str(customer_record.get(k, '')) for k in customer_record.keys()])
            print(f"CUSTOMER\t{customer_record['customer_id']}\t{output}")
            
    except Exception as e:
        sys.stderr.write(f"Error processing CSV line: {str(e)}\n")

def process_json_line(line):
    """Process purchase JSON data"""
    try:
        purchase = json.loads(line.strip())
        
        # Extract and transform purchase data
        customer_id = purchase.get('customer_id', '')
        purchase_date = standardize_date(purchase.get('purchase_date', ''))
        
        for item in purchase.get('items', []):
            purchase_record = {
                'record_type': 'purchase',
                'transaction_id': purchase.get('transaction_id', ''),
                'customer_id': customer_id,
                'purchase_date': purchase_date,
                'product_id': item.get('product_id', ''),
                'quantity': item.get('quantity', 0),
                'unit_price': item.get('unit_price', 0),
                'discount_percent': item.get('discount_percent', 0),
                'line_total': round(item.get('quantity', 0) * item.get('unit_price', 0) * 
                                 (1 - item.get('discount_percent', 0)/100), 2),
                'payment_method': purchase.get('payment_method', ''),
                'order_status': purchase.get('order_status', ''),
                'shipping_cost': purchase.get('shipping_cost', 0),
                'tax_amount': purchase.get('tax_amount', 0)
            }
            
            # Data validation
            if (purchase_record['customer_id'] and 
                purchase_record['product_id'] and 
                purchase_record['quantity'] > 0 and
                purchase_record['unit_price'] > 0):
                
                output = '\t'.join([str(purchase_record.get(k, '')) for k in purchase_record.keys()])
                print(f"PURCHASE\t{purchase_record['customer_id']}\t{output}")
                
    except Exception as e:
        sys.stderr.write(f"Error processing JSON line: {str(e)}\n")

def process_xml_content(content):
    """Process product XML data"""
    try:
        root = ET.fromstring(content)
        
        for product in root.findall('product'):
            product_record = {
                'record_type': 'product',
                'product_id': product.get('id', ''),
                'name': product.find('name').text if product.find('name') is not None else '',
                'description': product.find('description').text if product.find('description') is not None else '',
                'category': product.find('category').text if product.find('category') is not None else '',
                'brand': product.find('brand').text if product.find('brand') is not None else '',
                'price': float(product.find('price').text) if product.find('price') is not None else 0,
                'cost': float(product.find('cost').text) if product.find('cost') is not None else 0,
                'margin_percent': 0,
                'weight_kg': float(product.find('weight_kg').text) if product.find('weight_kg') is not None else 0,
                'dimensions': product.find('dimensions').text if product.find('dimensions') is not None else '',
                'stock_quantity': int(product.find('stock_quantity').text) if product.find('stock_quantity') is not None else 0,
                'supplier_id': product.find('supplier_id').text if product.find('supplier_id') is not None else '',
                'created_date': standardize_date(product.find('created_date').text) if product.find('created_date') is not None else '',
                'is_active': product.find('is_active').text.lower() == 'true' if product.find('is_active') is not None else False
            }
            
            # Calculate margin
            if product_record['price'] > 0 and product_record['cost'] > 0:
                product_record['margin_percent'] = round(
                    ((product_record['price'] - product_record['cost']) / product_record['price']) * 100, 2
                )
            
            # Data validation
            if (product_record['product_id'] and 
                product_record['name'] and 
                product_record['price'] > 0):
                
                output = '\t'.join([str(product_record.get(k, '')) for k in product_record.keys()])
                print(f"PRODUCT\t{product_record['product_id']}\t{output}")
                
    except Exception as e:
        sys.stderr.write(f"Error processing XML content: {str(e)}\n")

def main():
    file_type = get_file_type()
    
    if file_type == 'xml':
        # For XML, read entire content
        content = sys.stdin.read()
        process_xml_content(content)
    else:
        # For CSV and JSON, process line by line
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
                
            if file_type == 'csv':
                # Skip CSV header
                if line.startswith('customer_id'):
                    continue
                process_csv_line(line)
            elif file_type == 'json':
                process_json_line(line)

if __name__ == '__main__':
    main()
```

### ETL Reducer - Load and Aggregate (reducer.py):
```
#!/usr/bin/env python3
import sys
from collections import defaultdict
import json
from datetime import datetime

class ETLReducer:
    def __init__(self):
        self.customers = {}
        self.products = {}
        self.purchases = defaultdict(list)
        self.stats = {
            'customers_processed': 0,
            'products_processed': 0,
            'purchases_processed': 0,
            'data_quality_issues': 0
        }
    
    def process_customer(self, key, data_parts):
        """Process customer record"""
        try:
            customer_data = {
                'record_type': data_parts[0],
                'customer_id': data_parts[1],
                'full_name': data_parts[2],
                'email': data_parts[3],
                'phone': data_parts[4],
                'birth_date': data_parts[5],
                'gender': data_parts[6],
                'address': data_parts[7],
                'registration_date': data_parts[8],
                'customer_tier': data_parts[9],
                'data_quality_score': int(data_parts[10]) if data_parts[10] else 0
            }
            
            self.customers[key] = customer_data
            self.stats['customers_processed'] += 1
            
            if customer_data['data_quality_score'] < 60:
                self.stats['data_quality_issues'] += 1
                
        except Exception as e:
            sys.stderr.write(f"Error processing customer {key}: {str(e)}\n")
    
    def process_product(self, key, data_parts):
        """Process product record"""
        try:
            product_data = {
                'record_type': data_parts[0],
                'product_id': data_parts[1],
                'name': data_parts[2],
                'description': data_parts[3],
                'category': data_parts[4],
                'brand': data_parts[5],
                'price': float(data_parts[6]) if data_parts[6] else 0,
                'cost': float(data_parts[7]) if data_parts[7] else 0,
                'margin_percent': float(data_parts[8]) if data_parts[8] else 0,
                'weight_kg': float(data_parts[9]) if data_parts[9] else 0,
                'dimensions': data_parts[10],
                'stock_quantity': int(data_parts[11]) if data_parts[11] else 0,
                'supplier_id': data_parts[12],
                'created_date': data_parts[13],
                'is_active': data_parts[14].lower() == 'true' if data_parts[14] else False
            }
            
            self.products[key] = product_data
            self.stats['products_processed'] += 1
            
        except Exception as e:
            sys.stderr.write(f"Error processing product {key}: {str(e)}\n")
    
    def process_purchase(self, key, data_parts):
        """Process purchase record"""
        try:
            purchase_data = {
                'record_type': data_parts[0],
                'transaction_id': data_parts[1],
                'customer_id': data_parts[2],
                'purchase_date': data_parts[3],
                'product_id': data_parts[4],
                'quantity': int(data_parts[5]) if data_parts[5] else 0,
                'unit_price': float(data_parts[6]) if data_parts[6] else 0,
                'discount_percent': float(data_parts[7]) if data_parts[7] else 0,
                'line_total': float(data_parts[8]) if data_parts[8] else 0,
                'payment_method': data_parts[9],
                'order_status': data_parts[10],
                'shipping_cost': float(data_parts[11]) if data_parts[11] else 0,
                'tax_amount': float(data_parts[12]) if data_parts[12] else 0
            }
            
            self.purchases[key].append(purchase_data)
            self.stats['purchases_processed'] += 1
            
        except Exception as e:
            sys.stderr.write(f"Error processing purchase for {key}: {str(e)}\n")
    
    def generate_enriched_records(self):
        """Generate enriched records by joining data"""
        for customer_id, purchase_list in self.purchases.items():
            customer = self.customers.get(customer_id, {})
            
            for purchase in purchase_list:
                product = self.products.get(purchase.get('product_id'), {})
                
                # Create enriched record
                enriched_record = {
                    # Customer info
                    'customer_id': customer_id,
                    'customer_name': customer.get('full_name', ''),
                    'customer_email': customer.get('email', ''),
                    'customer_tier': customer.get('customer_tier', ''),
                    'customer_registration_date': customer.get('registration_date', ''),
                    
                    # Purchase info
                    'transaction_id': purchase.get('transaction_id', ''),
                    'purchase_date': purchase.get('purchase_date', ''),
                    'quantity': purchase.get('quantity', 0),
                    'unit_price': purchase.get('unit_price', 0),
                    'line_total': purchase.get('line_total', 0),
                    'payment_method': purchase.get('payment_method', ''),
                    'order_status': purchase.get('order_status', ''),
                    
                    # Product info
                    'product_id': purchase.get('product_id', ''),
                    'product_name': product.get('name', ''),
                    'product_category': product.get('category', ''),
                    'product_brand': product.get('brand', ''),
                    'product_cost': product.get('cost', 0),
                    'product_margin_percent': product.get('margin_percent', 0),
                    
                    # Calculated fields
                    'profit': (purchase.get('line_total', 0) - 
                             (product.get('cost', 0) * purchase.get('quantity', 0))),
                    'is_high_value_customer': customer.get('customer_tier') in ['Gold', 'Platinum'],
                    'is_premium_product': product.get('price', 0) > 100
                }
                
                # Output enriched record
                print(f"ENRICHED\t{json.dumps(enriched_record)}")
    
    def generate_statistics(self):
        """Generate processing statistics"""
        stats_record = {
            'record_type': 'STATISTICS',
            'processing_timestamp': datetime.now().isoformat(),
            'customers_processed': self.stats['customers_processed'],
            'products_processed': self.stats['products_processed'],
            'purchases_processed': self.stats['purchases_processed'],
            'data_quality_issues': self.stats['data_quality_issues'],
            'enriched_records_created': sum(len(purchases) for purchases in self.purchases.values()),
            'unique_customers_with_purchases': len(self.purchases),
            'customer_match_rate': len(self.purchases) / max(self.stats['customers_processed'], 1) * 100,
            'average_purchases_per_customer': sum(len(purchases) for purchases in self.purchases.values()) / max(len(self.purchases), 1)
        }
        
        print(f"STATS\t{json.dumps(stats_record)}")

def main():
    reducer = ETLReducer()
    
    # Process all input records
    for line in sys.stdin:
        try:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            
            record_type = parts[0]
            key = parts[1]
            data_parts = parts[2:]
            
            if record_type == 'CUSTOMER':
                reducer.process_customer(key, data_parts)
            elif record_type == 'PRODUCT':
                reducer.process_product(key, data_parts)
            elif record_type == 'PURCHASE':
                reducer.process_purchase(key, data_parts)
                
        except Exception as e:
            sys.stderr.write(f"Error processing line: {str(e)}\n")
            continue
    
    # Generate outputs
    reducer.generate_enriched_records()
    reducer.generate_statistics()

if __name__ == '__main__':
    main()
```
### Data Quality Monitor: quality_monitor.py
```
#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

def analyze_etl_output():
    """Analyze ETL output for quality metrics"""
    enriched_count = 0
    stats_info = {}
    quality_issues = defaultdict(int)
    
    # Category analysis
    category_sales = defaultdict(float)
    tier_analysis = defaultdict(int)
    payment_methods = defaultdict(int)
    
    for line in sys.stdin:
        try:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) != 2:
                continue
            
            record_type = parts[0]
            data = json.loads(parts[1])
            
            if record_type == 'ENRICHED':
                enriched_count += 1
                
                # Data quality checks
                if not data.get('customer_name'):
                    quality_issues['missing_customer_name'] += 1
                if not data.get('customer_email'):
                    quality_issues['missing_customer_email'] += 1
                if not data.get('product_name'):
                    quality_issues['missing_product_name'] += 1
                if data.get('line_total', 0) <= 0:
                    quality_issues['invalid_line_total'] += 1
                
                # Business analytics
                category_sales[data.get('product_category', 'Unknown')] += data.get('line_total', 0)
                tier_analysis[data.get('customer_tier', 'Unknown')] += 1
                payment_methods[data.get('payment_method', 'Unknown')] += 1
                
            elif record_type == 'STATS':
                stats_info = data
                
        except Exception as e:
            sys.stderr.write(f"Error analyzing line: {str(e)}\n")
            continue
    
    # Generate quality report
    print("="*60)
    print("ETL QUALITY REPORT")
    print("="*60)
    
    if stats_info:
        print(f"Processing Timestamp: {stats_info.get('processing_timestamp', 'N/A')}")
        print(f"Customers Processed: {stats_info.get('customers_processed', 0):,}")
        print(f"Products Processed: {stats_info.get('products_processed', 0):,}")
        print(f"Purchases Processed: {stats_info.get('purchases_processed', 0):,}")
        print(f"Enriched Records Created: {stats_info.get('enriched_records_created', 0):,}")
        print(f"Customer Match Rate: {stats_info.get('customer_match_rate', 0):.2f}%")
        print(f"Avg Purchases per Customer: {stats_info.get('average_purchases_per_customer', 0):.2f}")
    
    print("\nDATA QUALITY ISSUES:")
    for issue, count in quality_issues.items():
        print(f"  {issue.replace('_', ' ').title()}: {count:,}")
    
    print("\nSALES BY CATEGORY:")
    for category, sales in sorted(category_sales.items(), key=lambda x: x[1], reverse=True):
        print(f"  {category}: ${sales:,.2f}")
    
    print("\nCUSTOMER TIER DISTRIBUTION:")
    for tier, count in sorted(tier_analysis.items(), key=lambda x: x[1], reverse=True):
        print(f"  {tier}: {count:,}")
    
    print("\nPAYMENT METHOD USAGE:")
    for method, count in sorted(payment_methods.items(), key=lambda x: x[1], reverse=True):
        print(f"  {method.replace('_', ' ').title()}: {count:,}")

if __name__ == '__main__':
    analyze_etl_output()
```

### Execute Complete ETL Pipeline
```
# Step 1: Generate source data
python3 generate_etl_sources.py

# Step 2: Upload to HDFS
hdfs dfs -rm -r /etl_input /etl_output 2>/dev/null
hdfs dfs -mkdir /etl_input
hdfs dfs -put customers.csv purchases.json products.xml /etl_input/

# Step 3: Run ETL MapReduce job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

# Step 4: Generate quality report
hdfs dfs -cat /etl_output/part-00000 | python3 quality_monitor.py > etl_quality_report.txt

# Step 5: View results
echo "ETL Processing completed!"
echo "Quality Report:"
cat etl_quality_report.txt

# Step 6: Extract enriched data for further processing
hdfs dfs -cat /etl_output/part-00000 | grep "^ENRICHED" | head -10
```

## 2: E-commerce Sales Data ETL Pipeline
### Overview
This lab processes large e-commerce transaction data to extract sales insights, transforming raw transaction logs into meaningful business metrics.

### Dataset Description
* Source: E-commerce transaction logs (can use synthetic data or Kaggle's retail datasets)
* Format: CSV with columns: transaction_id, customer_id, product_id, category, price, quantity, timestamp, region
* Size: Simulating 10M+ records

### Data Generator (generate_ecommerce_data.py)
```
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
```

### Mapper (mapper.py):
```
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
```

### Reducer (reducer.py):
```
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
```

### Post-Processing Script (analyze_results.py)
```
#!/usr/bin/env python3
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict

def analyze_mapreduce_output(input_file):
    """Analyze and visualize MapReduce output"""
    
    categories = defaultdict(list)
    regions = defaultdict(list)
    customers = []
    products = []
    
    with open(input_file, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 4:
                continue
                
            key, total_revenue, count, avg_revenue = parts
            total_revenue = float(total_revenue)
            count = int(count)
            avg_revenue = float(avg_revenue)
            
            if key.startswith('category_'):
                category_month = key.replace('category_', '')
                categories[category_month].append({
                    'total_revenue': total_revenue,
                    'count': count,
                    'avg_revenue': avg_revenue
                })
            elif key.startswith('region_'):
                region_month = key.replace('region_', '')
                regions[region_month].append({
                    'total_revenue': total_revenue,
                    'count': count,
                    'avg_revenue': avg_revenue
                })
            elif key.startswith('customer_'):
                customers.append({
                    'customer_id': key.replace('customer_', ''),
                    'total_spent': total_revenue,
                    'transaction_count': count
                })
            elif key.startswith('product_'):
                products.append({
                    'product_id': key.replace('product_', ''),
                    'total_revenue': total_revenue,
                    'sales_count': count
                })
    
    # Generate insights
    print("=== E-COMMERCE SALES ANALYSIS RESULTS ===\n")
    
    # Top customers by spending
    top_customers = sorted(customers, key=lambda x: x['total_spent'], reverse=True)[:10]
    print("TOP 10 CUSTOMERS BY TOTAL SPENDING:")
    for i, customer in enumerate(top_customers, 1):
        print(f"{i}. {customer['customer_id']}: ${customer['total_spent']:.2f} "
              f"({customer['transaction_count']} transactions)")
    
    # Top products by revenue
    top_products = sorted(products, key=lambda x: x['total_revenue'], reverse=True)[:10]
    print("\nTOP 10 PRODUCTS BY REVENUE:")
    for i, product in enumerate(top_products, 1):
        print(f"{i}. {product['product_id']}: ${product['total_revenue']:.2f} "
              f"({product['sales_count']} sales)")
    
    return categories, regions, customers, products

def create_visualizations(categories, regions, customers, products):
    """Create visualizations from the analysis"""
    
    # Customer spending distribution
    plt.figure(figsize=(12, 8))
    
    plt.subplot(2, 2, 1)
    spending = [c['total_spent'] for c in customers]
    plt.hist(spending, bins=50, alpha=0.7, color='blue')
    plt.title('Customer Spending Distribution')
    plt.xlabel('Total Spent ($)')
    plt.ylabel('Number of Customers')
    
    # Top 20 customers
    plt.subplot(2, 2, 2)
    top_20_customers = sorted(customers, key=lambda x: x['total_spent'], reverse=True)[:20]
    customer_ids = [c['customer_id'][-4:] for c in top_20_customers]  # Last 4 chars for readability
    spending_amounts = [c['total_spent'] for c in top_20_customers]
    
    plt.bar(range(len(customer_ids)), spending_amounts, color='green', alpha=0.7)
    plt.title('Top 20 Customers by Spending')
    plt.xlabel('Customer ID (Last 4 digits)')
    plt.ylabel('Total Spent ($)')
    plt.xticks(range(len(customer_ids)), customer_ids, rotation=45)
    
    # Product performance
    plt.subplot(2, 2, 3)
    top_20_products = sorted(products, key=lambda x: x['total_revenue'], reverse=True)[:20]
    product_ids = [p['product_id'][-4:] for p in top_20_products]
    revenues = [p['total_revenue'] for p in top_20_products]
    
    plt.bar(range(len(product_ids)), revenues, color='orange', alpha=0.7)
    plt.title('Top 20 Products by Revenue')
    plt.xlabel('Product ID (Last 4 digits)')
    plt.ylabel('Total Revenue ($)')
    plt.xticks(range(len(product_ids)), product_ids, rotation=45)
    
    # Transaction count distribution
    plt.subplot(2, 2, 4)
    transaction_counts = [c['transaction_count'] for c in customers]
    plt.hist(transaction_counts, bins=30, alpha=0.7, color='red')
    plt.title('Customer Transaction Count Distribution')
    plt.xlabel('Number of Transactions')
    plt.ylabel('Number of Customers')
    
    plt.tight_layout()
    plt.savefig('ecommerce_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_results.py <mapreduce_output_file>")
        sys.exit(1)
    
    categories, regions, customers, products = analyze_mapreduce_output(sys.argv[1])
    create_visualizations(categories, regions, customers, products)
```

### Hadoop Job Execution Script (run_sales_etl.sh)
```
#!/bin/bash

# Lab 4.2: E-commerce Sales ETL Pipeline

echo "Starting E-commerce Sales ETL Pipeline..."

# Configuration
INPUT_DIR="/user/input/ecommerce"
OUTPUT_DIR="/user/output/sales_analysis"
LOCAL_DATA_DIR="./data"

# Clean up previous runs
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null

# Create directories
hdfs dfs -mkdir -p $INPUT_DIR
mkdir -p $LOCAL_DATA_DIR

# Generate data
echo "Generating e-commerce data..."
python3 generate_ecommerce_data.py

# Upload data to HDFS
echo "Uploading data to HDFS..."
hdfs dfs -put ecommerce_data.csv $INPUT_DIR/

# Make scripts executable
chmod +x sales_mapper.py sales_reducer.py

# Run MapReduce job
echo "Running MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input $INPUT_DIR/ecommerce_data.csv \
    -output $OUTPUT_DIR

# Download results
echo "Downloading results..."
hdfs dfs -get $OUTPUT_DIR/part-00000 ./sales_results.txt

# Analyze results
echo "Analyzing results..."
python3 analyze_results.py sales_results.txt

echo "E-commerce Sales ETL Pipeline completed!"
echo "Results saved to sales_results.txt"
echo "Visualizations saved to ecommerce_analysis.png"
```


```

```


## 3: Temperature Data Analysis
Difficulty: Beginner-Intermediate
Time: 1 hour
### Objective
Process weather data to find maximum temperatures by year.
Step-by-Step Instructions

### Generate Sample Weather Data:
```

```

### Mapper (mapper.py):
```

```

### Reducer (reducer.py):
```

```

### Run Job
```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /output/part-00000
```
