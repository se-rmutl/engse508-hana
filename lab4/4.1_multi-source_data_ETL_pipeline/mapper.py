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