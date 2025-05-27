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