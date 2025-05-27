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