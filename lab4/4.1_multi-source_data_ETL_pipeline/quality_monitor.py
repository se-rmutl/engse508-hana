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