#!/usr/bin/env python3
import sys

current_customer_id = None
customer_info = None
orders = []

for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split('\t')
        customer_id = parts[0]
        record_type = parts[1]
        
        if current_customer_id != customer_id:
            # Process previous customer
            if current_customer_id and customer_info:
                total_amount = sum(int(order[1]) for order in orders)
                order_count = len(orders)
                avg_amount = total_amount / order_count if order_count > 0 else 0
                
                name, tier, country = customer_info
                print(f"{current_customer_id}\t{name}\t{tier}\t{country}\t{order_count}\t{total_amount}\t{avg_amount:.2f}")
            
            # Reset for new customer
            current_customer_id = customer_id
            customer_info = None
            orders = []
        
        if record_type == 'customer':
            customer_info = parts[2:5]  # name, tier, country
        elif record_type == 'order':
            orders.append(parts[2:4])  # order_id, amount
            
    except:
        continue

# Process last customer
if current_customer_id and customer_info:
    total_amount = sum(int(order[1]) for order in orders)
    order_count = len(orders)
    avg_amount = total_amount / order_count if order_count > 0 else 0
    
    name, tier, country = customer_info
    print(f"{current_customer_id}\t{name}\t{tier}\t{country}\t{order_count}\t{total_amount}\t{avg_amount:.2f}")