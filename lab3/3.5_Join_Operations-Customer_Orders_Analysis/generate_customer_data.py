import csv
import random

# Generate customers
customers = []
for i in range(1000):
    customers.append([
        i+1,  # customer_id
        f"Customer_{i+1}",  # name
        random.choice(['Premium', 'Standard', 'Basic']),  # tier
        random.choice(['USA', 'Canada', 'UK', 'Germany', 'France'])  # country
    ])

with open('customers.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['customer_id', 'name', 'tier', 'country'])
    writer.writerows(customers)

# Generate orders
orders = []
for i in range(5000):
    orders.append([
        i+1,  # order_id
        random.randint(1, 1000),  # customer_id
        random.randint(50, 1000),  # amount
        f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}"  # date
    ])

with open('orders.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['order_id', 'customer_id', 'amount', 'date'])
    writer.writerows(orders)