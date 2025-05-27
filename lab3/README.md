# LAB3:  ETL (Extract, Transform, Load) processes using Hadoop MapReduce

## 1.Word Count with File Processing
Difficulty: Beginner
Time: 45 minutes
### Objective
Process multiple text files and count word frequencies with improved error handling.
Step-by-Step Instructions
### Enhanced Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import re

def clean_word(word):
    # Remove punctuation and convert to lowercase
    return re.sub(r'[^\w]', '', word.lower())

for line in sys.stdin:
    try:
        line = line.strip()
        words = line.split()
        for word in words:
            clean = clean_word(word)
            if clean:  # Only emit non-empty words
                print(f"{clean}\t1")
    except Exception as e:
        # Log errors to stderr
        sys.stderr.write(f"Error processing line: {e}\n")
        continue
```

## Enhanced Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys
from collections import defaultdict

word_counts = defaultdict(int)

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue
        word, count = line.split('\t')
        word_counts[word] += int(count)
    except ValueError:
        sys.stderr.write(f"Invalid line format: {line}\n")
        continue

# Output sorted results
for word in sorted(word_counts.keys()):
    print(f"{word}\t{word_counts[word]}")
```

## Sample Data Generator:
```
# Create sample text files
mkdir sample_data
cat > sample_data/file1.txt << EOF
The quick brown fox jumps over the lazy dog.
The dog was sleeping under the tree.
EOF

cat > sample_data/file2.txt << EOF
Python is a great programming language.
Hadoop MapReduce is powerful for big data processing.
EOF

# Upload to HDFS
hdfs dfs -rm -r /input /output 2>/dev/null
hdfs dfs -mkdir /input
hdfs dfs -put sample_data/* /input/
```
## Run Enhanced Job
```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /output/part-00000
```

## 2: Temperature Data Analysis
Difficulty: Beginner-Intermediate
Time: 1 hour
## Objective
Process weather data to find maximum temperatures by year.
Step-by-Step Instructions

## Generate Sample Weather Data: generate_weather_data.py
```
import random
import datetime

# Generate sample weather data
with open('weather_data.txt', 'w') as f:
    for year in range(2020, 2024):
        for day in range(1, 366):
            temp = random.randint(-10, 45)  # Temperature in Celsius
            date = datetime.date(year, 1, 1) + datetime.timedelta(days=day-1)
            f.write(f"{date.strftime('%Y-%m-%d')}\t{temp}\n")
```

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        date, temperature = line.split('\t')
        year = date.split('-')[0]
        print(f"{year}\t{temperature}")
    except ValueError:
        continue
```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys

current_year = None
max_temp = float('-inf')

for line in sys.stdin:
    try:
        line = line.strip()
        year, temp = line.split('\t')
        temp = int(temp)
        
        if current_year == year:
            max_temp = max(max_temp, temp)
        else:
            if current_year:
                print(f"{current_year}\t{max_temp}")
            current_year = year
            max_temp = temp
    except ValueError:
        continue

if current_year:
    print(f"{current_year}\t{max_temp}")
```

## Run Job
```
python3 generate_weather_data.py
hdfs dfs -rm -r /weather_input /weather_output 2>/dev/null
hdfs dfs -mkdir /weather_input
hdfs dfs -put weather_data.txt /weather_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /weather_output/part-00000
```

## 3: Sales Data Processing with Multiple Outputs
Difficulty: Intermediate
Time: 1.5 hours
## Objective
Process sales data to calculate total sales by region and product category.
Step-by-Step Instructions

## Generate Sales Data: generate_sales_data.py
```
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
```

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import csv

# Skip header
header_skipped = False

for line in sys.stdin:
    if not header_skipped:
        header_skipped = True
        continue
    
    try:
        row = next(csv.reader([line.strip()]))
        if len(row) == 5:
            date, product, region, quantity, price = row
            total_sale = int(quantity) * int(price)
            
            # Emit region-based aggregation
            print(f"region_{region}\t{total_sale}")
            
            # Emit product-based aggregation
            print(f"product_{product}\t{total_sale}")
    except:
        continue
```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys

current_key = None
current_sum = 0

for line in sys.stdin:
    try:
        line = line.strip()
        key, value = line.split('\t')
        value = int(value)
        
        if current_key == key:
            current_sum += value
        else:
            if current_key:
                category, name = current_key.split('_', 1)
                print(f"{category}\t{name}\t{current_sum}")
            current_key = key
            current_sum = value
    except:
        continue

if current_key:
    category, name = current_key.split('_', 1)
    print(f"{category}\t{name}\t{current_sum}")
```

## Run Job
```
python3 generate_sales_data.py
hdfs dfs -rm -r /sales_input /sales_output 2>/dev/null
hdfs dfs -mkdir /sales_input
hdfs dfs -put sales_data.csv /sales_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /sales_output/part-00000
```

## 4: Log File Analysis with Pattern Matching
Difficulty: Intermediate
Time: 1.5 hours
## Objective
Analyze web server logs to extract insights about traffic patterns, error rates, and popular pages.
Step-by-Step Instructions

## Generate Sample Data: generate_logs.py
```
import random
import datetime

ips = ['192.168.1.' + str(i) for i in range(1, 101)]
pages = ['/home', '/products', '/about', '/contact', '/login', '/checkout', '/api/users', '/api/orders']
status_codes = [200, 200, 200, 200, 404, 500, 301, 403]  # Weighted towards 200
user_agents = ['Mozilla/5.0', 'Chrome/91.0', 'Safari/14.0', 'Edge/91.0']

with open('access.log', 'w') as f:
    base_time = datetime.datetime(2023, 1, 1)
    for i in range(50000):
        ip = random.choice(ips)
        timestamp = base_time + datetime.timedelta(seconds=i*2)
        method = 'GET'
        page = random.choice(pages)
        status = random.choice(status_codes)
        size = random.randint(500, 5000)
        user_agent = random.choice(user_agents)
        
        log_line = f'{ip} - - [{timestamp.strftime("%d/%b/%Y:%H:%M:%S +0000")}] "{method} {page} HTTP/1.1" {status} {size} "-" "{user_agent}"'
        f.write(log_line + '\n')
```

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import re

# Common Log Format regex
log_pattern = r'(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) \S+" (\d+) (\d+)'

for line in sys.stdin:
    try:
        line = line.strip()
        match = re.match(log_pattern, line)
        
        if match:
            ip, timestamp, method, url, status, size = match.groups()
            status = int(status)
            size = int(size)
            
            # Extract hour from timestamp
            hour = timestamp.split(':')[1]
            
            # Emit different types of analysis
            print(f"page_{url}\t1")  # Page hit count
            print(f"status_{status}\t1")  # Status code count
            print(f"hour_{hour}\t1")  # Traffic by hour
            print(f"ip_{ip}\t1")  # Requests by IP
            
            if status >= 400:  # Error analysis
                print(f"error_{url}\t1")
                
    except Exception as e:
        sys.stderr.write(f"Error processing line: {str(e)}\n")
        continue
```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    try:
        line = line.strip()
        key, count = line.split('\t')
        count = int(count)
        
        if current_key == key:
            current_count += count
        else:
            if current_key:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = count
    except:
        continue

if current_key:
    print(f"{current_key}\t{current_count}")
```

## Run Job
```
python3 generate_logs.py
hdfs dfs -rm -r /logs_input /logs_output 2>/dev/null
hdfs dfs -mkdir /logs_input
hdfs dfs -put access.log /logs_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

# Create analysis report
hdfs dfs -cat /logs_output/part-00000 | sort -k2 -nr > log_analysis_report.txt
echo "Top 10 most visited pages:"
grep "^page_" log_analysis_report.txt | head -10
echo -e "\nStatus code distribution:"
grep "^status_" log_analysis_report.txt
```

## 5: Join Operations - Customer Orders Analysis
Difficulty: Intermediate-Advanced
Time: 2 hours
## Objective
Perform join operations on customer and order data to analyze customer behavior.
Step-by-Step Instructions

## Generate Related Datasets: generate_customer_data.py
```
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
```

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import csv
import os

filename = os.environ.get('mapreduce_map_input_file', '')

for line in sys.stdin:
    try:
        row = next(csv.reader([line.strip()]))
        
        if 'customer' in filename.lower():
            # Customer data: customer_id, name, tier, country
            if len(row) == 4 and row[0] != 'customer_id':
                customer_id, name, tier, country = row
                print(f"{customer_id}\tcustomer\t{name}\t{tier}\t{country}")
        
        elif 'order' in filename.lower():
            # Order data: order_id, customer_id, amount, date
            if len(row) == 4 and row[0] != 'order_id':
                order_id, customer_id, amount, date = row
                print(f"{customer_id}\torder\t{order_id}\t{amount}\t{date}")
    except:
        continue
```

## Reducer (reducer.py):
```
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
```

## Run Job
```
python3 generate_customer_data.py

hdfs dfs -rm -r /join_input /join_output 2>/dev/null
hdfs dfs -mkdir /join_input
hdfs dfs -put customers.csv orders.csv /join_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /join_output/part-00000 | head -20
```

## 6: Secondary Sorting with Composite Keys
Difficulty: Advanced
Time: 2.5 hours
## Objective
Implement secondary sorting to process stock price data and find daily high/low prices.
Step-by-Step Instructions

## Generate Stock Data: generate_stock_data.py
```
import csv
import random
import datetime

stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

with open('stock_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['date', 'symbol', 'time', 'price', 'volume'])
    
    base_date = datetime.date(2023, 1, 1)
    for day in range(365):
        current_date = base_date + datetime.timedelta(days=day)
        for stock in stocks:
            base_price = random.randint(100, 300)
            for minute in range(0, 480, 5):  # Trading day: 8 hours, every 5 minutes
                time_str = f"{9 + minute//60:02d}:{minute%60:02d}"
                price = base_price + random.randint(-10, 10)
                volume = random.randint(1000, 10000)
                writer.writerow([current_date, stock, time_str, price, volume])
```

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import csv

for line in sys.stdin:
    try:
        row = next(csv.reader([line.strip()]))
        if len(row) == 5 and row[0] != 'date':
            date, symbol, time, price, volume = row
            
            # Create composite key: symbol_date, and natural key: time
            # This allows sorting by symbol, then date, then time
            composite_key = f"{symbol}_{date}"
            print(f"{composite_key}\t{time}\t{price}\t{volume}")
    except:
        continue
```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys
from datetime import datetime

current_key = None
daily_prices = []

def process_daily_data(key, prices):
    if not prices:
        return
    
    symbol, date = key.split('_')
    prices.sort(key=lambda x: x[0])  # Sort by time
    
    high_price = max(float(p[1]) for p in prices)
    low_price = min(float(p[1]) for p in prices)
    open_price = float(prices[0][1])  # First price of day
    close_price = float(prices[-1][1])  # Last price of day
    total_volume = sum(int(p[2]) for p in prices)
    
    print(f"{symbol}\t{date}\t{open_price}\t{high_price}\t{low_price}\t{close_price}\t{total_volume}")

for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split('\t')
        key = parts[0]
        time = parts[1]
        price = parts[2]
        volume = parts[3]
        
        if current_key == key:
            daily_prices.append((time, price, volume))
        else:
            if current_key:
                process_daily_data(current_key, daily_prices)
            current_key = key
            daily_prices = [(time, price, volume)]
    except:
        continue

if current_key:
    process_daily_data(current_key, daily_prices)
```

## Run Job
```
python3 generate_stock_data.py

hdfs dfs -rm -r /stock_input /stock_output 2>/dev/null
hdfs dfs -mkdir /stock_input
hdfs dfs -put stock_data.csv /stock_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /stock_output/part-00000 | head -20
```

## 7: Advanced Text Processing with N-grams
Difficulty: Advanced
Time: 2.5 hours
## Objective
Process large text corpus to generate n-grams and analyze text patterns.
Step-by-Step Instructions

## Download Sample Text Data:
```
# Download a large text file (Project Gutenberg)
wget https://www.gutenberg.org/files/2701/2701-0.txt -O moby_dick.txt
wget https://www.gutenberg.org/files/1342/1342-0.txt -O pride_prejudice.txt
wget https://www.gutenberg.org/files/11/11-0.txt -O alice_wonderland.txt
```

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import re
import string

def clean_text(text):
    # Remove punctuation and convert to lowercase
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

def generate_ngrams(words, n):
    ngrams = []
    for i in range(len(words) - n + 1):
        ngram = ' '.join(words[i:i+n])
        ngrams.append(ngram)
    return ngrams

# Configure n-gram size (can be passed as parameter)
N = 3  # trigrams

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue
            
        # Clean and tokenize
        clean_line = clean_text(line)
        words = clean_line.split()
        
        if len(words) >= N:
            ngrams = generate_ngrams(words, N)
            for ngram in ngrams:
                print(f"{ngram}\t1")
    except:
        continue
```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys

# Minimum frequency threshold
MIN_FREQUENCY = 5

current_ngram = None
current_count = 0

for line in sys.stdin:
    try:
        line = line.strip()
        ngram, count = line.split('\t')
        count = int(count)
        
        if current_ngram == ngram:
            current_count += count
        else:
            if current_ngram and current_count >= MIN_FREQUENCY:
                print(f"{current_ngram}\t{current_count}")
            current_ngram = ngram
            current_count = count
    except:
        continue

if current_ngram and current_count >= MIN_FREQUENCY:
    print(f"{current_ngram}\t{current_count}")
```

## Run Job
```
hdfs dfs -rm -r /text_input /ngram_output 2>/dev/null
hdfs dfs -mkdir /text_input
hdfs dfs -put *.txt /text_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

# Analyze results
hdfs dfs -cat /ngram_output/part-00000 | sort -k2 -nr | head -20 > top_ngrams.txt
echo "Top 20 trigrams:"
cat top_ngrams.txt
```

## 8: Machine Learning Feature Extraction
Difficulty: Advanced
Time: 3 hours
## Objective
Extract features from text data for machine learning and perform TF-IDF calculation.
Step-by-Step Instructions

## Generate Document Corpus: generate_documents.py
```
import random

topics = {
    'technology': ['computer', 'software', 'programming', 'algorithm', 'data', 'network', 'system', 'digital'],
    'science': ['research', 'experiment', 'hypothesis', 'theory', 'analysis', 'discovery', 'study', 'method'],
    'business': ['market', 'profit', 'customer', 'strategy', 'company', 'revenue', 'growth', 'investment'],
    'sports': ['game', 'team', 'player', 'score', 'match', 'competition', 'tournament', 'champion']
}

documents = []
for doc_id in range(1000):
    topic = random.choice(list(topics.keys()))
    words = topics[topic] + ['the', 'and', 'in', 'of', 'to', 'a', 'is', 'for', 'with']
    
    doc_length = random.randint(50, 200)
    doc_words = [random.choice(words) for _ in range(doc_length)]
    
    document = ' '.join(doc_words)
    documents.append(f"doc_{doc_id}\t{topic}\t{document}")

with open('documents.txt', 'w') as f:
    for doc in documents:
        f.write(doc + '\n')
```

## Mapper (mapper.py):
```

```

## Reducer (reducer.py):
```

```

## Run Job
```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /output/part-00000
```

## 2: Temperature Data Analysis
Difficulty: Beginner-Intermediate
Time: 1 hour
## Objective
Process weather data to find maximum temperatures by year.
Step-by-Step Instructions

## Generate Sample Weather Data:
```

```

## Mapper (mapper.py):
```

```

## Reducer (reducer.py):
```

```

## Run Job
```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input \
    -output /output

hdfs dfs -cat /output/part-00000
```

## Lab Progression Summary:
Beginner (Labs 1-3):

Environment Setup & Hello World - Basic word count
Enhanced Word Count - File processing with error handling
Temperature Analysis - Weather data aggregation

Intermediate (Labs 4-6):
4. Sales Data Processing - Multiple output types and business analytics
5. Log File Analysis - Web server log processing with regex patterns
6. Join Operations - Customer-order data relationships
Advanced (Labs 7-9):
7. Secondary Sorting - Stock price analysis with composite keys
8. N-gram Processing - Advanced text analysis for large corpora
9. ML Feature Extraction - TF-IDF calculation for machine learning
Expert (Lab 10):
10. Real-time Analytics - Time-windowed aggregations with anomaly detection

## Key Features of This Tutorial Series:
✅ Progressive Difficulty - Each lab builds on previous concepts
✅ Real-world Examples - Sales, logs, stock prices, sensor data
✅ Complete Code - All mapper/reducer scripts included
✅ Step-by-step Instructions - Detailed execution commands
✅ External Resources - Links to datasets and documentation
✅ Troubleshooting Guide - Common issues and solutions
✅ Performance Tips - Memory tuning and optimization
✅ Career Path - Certification recommendations
What You'll Learn:

Hadoop ecosystem setup and configuration
Python streaming MapReduce programming
Data processing patterns (aggregation, filtering, joining)
Performance optimization techniques
Real-world big data analytics scenarios
Advanced concepts like secondary sorting and windowed processing

Each lab includes sample data generation scripts, so you can run everything locally. The tutorials progress from simple word counting to complex real-time analytics with anomaly detection!