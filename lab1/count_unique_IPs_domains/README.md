# LAB2: Count unique IPs or domains in logs

## Objective
Hadoop MapReduce program in Python for counting unique IPs or domains in logs. 

## Key Features:

## Download NASA HTTP Web Server, a Large Log File (Public Dataset)
NASA HTTP Web Server Log from July 1995, which is often used in log analysis and Hadoop demos: Download (Approx. 20MB uncompressed)
```
mkdir works
cd works

wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
gunzip NASA_access_log_Jul95.gz
```

## Put the File into HDFS
```
hdfs dfs -mkdir -p /logs
hdfs dfs -put NASA_access_log_Jul95 /logs/

```

## The Python Code, Count unique IPs or domains in logs using MapReduce:

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys
import io # Required for TextIOWrapper

def run_mapper():
    for line_num, raw_line in enumerate(sys.stdin): # sys.stdin is now the reconfigured wrapper
        try:
            # raw_line is already decoded by the TextIOWrapper
            line = raw_line.strip()
            
            if not line: # Skip fully empty or whitespace-only lines
                # print(f"DEBUG: Mapper skipping empty line {line_num+1}", file=sys.stderr)
                continue
            
            parts = line.split(' ') 
            
            if not parts or not parts[0]: # Check if parts is empty or first part is empty
                # print(f"DEBUG: Mapper skipping line {line_num+1} with no IP: '{raw_line.strip()}'", file=sys.stderr)
                continue

            ip_address = parts[0]
            # The print function will encode this string to UTF-8 by default for stdout,
            # which is what Hadoop Streaming generally expects.
            print(f'{ip_address}\t1')

        except Exception as e:
            # raw_line here would be the string decoded using latin-1
            print(f"MAPPER ERROR: Failed processing line {line_num+1}: '{raw_line.strip()}'", file=sys.stderr)
            print(f"MAPPER EXCEPTION: {e}", file=sys.stderr)

if __name__ == "__main__":
    # Reconfigure sys.stdin to read with 'latin-1' encoding.
    # This will treat all bytes as valid characters from the Latin-1 set.
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')
    
    # Optional: If you want to be sure about output encoding for Hadoop,
    # though print() usually handles this well by defaulting to UTF-8 on Linux.
    # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    run_mapper()

```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys
import io # Required for TextIOWrapper

def run_reducer():
    current_ip = None
    current_count = 0

    for line_num, raw_line in enumerate(sys.stdin): # sys.stdin is now the reconfigured wrapper
        try:
            # raw_line is already decoded by the TextIOWrapper
            line = raw_line.strip()

            # Split the input line (ip_address <tab> count_str)
            ip_address, count_str = line.split('\t', 1)
            count = int(count_str)

            if current_ip == ip_address:
                current_count += count
            else:
                if current_ip:
                    # Output the previous IP's count
                    print(f'{current_ip}\t{current_count}')
                
                current_ip = ip_address
                current_count = count
        except ValueError:
            # This error is for when int(count_str) fails or line.split fails
            print(f"REDUCER ERROR: ValueError on line {line_num+1} (decoded): '{raw_line.strip()}'", file=sys.stderr)
            continue # Skip malformed lines
        except Exception as e:
            print(f"REDUCER ERROR: General exception on line {line_num+1} (decoded): '{raw_line.strip()}'", file=sys.stderr)
            print(f"REDUCER EXCEPTION: {e}", file=sys.stderr)
            continue # Skip lines that cause other errors

    # Output the last IP address count
    if current_ip:
        print(f'{current_ip}\t{current_count}')

if __name__ == "__main__":
    # Reconfigure sys.stdin for the reducer as well.
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')
    
    # Optional: Reconfigure sys.stdout for the reducer.
    # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    run_reducer()

```

## Running steps:
## Method 1
```
chmod +x mapper.py reducer.py

hadoop fs -rm -r -f /output/ip_counts

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files $(pwd)/mapper.py,$(pwd)/reducer.py \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /logs/NASA_access_log_Jul95 \
    -output /output/ip_counts
```
## Check Hadoop output with:
```
hdfs dfs -ls /output/ip_counts
hdfs dfs -cat /output/ip_counts/part-*
```

## Method 2
Create a script and run it:
```
chmod +x mapper.py reducer.py

nano run_mapreduce.sh
chmod 755 run_mapreduce.sh
./run_mapreduce.sh
```
## run_mapreduce.sh:
```
#!/bin/bash

# Simple shell script to run the MapReduce job

# Ensure we have HADOOP_HOME set
if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME is not set. Please set it to your Hadoop installation directory."
    exit 1
fi

# Input and output paths
INPUT_PATH="/logs/NASA_access_log_Jul95"
OUTPUT_PATH="/output/ip_counts"

# Remove output directory if it exists
hadoop fs -rm -r -f $OUTPUT_PATH

# Run the Hadoop streaming job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files $(pwd)/mapper.py,$(pwd)/reducer.py \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input $INPUT_PATH \
    -output $OUTPUT_PATH

# Show the result
echo "Job completed. Results:"
hadoop fs -cat $OUTPUT_PATH/part-*
```

## Post-Job CSV Export Script

Saving raw output to another file (*.tsv), which is used for conversion to *.csv 
```
hdfs dfs -cat /output/ip_counts/part-* > ip_counts.tsv
```
Then use this script to convert *.tsv to *. csv:   
Edit ->   nano convert_to_csv.py
```
# convert_to_csv.py
import pandas as pd

df = pd.read_csv("ip_counts.tsv", sep='\t', names=['IP', 'Count'])
df.to_csv("ip_frequency.csv", index=False)

```
Create a script and run it:
```
chmod +x convert_to_csv.py
python3 convert_to_csv.py
```

Here’s a version with improved y-axis scaling and visualization clarity::    
Edit ->   nano plot_ip_frequency.py
```
import pandas as pd
import matplotlib.pyplot as plt

# Load CSV file
df = pd.read_csv("ip_frequency.csv")

# Sort by request count, descending, and select top N
top_n = 20
df = df.sort_values(by='Count', ascending=False).head(top_n)

# Plot
plt.figure(figsize=(14, 8))
bars = plt.bar(df['IP'], df['Count'], color='skyblue', edgecolor='black')

# Y-axis label and custom limit
plt.ylabel('Number of Requests')
plt.xlabel('IP Address')
plt.title(f'Top {top_n} IP Addresses by Request Count')

# Set dynamic y-axis limit (10% above max for padding)
plt.ylim(0, df['Count'].max() * 1.10)

# Add number labels above bars
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, height + 1, str(height),
             ha='center', va='bottom', fontsize=9)

# Improve x-label visibility
plt.xticks(rotation=45, ha='right')

# Tight layout for clarity
plt.tight_layout()

# Save and show
plt.savefig("top_ip_requests.png", dpi=300)
#plt.show()
```

## Key Improvements
plt.ylim(0, df['Count'].max() * 1.10) ensures space above tallest bar.
plt.figure(figsize=(14, 8)) increases visual size.
plt.text(..., str(height)) shows the actual number on each bar.
plt.xticks(rotation=45) makes long IPs readable.

Create a script and run it:
```
chmod +x plot_ip_frequency.py
python3 plot_ip_frequency.py
```
