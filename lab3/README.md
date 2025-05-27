# LAB3:  ETL (Extract, Transform, Load) processes using Hadoop MapReduce

## 1.Word Count with File Processing
Difficulty: Beginner
Time: 45 minutes
## Objective
Process multiple text files and count word frequencies with improved error handling.
Step-by-Step Instructions
## Enhanced Mapper (mapper.py):
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

## Generate Sample Weather Data:
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


