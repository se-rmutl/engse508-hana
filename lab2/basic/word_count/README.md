# LAB2: MapReduce Basic

## Objective
Hadoop MapReduce program in Python for analyzing maintenance logs. This example will process machine maintenance logs to extract insights like failure patterns, component statistics, and maintenance trends.

## Key Features:

## 1. Basic local word count:
```
echo "I am who I am" | python3 mapper.py
echo "I am who I am" | python3 mapper.py | sort -k1,1
echo "I am who I am" | python3 mapper.py | sort -k1,1 | python3 reducer.py
```

## 2. Word count using MapReduce:

## Mapper (mapper.py):
```
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    for word in line.strip().split():
        print(f"{word.lower()}\t1")
```

## Reducer (reducer.py):
```
#!/usr/bin/env python3
import sys
from collections import defaultdict

word_counts = defaultdict(int)

for line in sys.stdin:
    word, count = line.strip().split("\t")
    word_counts[word] += int(count)

for word, count in word_counts.items():
    print(f"{word}\t{count}")
```

## Running steps:
```
hdfs dfs -mkdir -p /input/words
wget https://www.gutenberg.org/files/1342/1342-0.txt
more 1342-0.txt
hdfs dfs -put 1342-0.txt /input/words
hdfs dfs -ls /input/words

cat 1342-0.txt | python3 mapper.py | sort -k1,1 | python3 reducer.py

hdfs dfs -rm -r /output/wordcount-output

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /input/words \
    -output /output/wordcount-output

hdfs dfs -ls /output/wordcount-output
hdfs dfs -cat /output/wordcount-output/part-00000 | head -100

```