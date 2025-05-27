#!/bin/bash

# Lab 4.2: E-commerce Sales ETL Pipeline

echo "Starting E-commerce Sales ETL Pipeline..."

# Configuration
INPUT_DIR="/user/input/ecommerce"
OUTPUT_DIR="/user/output/sales_analysis"
LOCAL_DATA_DIR="./data"

# Clean up previous runs
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null

# Create directories
hdfs dfs -mkdir -p $INPUT_DIR
mkdir -p $LOCAL_DATA_DIR

# Generate data
echo "Generating e-commerce data..."
python3 generate_ecommerce_data.py

# Upload data to HDFS
echo "Uploading data to HDFS..."
hdfs dfs -put ecommerce_data.csv $INPUT_DIR/

# Make scripts executable
chmod +x sales_mapper.py sales_reducer.py

# Run MapReduce job
echo "Running MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input $INPUT_DIR/ecommerce_data.csv \
    -output $OUTPUT_DIR

# Download results
echo "Downloading results..."
hdfs dfs -get $OUTPUT_DIR/part-00000 ./sales_results.txt

# Analyze results
echo "Analyzing results..."
python3 analyze_results.py sales_results.txt

echo "E-commerce Sales ETL Pipeline completed!"
echo "Results saved to sales_results.txt"
echo "Visualizations saved to ecommerce_analysis.png"