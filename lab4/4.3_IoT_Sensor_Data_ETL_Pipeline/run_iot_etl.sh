#!/bin/bash

# Lab 2: IoT Sensor Data ETL Pipeline

echo "Starting IoT Sensor Data ETL Pipeline..."

# Configuration
INPUT_DIR="/user/input/iot"
OUTPUT_DIR="/user/output/iot_analysis"
LOCAL_DATA_DIR="./data"

# Clean up previous runs
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null

# Create directories
hdfs dfs -mkdir -p $INPUT_DIR
mkdir -p $LOCAL_DATA_DIR

# Generate IoT data
echo "Generating IoT sensor data..."
python3 generate_iot_data.py

# Upload data to HDFS
echo "Uploading data to HDFS..."
hdfs dfs -put iot_sensor_data.json $INPUT_DIR/

# Make scripts executable
chmod +x iot_mapper.py iot_reducer.py

# Run MapReduce job
echo "Running IoT MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input $INPUT_DIR/iot_sensor_data.json \
    -output $OUTPUT_DIR

# Download results
echo "Downloading results..."
hdfs dfs -get $OUTPUT_DIR/part-00000 ./iot_results.json

# Analyze results and generate reports
echo "Analyzing IoT results..."
python3 analyze_iot_results.py iot_results.json

echo "IoT Sensor Data ETL Pipeline completed!"
echo "Results saved to iot_results.json"
echo "Dashboard saved to iot_analysis_dashboard.png"