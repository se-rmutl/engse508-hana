#!/bin/bash
# run_analysis.sh - Script to execute the Hadoop MapReduce job

# Configuration
INPUT_DIR="/user/input/maintenance_logs"
OUTPUT_DIR="/user/output/maintenance_analysis"
HADOOP_STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"

# Clean up previous output
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null

# Copy input data to HDFS
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put maintenance_logs.txt $INPUT_DIR/

# Run MapReduce job
hadoop jar $HADOOP_STREAMING_JAR \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input $INPUT_DIR \
    -output $OUTPUT_DIR

# View results
echo "Analysis Results:"
hdfs dfs -cat $OUTPUT_DIR/part-00000 | head -20

# Copy results back to local filesystem
hdfs dfs -get $OUTPUT_DIR/part-00000 maintenance_analysis_results.json

echo "Results saved to maintenance_analysis_results.json"