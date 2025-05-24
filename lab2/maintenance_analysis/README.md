# LAB2: Analyzing Maintenance Logs

## Objective
Hadoop MapReduce program in Python for analyzing maintenance logs. This example will process machine maintenance logs to extract insights like failure patterns, component statistics, and maintenance trends.

## Key Features:

## Mapper (mapper.py):

Parses maintenance logs in JSON or pipe-separated format
Extracts multiple analytics dimensions:

Component failure frequencies
Machine downtime statistics
Error severity distributions
Hourly/monthly maintenance patterns
Technician workloads
Machine-component correlations

## Reducer (reducer.py):

Aggregates data for comprehensive insights
Outputs structured JSON results for each analysis type
Calculates statistics like averages, totals, and counts

## Sample Data Generator:

Creates realistic maintenance log data for testing
Includes 50 machines, various components, and realistic failure patterns
Generates time-correlated data with work-hour weighting

## Analytics Provided:

Component Failure Analysis - Which components fail most frequently
Machine Downtime Stats - Total, average, and maximum downtime per machine
Severity Distribution - Breakdown of issue criticality
Temporal Patterns - When maintenance occurs (hourly/monthly trends)
Technician Workload - Task distribution across technicians
Action Type Frequency - Types of maintenance performed
Critical Incidents - Extended downtime events
Machine-Component Correlations - Which machines have issues with specific components

## Usage:
## A.Step-by-step for understanding.

Prepare the files:
```
chmod +x mapper.py reducer.py
python3 generate_sample_data.py  # Creates sample data
```

Configuration
Clean up previous output
```
hdfs dfs -rm -r /user/output/maintenance_analysis
```

Copy input data to HDFS
```
hdfs dfs -mkdir -p /user/input/maintenance_logs
hdfs dfs -put maintenance_logs.txt /user/input/maintenance_logs/
```

Run the MapReduce job:
```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /user/input/maintenance_logs \
    -output /user/output/maintenance_analysis
```

View results:
```
hdfs dfs -cat /user/output/maintenance_analysis/part-00000  | head -20
```
Copy results back to local filesystem
```
hdfs dfs -get /user/output/maintenance_analysis/part-00000 maintenance_analysis_results.json
```

View json data results:
```
cat maintenance_analysis_results.json  | head -20
```

## B.Using run script.

Prepare the files and run a script:
```
chmod +x run_analysis.sh
./run_analysis.sh
```

## Try to modify LAB2:
For generate_sample_data.py file.
Try to change number of sample logs to larger than you think ??.
ie.

def generate_sample_maintenance_logs(num_records=200000):
    """Generate sample maintenance log data"""

..
..