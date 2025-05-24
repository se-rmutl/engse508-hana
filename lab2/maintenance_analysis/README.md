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