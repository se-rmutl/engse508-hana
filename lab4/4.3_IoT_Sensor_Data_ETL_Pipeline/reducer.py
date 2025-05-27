#!/usr/bin/env python3
import sys
import json
from collections import defaultdict
import math

def calculate_statistics(values):
    """Calculate statistical measures for a list of values"""
    if not values:
        return {}
    
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    std_dev = math.sqrt(variance)
    
    sorted_values = sorted(values)
    median = sorted_values[n // 2] if n % 2 == 1 else (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    
    return {
        'count': n,
        'mean': round(mean, 2),
        'median': round(median, 2),
        'std_dev': round(std_dev, 2),
        'min': min(values),
        'max': max(values)
    }

def main():
    """
    Reducer for IoT sensor data
    Aggregates and calculates statistics for different key types
    """
    current_key = None
    values = []
    anomalies = []
    battery_alerts = []
    device_metrics = []
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            key, value = line.split('\t', 1)
            
            if current_key != key:
                # Process the previous group
                if current_key:
                    process_group(current_key, values, anomalies, battery_alerts, device_metrics)
                
                # Reset for new group
                current_key = key
                values = []
                anomalies = []
                battery_alerts = []
                device_metrics = []
            
            # Categorize the value based on key type
            if current_key.startswith('anomaly_'):
                anomalies.append(value)
            elif current_key.startswith('low_battery_'):
                battery_alerts.append(value)
            elif current_key.startswith('device_stats_'):
                device_metrics.append(value)
            else:
                # Numeric values for aggregation
                try:
                    values.append(float(value))
                except ValueError:
                    # Skip non-numeric values
                    continue
        
        except ValueError:
            continue
    
    # Process the last group
    if current_key:
        process_group(current_key, values, anomalies, battery_alerts, device_metrics)

def process_group(key, values, anomalies, battery_alerts, device_metrics):
    """Process a group of values based on the key type"""
    
    if key.startswith('hourly_') or key.startswith('daily_') or key.startswith('location_summary_'):
        # Statistical aggregation
        if values:
            stats = calculate_statistics(values)
            output = {
                'key': key,
                'type': 'statistics',
                'data': stats
            }
            print(json.dumps(output))
    
    elif key.startswith('anomaly_'):
        # Anomaly reporting
        if anomalies:
            output = {
                'key': key,
                'type': 'anomalies',
                'count': len(anomalies),
                'anomalies': anomalies[:10]  # Limit to first 10 for output size
            }
            print(json.dumps(output))
    
    elif key.startswith('low_battery_'):
        # Battery alerts
        if battery_alerts:
            output = {
                'key': key,
                'type': 'battery_alerts',
                'count': len(battery_alerts),
                'alerts': battery_alerts
            }
            print(json.dumps(output))
    
    elif key.startswith('device_stats_'):
        # Device performance metrics
        if device_metrics:
            sensor_values = []
            battery_values = []
            
            for metric in device_metrics:
                parts = metric.split('|')
                if len(parts) == 2:
                    sensor_values.append(float(parts[0]))
                    battery_values.append(float(parts[1]))
            
            output = {
                'key': key,
                'type': 'device_performance',
                'sensor_stats': calculate_statistics(sensor_values),
                'battery_stats': calculate_statistics(battery_values),
                'health_score': calculate_health_score(sensor_values, battery_values)
            }
            print(json.dumps(output))

def calculate_health_score(sensor_values, battery_values):
    """Calculate a simple health score for the device"""
    if not sensor_values or not battery_values:
        return 0
    
    # Simple scoring based on battery level and sensor stability
    avg_battery = sum(battery_values) / len(battery_values)
    sensor_stability = 100 - (math.sqrt(sum((x - sum(sensor_values)/len(sensor_values))**2 for x in sensor_values) / len(sensor_values)))
    
    health_score = (avg_battery * 0.4 + max(0, sensor_stability) * 0.6)
    return min(100, max(0, round(health_score, 1)))

if __name__ == "__main__":
    main()