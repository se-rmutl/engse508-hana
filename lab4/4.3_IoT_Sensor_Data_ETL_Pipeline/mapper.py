#!/usr/bin/env python3
import sys
import json
from datetime import datetime
import math

def detect_anomaly(device_type, value):
    """Simple anomaly detection based on device type"""
    thresholds = {
        'Temperature': {'min': 10, 'max': 35},
        'Humidity': {'min': 20, 'max': 80},
        'Pressure': {'min': 1000, 'max': 1040},
        'Vibration': {'min': 0, 'max': 10},
        'Light': {'min': 100, 'max': 1000}
    }
    
    if device_type in thresholds:
        return value < thresholds[device_type]['min'] or value > thresholds[device_type]['max']
    return False

def main():
    """
    Mapper for IoT sensor data
    Emits various key-value pairs for different analyses
    """
    for line in sys.stdin:
        try:
            data = json.loads(line.strip())
            
            device_id = data['device_id']
            device_type = data['device_type']
            location = data['location']
            timestamp = data['timestamp']
            value = float(data['value'])
            battery_level = float(data['battery_level'])
            
            # Parse timestamp for time-based aggregations
            dt = datetime.fromisoformat(timestamp)
            hour = dt.strftime('%Y-%m-%d-%H')
            day = dt.strftime('%Y-%m-%d')
            
            # 1. Hourly averages by location and device type
            print(f"hourly_{location}_{device_type}_{hour}\t{value}")
            
            # 2. Daily statistics by device
            print(f"daily_{device_id}_{day}\t{value}")
            
            # 3. Anomaly detection
            if detect_anomaly(device_type, value):
                print(f"anomaly_{device_type}_{location}\t{device_id}|{value}|{timestamp}")
            
            # 4. Battery monitoring (low battery alerts)
            if battery_level < 20:
                print(f"low_battery_{location}\t{device_id}|{battery_level}|{timestamp}")
            
            # 5. Device performance metrics
            print(f"device_stats_{device_id}\t{value}|{battery_level}")
            
            # 6. Location-based aggregations
            print(f"location_summary_{location}_{device_type}\t{value}")
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            # Skip malformed records
            continue

if __name__ == "__main__":
    main()