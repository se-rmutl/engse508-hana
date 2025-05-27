#!/usr/bin/env python3
import json
import random
import time
from datetime import datetime, timedelta
import uuid

def generate_iot_data(num_records=2000000, filename='iot_sensor_data.json'):
    """Generate synthetic IoT sensor data"""
    
    device_types = ['Temperature', 'Humidity', 'Pressure', 'Vibration', 'Light']
    locations = ['Factory_Floor_A', 'Factory_Floor_B', 'Warehouse_1', 'Warehouse_2', 'Office_Building']
    
    with open(filename, 'w') as f:
        start_time = datetime.now() - timedelta(days=30)
        
        for i in range(num_records):
            # Simulate some devices having issues (anomalies)
            is_anomaly = random.random() < 0.05  # 5% anomaly rate
            
            device_id = f"DEVICE_{random.randint(1, 1000)}"
            device_type = random.choice(device_types)
            location = random.choice(locations)
            
            # Generate sensor values based on type
            if device_type == 'Temperature':
                normal_value = random.uniform(18, 25)  # Normal room temperature
                value = random.uniform(45, 60) if is_anomaly else normal_value
                unit = 'Celsius'
            elif device_type == 'Humidity':
                normal_value = random.uniform(30, 70)  # Normal humidity
                value = random.uniform(85, 95) if is_anomaly else normal_value
                unit = 'Percent'
            elif device_type == 'Pressure':
                normal_value = random.uniform(1010, 1030)  # Normal atmospheric pressure
                value = random.uniform(980, 1000) if is_anomaly else normal_value
                unit = 'hPa'
            elif device_type == 'Vibration':
                normal_value = random.uniform(0, 5)  # Normal vibration
                value = random.uniform(15, 25) if is_anomaly else normal_value
                unit = 'm/s²'
            else:  # Light
                normal_value = random.uniform(200, 800)  # Normal light levels
                value = random.uniform(0, 50) if is_anomaly else normal_value
                unit = 'Lux'
            
            timestamp = start_time + timedelta(minutes=random.randint(0, 43200))  # 30 days in minutes
            
            record = {
                'device_id': device_id,
                'device_type': device_type,
                'location': location,
                'timestamp': timestamp.isoformat(),
                'value': round(value, 2),
                'unit': unit,
                'battery_level': random.uniform(10, 100),
                'signal_strength': random.uniform(-100, -20),
                'is_anomaly': is_anomaly  # This would not be in real data, just for validation
            }
            
            f.write(json.dumps(record) + '\n')
    
    print(f"Generated {num_records} IoT sensor records in {filename}")

if __name__ == "__main__":
    generate_iot_data(2000000)  # Generate 2M records