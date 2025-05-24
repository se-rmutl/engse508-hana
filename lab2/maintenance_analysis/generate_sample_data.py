#!/usr/bin/env python3
"""
Sample data generator for testing the MapReduce program
"""

import json
import random
from datetime import datetime, timedelta

def generate_sample_maintenance_logs(num_records=10000):
    """Generate sample maintenance log data"""
    
    machines = [f"MACHINE_{i:03d}" for i in range(1, 51)]  # 50 machines
    components = [
        'ENGINE', 'TRANSMISSION', 'BRAKE_SYSTEM', 'COOLING_SYSTEM',
        'ELECTRICAL', 'HYDRAULICS', 'PNEUMATICS', 'CONTROL_UNIT',
        'SENSORS', 'ACTUATORS', 'FILTERS', 'BELTS', 'BEARINGS'
    ]
    actions = [
        'PREVENTIVE_MAINTENANCE', 'CORRECTIVE_REPAIR', 'INSPECTION',
        'REPLACEMENT', 'CALIBRATION', 'CLEANING', 'LUBRICATION',
        'EMERGENCY_REPAIR', 'UPGRADE', 'DIAGNOSTIC'
    ]
    severities = ['CRITICAL', 'WARNING', 'INFO', 'ERROR']
    technicians = [f"TECH_{i:02d}" for i in range(1, 21)]  # 20 technicians
    
    base_date = datetime(2024, 1, 1)
    
    sample_logs = []
    
    for i in range(num_records):
        # Generate realistic timestamp (more incidents during work hours)
        days_offset = random.randint(0, 365)
        hour = random.choices(
            range(24), 
            weights=[2, 1, 1, 1, 2, 3, 5, 8, 10, 12, 15, 15, 12, 10, 8, 8, 6, 5, 4, 3, 3, 2, 2, 2]
        )[0]
        
        timestamp = base_date + timedelta(days=days_offset, hours=hour, 
                                        minutes=random.randint(0, 59))
        
        # Generate correlated data (certain components fail more often)
        component = random.choices(
            components,
            weights=[15, 12, 10, 8, 6, 8, 6, 4, 5, 5, 3, 4, 3]
        )[0]
        
        # Duration based on action type and severity
        action = random.choice(actions)
        severity = random.choice(severities)
        
        base_duration = {
            'PREVENTIVE_MAINTENANCE': 2.0,
            'CORRECTIVE_REPAIR': 4.0,
            'INSPECTION': 0.5,
            'REPLACEMENT': 6.0,
            'EMERGENCY_REPAIR': 8.0
        }.get(action, 2.0)
        
        severity_multiplier = {
            'CRITICAL': 2.0,
            'ERROR': 1.5,
            'WARNING': 1.0,
            'INFO': 0.5
        }.get(severity, 1.0)
        
        duration = base_duration * severity_multiplier * random.uniform(0.5, 2.0)
        
        descriptions = {
            'CRITICAL': [
                'Complete system failure detected',
                'Emergency shutdown required',
                'Safety system malfunction',
                'Production line stopped'
            ],
            'ERROR': [
                'Component malfunction detected',
                'Performance degradation observed',
                'Abnormal readings detected',
                'System error reported'
            ],
            'WARNING': [
                'Preventive maintenance due',
                'Minor deviation detected',
                'Warning threshold exceeded',
                'Scheduled inspection required'
            ],
            'INFO': [
                'Routine maintenance completed',
                'System operating normally',
                'Scheduled service performed',
                'Regular inspection completed'
            ]
        }
        
        log_entry = {
            'timestamp': timestamp.isoformat(),
            'machine_id': random.choice(machines),
            'component': component,
            'action': action,
            'severity': severity,
            'duration': round(duration, 2),
            'technician': random.choice(technicians),
            'description': random.choice(descriptions[severity])
        }
        
        sample_logs.append(json.dumps(log_entry))
    
    return sample_logs

# Generate and save sample data
def main():
    print("Generating sample maintenance logs...")
    logs = generate_sample_maintenance_logs(1000)  # Generate 1000 records for testing
    
    with open('maintenance_logs.txt', 'w') as f:
        for log in logs:
            f.write(log + '\n')
    
    print(f"Generated {len(logs)} sample maintenance log entries in 'maintenance_logs.txt'")
    
    # Also create some pipe-separated format samples
    print("Creating pipe-separated format samples...")
    with open('maintenance_logs_pipe.txt', 'w') as f:
        for i in range(100):
            log_data = json.loads(logs[i])
            pipe_format = f"{log_data['timestamp']}|{log_data['machine_id']}|{log_data['component']}|{log_data['action']}|{log_data['severity']}|{log_data['duration']}|{log_data['technician']}|{log_data['description']}"
            f.write(pipe_format + '\n')
    
    print("Created 100 pipe-separated format samples in 'maintenance_logs_pipe.txt'")

if __name__ == "__main__":
    main()