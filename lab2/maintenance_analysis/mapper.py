#!/usr/bin/env python3
"""
Mapper for processing maintenance log entries.
Expected log format (JSON or structured text):
timestamp|machine_id|component|action|severity|duration|technician|description
"""

import sys
import json
import re
from datetime import datetime

class MaintenanceLogMapper:
    """
    Mapper for processing maintenance log entries.
    """
    
    def __init__(self):
        self.error_patterns = {
            'CRITICAL': ['critical', 'fatal', 'emergency', 'severe'],
            'WARNING': ['warning', 'warn', 'caution', 'alert'],
            'INFO': ['info', 'normal', 'routine', 'scheduled'],
            'ERROR': ['error', 'fail', 'fault', 'malfunction']
        }
    
    def parse_log_entry(self, line):
        """Parse a log entry and extract relevant fields"""
        try:
            # Handle JSON format
            if line.strip().startswith('{'):
                data = json.loads(line.strip())
                return {
                    'timestamp': data.get('timestamp', ''),
                    'machine_id': data.get('machine_id', ''),
                    'component': data.get('component', ''),
                    'action': data.get('action', ''),
                    'severity': data.get('severity', ''),
                    'duration': float(data.get('duration', 0)),
                    'technician': data.get('technician', ''),
                    'description': data.get('description', '')
                }
            else:
                # Handle pipe-separated format
                parts = line.strip().split('|')
                if len(parts) >= 6:
                    return {
                        'timestamp': parts[0],
                        'machine_id': parts[1],
                        'component': parts[2],
                        'action': parts[3],
                        'severity': parts[4],
                        'duration': float(parts[5]) if parts[5].replace('.', '').isdigit() else 0,
                        'technician': parts[6] if len(parts) > 6 else '',
                        'description': parts[7] if len(parts) > 7 else ''
                    }
        except (json.JSONDecodeError, ValueError, IndexError):
            return None
        
        return None
    
    def extract_time_components(self, timestamp):
        """Extract hour, day of week, month from timestamp"""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return {
                'hour': dt.hour,
                'day_of_week': dt.weekday(),
                'month': dt.month,
                'year': dt.year
            }
        except:
            return {'hour': 0, 'day_of_week': 0, 'month': 1, 'year': 2024}
    
    def categorize_severity(self, description, severity):
        """Categorize severity based on keywords in description"""
        text = (description + ' ' + severity).lower()
        
        for category, keywords in self.error_patterns.items():
            if any(keyword in text for keyword in keywords):
                return category
        
        return 'UNKNOWN'
    
    def map(self, line):
        """Main mapper function"""
        data = self.parse_log_entry(line)
        if not data:
            return
        
        time_info = self.extract_time_components(data['timestamp'])
        severity_category = self.categorize_severity(data['description'], data['severity'])
        
        # Emit multiple key-value pairs for different analyses
        
        # 1. Component failure frequency
        print(f"component_failure\t{data['component']}\t1")
        
        # 2. Machine downtime analysis
        if data['duration'] > 0:
            print(f"machine_downtime\t{data['machine_id']}\t{data['duration']}")
        
        # 3. Severity distribution
        print(f"severity_dist\t{severity_category}\t1")
        
        # 4. Hourly maintenance patterns
        print(f"hourly_pattern\t{time_info['hour']}\t1")
        
        # 5. Monthly trends
        print(f"monthly_trend\t{time_info['year']}-{time_info['month']:02d}\t1")
        
        # 6. Technician workload
        if data['technician']:
            print(f"technician_load\t{data['technician']}\t1")
        
        # 7. Action type analysis
        print(f"action_type\t{data['action']}\t1")
        
        # 8. Machine-component failure correlation
        print(f"machine_component\t{data['machine_id']}:{data['component']}\t1")
        
        # 9. Critical issues (duration > threshold)
        if data['duration'] > 240:  # 4 hours
            print(f"critical_downtime\t{data['machine_id']}\t{data['duration']}")

def main():
    """Main mapper execution"""
    mapper = MaintenanceLogMapper()
    
    for line in sys.stdin:
        mapper.map(line)

if __name__ == "__main__":
    main()