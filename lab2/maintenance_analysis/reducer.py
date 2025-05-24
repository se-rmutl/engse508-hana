#!/usr/bin/env python3
"""
Reducer for aggregating maintenance log analysis results
"""

import sys
import json

class MaintenanceLogReducer:
    """
    Reducer for aggregating maintenance log analysis results
    """
    
    def __init__(self):
        self.current_key = None
        self.current_values = []
    
    def emit_result(self, key, values):
        """Process and emit results for a specific key"""
        key_parts = key.split('\t')
        analysis_type = key_parts[0]
        identifier = key_parts[1] if len(key_parts) > 1 else 'unknown'
        
        if analysis_type == 'component_failure':
            # Count failures per component
            total_failures = sum(int(v) for v in values)
            result = {
                'analysis': 'component_failure_frequency',
                'component': identifier,
                'failure_count': total_failures
            }
            print(json.dumps(result))
            
        elif analysis_type == 'machine_downtime':
            # Calculate downtime statistics per machine
            downtimes = [float(v) for v in values]
            result = {
                'analysis': 'machine_downtime_stats',
                'machine_id': identifier,
                'total_downtime_hours': sum(downtimes),
                'average_downtime_hours': sum(downtimes) / len(downtimes),
                'max_downtime_hours': max(downtimes),
                'incident_count': len(downtimes)
            }
            print(json.dumps(result))
            
        elif analysis_type == 'severity_dist':
            # Count by severity category
            count = sum(int(v) for v in values)
            result = {
                'analysis': 'severity_distribution',
                'severity': identifier,
                'count': count
            }
            print(json.dumps(result))
            
        elif analysis_type == 'hourly_pattern':
            # Maintenance frequency by hour
            count = sum(int(v) for v in values)
            result = {
                'analysis': 'hourly_maintenance_pattern',
                'hour': int(identifier),
                'maintenance_count': count
            }
            print(json.dumps(result))
            
        elif analysis_type == 'monthly_trend':
            # Monthly maintenance trends
            count = sum(int(v) for v in values)
            result = {
                'analysis': 'monthly_maintenance_trend',
                'month': identifier,
                'maintenance_count': count
            }
            print(json.dumps(result))
            
        elif analysis_type == 'technician_load':
            # Workload per technician
            count = sum(int(v) for v in values)
            result = {
                'analysis': 'technician_workload',
                'technician': identifier,
                'task_count': count
            }
            print(json.dumps(result))
            
        elif analysis_type == 'action_type':
            # Frequency of different action types
            count = sum(int(v) for v in values)
            result = {
                'analysis': 'action_type_frequency',
                'action': identifier,
                'count': count
            }
            print(json.dumps(result))
            
        elif analysis_type == 'machine_component':
            # Machine-component failure correlation
            count = sum(int(v) for v in values)
            machine_id, component = identifier.split(':', 1)
            result = {
                'analysis': 'machine_component_correlation',
                'machine_id': machine_id,
                'component': component,
                'failure_count': count
            }
            print(json.dumps(result))
            
        elif analysis_type == 'critical_downtime':
            # Critical downtime incidents
            downtimes = [float(v) for v in values]
            result = {
                'analysis': 'critical_downtime_incidents',
                'machine_id': identifier,
                'total_critical_hours': sum(downtimes),
                'incident_count': len(downtimes),
                'max_critical_hours': max(downtimes)
            }
            print(json.dumps(result))
    
    def reduce(self):
        """Main reducer function"""
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split('\t')
            if len(parts) < 2:
                continue
                
            key = '\t'.join(parts[:-1])
            value = parts[-1]
            
            if self.current_key == key:
                self.current_values.append(value)
            else:
                if self.current_key is not None:
                    self.emit_result(self.current_key, self.current_values)
                
                self.current_key = key
                self.current_values = [value]
        
        # Process the last group
        if self.current_key is not None:
            self.emit_result(self.current_key, self.current_values)

def main():
    """Main reducer execution"""
    reducer = MaintenanceLogReducer()
    reducer.reduce()

if __name__ == "__main__":
    main()