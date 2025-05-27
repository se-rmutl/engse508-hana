#!/usr/bin/env python3
import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns

def analyze_iot_results(input_file):
    """Analyze IoT MapReduce results and generate reports"""
    
    statistics = []
    anomalies = []
    battery_alerts = []
    device_performance = []
    
    # Parse MapReduce output
    with open(input_file, 'r') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                
                if data['type'] == 'statistics':
                    statistics.append(data)
                elif data['type'] == 'anomalies':
                    anomalies.append(data)
                elif data['type'] == 'battery_alerts':
                    battery_alerts.append(data)
                elif data['type'] == 'device_performance':
                    device_performance.append(data)
            except (json.JSONDecodeError, KeyError):
                continue
    
    # Generate comprehensive report
    print("=== IoT SENSOR DATA ANALYSIS REPORT ===\n")
    
    # 1. System Overview
    print("1. SYSTEM OVERVIEW")
    print(f"   - Total statistical aggregations: {len(statistics)}")
    print(f"   - Anomaly groups detected: {len(anomalies)}")
    print(f"   - Low battery alerts: {len(battery_alerts)}")
    print(f"   - Devices monitored: {len(device_performance)}")
    
    # 2. Anomaly Analysis
    total_anomalies = sum(a['count'] for a in anomalies)
    print(f"\n2. ANOMALY DETECTION")
    print(f"   - Total anomalies detected: {total_anomalies}")
    
    if anomalies:
        print("   - Top anomaly sources:")
        sorted_anomalies = sorted(anomalies, key=lambda x: x['count'], reverse=True)
        for i, anomaly in enumerate(sorted_anomalies[:5], 1):
            key_parts = anomaly['key'].split('_')
            device_type = key_parts[1] if len(key_parts) > 1 else 'Unknown'
            location = key_parts[2] if len(key_parts) > 2 else 'Unknown'
            print(f"     {i}. {device_type} sensors in {location}: {anomaly['count']} anomalies")
    
    # 3. Battery Health
    total_battery_alerts = sum(b['count'] for b in battery_alerts)
    print(f"\n3. BATTERY HEALTH")
    print(f"   - Total low battery alerts: {total_battery_alerts}")
    
    if battery_alerts:
        print("   - Locations with most battery issues:")
        location_battery_issues = {}
        for alert in battery_alerts:
            location = alert['key'].split('_')[2] if len(alert['key'].split('_')) > 2 else 'Unknown'
            location_battery_issues[location] = location_battery_issues.get(location, 0) + alert['count']
        
        sorted_locations = sorted(location_battery_issues.items(), key=lambda x: x[1], reverse=True)
        for i, (location, count) in enumerate(sorted_locations[:5], 1):
            print(f"     {i}. {location}: {count} devices")
    
    # 4. Device Performance
    print(f"\n4. DEVICE PERFORMANCE")
    if device_performance:
        health_scores = [d['health_score'] for d in device_performance if 'health_score' in d]
        if health_scores:
            avg_health = sum(health_scores) / len(health_scores)
            print(f"   - Average system health score: {avg_health:.1f}/100")
            
            # Identify devices needing attention
            poor_devices = [d for d in device_performance if d.get('health_score', 100) < 50]
            print(f"   - Devices requiring attention: {len(poor_devices)}")
            
            if poor_devices:
                print("   - Worst performing devices:")
                sorted_poor = sorted(poor_devices, key=lambda x: x.get('health_score', 0))
                for i, device in enumerate(sorted_poor[:5], 1):
                    device_id = device['key'].split('_')[2] if len(device['key'].split('_')) > 2 else 'Unknown'
                    print(f"     {i}. {device_id}: Health Score {device.get('health_score', 0)}/100")
    
    # 5. Operational Insights
    print(f"\n5. OPERATIONAL INSIGHTS")
    
    # Location analysis
    location_stats = {}
    for stat in statistics:
        if stat['key'].startswith('location_summary_'):
            key_parts = stat['key'].split('_')
            if len(key_parts) >= 4:
                location = key_parts[2]
                device_type = key_parts[3]
                
                if location not in location_stats:
                    location_stats[location] = {}
                
                location_stats[location][device_type] = stat['data']
    
    if location_stats:
        print("   - Location Performance Summary:")
        for location, devices in location_stats.items():
            total_readings = sum(d.get('count', 0) for d in devices.values())
            print(f"     {location}: {total_readings} total readings across {len(devices)} device types")
    
    return statistics, anomalies, battery_alerts, device_performance

def create_iot_visualizations(statistics, anomalies, battery_alerts, device_performance):
    """Create visualizations for IoT analysis"""
    
    plt.figure(figsize=(15, 12))
    
    # 1. Anomaly distribution by device type
    plt.subplot(2, 3, 1)
    device_anomalies = {}
    for anomaly in anomalies:
        key_parts = anomaly['key'].split('_')
        device_type = key_parts[1] if len(key_parts) > 1 else 'Unknown'
        device_anomalies[device_type] = device_anomalies.get(device_type, 0) + anomaly['count']
    
    if device_anomalies:
        plt.bar(device_anomalies.keys(), device_anomalies.values(), color='red', alpha=0.7)
        plt.title('Anomalies by Device Type')
        plt.xlabel('Device Type')
        plt.ylabel('Number of Anomalies')
        plt.xticks(rotation=45)
    
    # 2. Health score distribution
    plt.subplot(2, 3, 2)
    health_scores = [d['health_score'] for d in device_performance if 'health_score' in d]
    if health_scores:
        plt.hist(health_scores, bins=20, color='green', alpha=0.7, edgecolor='black')
        plt.title('Device Health Score Distribution')
        plt.xlabel('Health Score')
        plt.ylabel('Number of Devices')
        plt.axvline(sum(health_scores)/len(health_scores), color='red', linestyle='--', 
                   label=f'Average: {sum(health_scores)/len(health_scores):.1f}')
        plt.legend()
    
    # 3. Battery alerts by location
    plt.subplot(2, 3, 3)
    location_battery_issues = {}
    for alert in battery_alerts:
        location = alert['key'].split('_')[2] if len(alert['key'].split('_')) > 2 else 'Unknown'
        location_battery_issues[location] = location_battery_issues.get(location, 0) + alert['count']
    
    if location_battery_issues:
        plt.bar(location_battery_issues.keys(), location_battery_issues.values(), 
                color='orange', alpha=0.7)
        plt.title('Battery Alerts by Location')
        plt.xlabel('Location')
        plt.ylabel('Number of Alerts')
        plt.xticks(rotation=45)
    
    # 4. Average sensor readings by device type
    plt.subplot(2, 3, 4)
    device_type_stats = {}
    for stat in statistics:
        if stat['key'].startswith('location_summary_'):
            key_parts = stat['key'].split('_')
            if len(key_parts) >= 4:
                device_type = key_parts[3]
                if device_type not in device_type_stats:
                    device_type_stats[device_type] = []
                device_type_stats[device_type].append(stat['data']['mean'])
    
    if device_type_stats:
        device_types = list(device_type_stats.keys())
        avg_readings = [sum(readings)/len(readings) for readings in device_type_stats.values()]
        plt.bar(device_types, avg_readings, color='blue', alpha=0.7)
        plt.title('Average Sensor Readings by Type')
        plt.xlabel('Device Type')
        plt.ylabel('Average Reading')
        plt.xticks(rotation=45)
    
    # 5. Data quality metrics
    plt.subplot(2, 3, 5)
    total_readings = sum(stat['data']['count'] for stat in statistics if 'data' in stat)
    total_anomalies = sum(a['count'] for a in anomalies)
    total_battery_alerts = sum(b['count'] for b in battery_alerts)
    
    categories = ['Normal\nReadings', 'Anomalies', 'Battery\nAlerts']
    values = [total_readings - total_anomalies, total_anomalies, total_battery_alerts]
    colors = ['green', 'red', 'orange']
    
    plt.pie(values, labels=categories, colors=colors, autopct='%1.1f%%', startangle=90)
    plt.title('Data Quality Overview')
    
    # 6. System reliability trend (simulated hourly data)
    plt.subplot(2, 3, 6)
    # Extract hourly data for trend analysis
    hourly_data = {}
    for stat in statistics:
        if stat['key'].startswith('hourly_'):
            key_parts = stat['key'].split('_')
            if len(key_parts) >= 4:
                hour = key_parts[-1]  # Last part should be the hour
                if hour not in hourly_data:
                    hourly_data[hour] = {'readings': 0, 'devices': 0}
                hourly_data[hour]['readings'] += stat['data']['count']
                hourly_data[hour]['devices'] += 1
    
    if hourly_data:
        sorted_hours = sorted(hourly_data.keys())[-24:]  # Last 24 hours
        reading_counts = [hourly_data[hour]['readings'] for hour in sorted_hours]
        
        plt.plot(range(len(sorted_hours)), reading_counts, marker='o', color='purple')
        plt.title('Readings Volume (Last 24 Hours)')
        plt.xlabel('Time (Hours Ago)')
        plt.ylabel('Number of Readings')
        plt.xticks(range(0, len(sorted_hours), 4), 
                  [f'{24-i}h' for i in range(0, len(sorted_hours), 4)])
    
    plt.tight_layout()
    plt.savefig('iot_analysis_dashboard.png', dpi=300, bbox_inches='tight')
    plt.show()

def generate_alerts(anomalies, battery_alerts, device_performance):
    """Generate actionable alerts for operations team"""
    
    alerts = []
    
    # Critical anomalies
    critical_anomalies = [a for a in anomalies if a['count'] > 10]
    for anomaly in critical_anomalies:
        key_parts = anomaly['key'].split('_')
        device_type = key_parts[1] if len(key_parts) > 1 else 'Unknown'
        location = key_parts[2] if len(key_parts) > 2 else 'Unknown'
        
        alerts.append({
            'severity': 'HIGH',
            'type': 'ANOMALY_CLUSTER',
            'message': f'High anomaly rate detected: {anomaly["count"]} anomalies from {device_type} sensors in {location}',
            'action': f'Investigate {device_type} sensors in {location} for hardware issues or environmental changes'
        })
    
    # Battery maintenance
    critical_battery = [b for b in battery_alerts if b['count'] > 5]
    for battery in critical_battery:
        location = battery['key'].split('_')[2] if len(battery['key'].split('_')) > 2 else 'Unknown'
        
        alerts.append({
            'severity': 'MEDIUM',
            'type': 'BATTERY_MAINTENANCE',
            'message': f'Multiple low battery devices in {location}: {battery["count"]} devices need attention',
            'action': f'Schedule battery replacement/charging for devices in {location}'
        })
    
    # Poor performing devices
    poor_devices = [d for d in device_performance if d.get('health_score', 100) < 30]
    for device in poor_devices:
        device_id = device['key'].split('_')[2] if len(device['key'].split('_')) > 2 else 'Unknown'
        
        alerts.append({
            'severity': 'HIGH',
            'type': 'DEVICE_FAILURE',
            'message': f'Device {device_id} health score critically low: {device.get("health_score", 0)}/100',
            'action': f'Immediate inspection required for device {device_id} - potential hardware failure'
        })
    
    return sorted(alerts, key=lambda x: {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}[x['severity']], reverse=True)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_iot_results.py <mapreduce_output_file>")
        sys.exit(1)
    
    statistics, anomalies, battery_alerts, device_performance = analyze_iot_results(sys.argv[1])
    create_iot_visualizations(statistics, anomalies, battery_alerts, device_performance)
    
    # Generate operational alerts
    alerts = generate_alerts(anomalies, battery_alerts, device_performance)
    
    if alerts:
        print(f"\n6. OPERATIONAL ALERTS ({len(alerts)} total)")
        for i, alert in enumerate(alerts[:10], 1):  # Show top 10 alerts
            print(f"   {i}. [{alert['severity']}] {alert['type']}")
            print(f"      {alert['message']}")
            print(f"      Action: {alert['action']}\n")
    
    print("Analysis complete! Check iot_analysis_dashboard.png for visualizations.")