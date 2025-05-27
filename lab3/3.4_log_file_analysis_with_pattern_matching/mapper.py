#!/usr/bin/env python3
import sys
import re

# Common Log Format regex
log_pattern = r'(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) \S+" (\d+) (\d+)'

for line in sys.stdin:
    try:
        line = line.strip()
        match = re.match(log_pattern, line)
        
        if match:
            ip, timestamp, method, url, status, size = match.groups()
            status = int(status)
            size = int(size)
            
            # Extract hour from timestamp
            hour = timestamp.split(':')[1]
            
            # Emit different types of analysis
            print(f"page_{url}\t1")  # Page hit count
            print(f"status_{status}\t1")  # Status code count
            print(f"hour_{hour}\t1")  # Traffic by hour
            print(f"ip_{ip}\t1")  # Requests by IP
            
            if status >= 400:  # Error analysis
                print(f"error_{url}\t1")
                
    except Exception as e:
        sys.stderr.write(f"Error processing line: {str(e)}\n")
        continue