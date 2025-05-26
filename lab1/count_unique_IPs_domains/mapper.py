#!/usr/bin/env python3
import sys
import io # Required for TextIOWrapper

def run_mapper():
    for line_num, raw_line in enumerate(sys.stdin): # sys.stdin is now the reconfigured wrapper
        try:
            # raw_line is already decoded by the TextIOWrapper
            line = raw_line.strip()
            
            if not line: # Skip fully empty or whitespace-only lines
                # print(f"DEBUG: Mapper skipping empty line {line_num+1}", file=sys.stderr)
                continue
            
            parts = line.split(' ') 
            
            if not parts or not parts[0]: # Check if parts is empty or first part is empty
                # print(f"DEBUG: Mapper skipping line {line_num+1} with no IP: '{raw_line.strip()}'", file=sys.stderr)
                continue

            ip_address = parts[0]
            # The print function will encode this string to UTF-8 by default for stdout,
            # which is what Hadoop Streaming generally expects.
            print(f'{ip_address}\t1')

        except Exception as e:
            # raw_line here would be the string decoded using latin-1
            print(f"MAPPER ERROR: Failed processing line {line_num+1}: '{raw_line.strip()}'", file=sys.stderr)
            print(f"MAPPER EXCEPTION: {e}", file=sys.stderr)

if __name__ == "__main__":
    # Reconfigure sys.stdin to read with 'latin-1' encoding.
    # This will treat all bytes as valid characters from the Latin-1 set.
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')
    
    # Optional: If you want to be sure about output encoding for Hadoop,
    # though print() usually handles this well by defaulting to UTF-8 on Linux.
    # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    run_mapper()
