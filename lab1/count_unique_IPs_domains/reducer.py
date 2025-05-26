#!/usr/bin/env python3
import sys
import io # Required for TextIOWrapper

def run_reducer():
    current_ip = None
    current_count = 0

    for line_num, raw_line in enumerate(sys.stdin): # sys.stdin is now the reconfigured wrapper
        try:
            # raw_line is already decoded by the TextIOWrapper
            line = raw_line.strip()

            # Split the input line (ip_address <tab> count_str)
            ip_address, count_str = line.split('\t', 1)
            count = int(count_str)

            if current_ip == ip_address:
                current_count += count
            else:
                if current_ip:
                    # Output the previous IP's count
                    print(f'{current_ip}\t{current_count}')
                
                current_ip = ip_address
                current_count = count
        except ValueError:
            # This error is for when int(count_str) fails or line.split fails
            print(f"REDUCER ERROR: ValueError on line {line_num+1} (decoded): '{raw_line.strip()}'", file=sys.stderr)
            continue # Skip malformed lines
        except Exception as e:
            print(f"REDUCER ERROR: General exception on line {line_num+1} (decoded): '{raw_line.strip()}'", file=sys.stderr)
            print(f"REDUCER EXCEPTION: {e}", file=sys.stderr)
            continue # Skip lines that cause other errors

    # Output the last IP address count
    if current_ip:
        print(f'{current_ip}\t{current_count}')

if __name__ == "__main__":
    # Reconfigure sys.stdin for the reducer as well.
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')
    
    # Optional: Reconfigure sys.stdout for the reducer.
    # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    run_reducer()
