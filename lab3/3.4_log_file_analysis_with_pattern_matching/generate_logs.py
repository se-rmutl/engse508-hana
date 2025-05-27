import random
import datetime

ips = ['192.168.1.' + str(i) for i in range(1, 101)]
pages = ['/home', '/products', '/about', '/contact', '/login', '/checkout', '/api/users', '/api/orders']
status_codes = [200, 200, 200, 200, 404, 500, 301, 403]  # Weighted towards 200
user_agents = ['Mozilla/5.0', 'Chrome/91.0', 'Safari/14.0', 'Edge/91.0']

with open('access.log', 'w') as f:
    base_time = datetime.datetime(2023, 1, 1)
    for i in range(50000):
        ip = random.choice(ips)
        timestamp = base_time + datetime.timedelta(seconds=i*2)
        method = 'GET'
        page = random.choice(pages)
        status = random.choice(status_codes)
        size = random.randint(500, 5000)
        user_agent = random.choice(user_agents)
        
        log_line = f'{ip} - - [{timestamp.strftime("%d/%b/%Y:%H:%M:%S +0000")}] "{method} {page} HTTP/1.1" {status} {size} "-" "{user_agent}"'
        f.write(log_line + '\n')