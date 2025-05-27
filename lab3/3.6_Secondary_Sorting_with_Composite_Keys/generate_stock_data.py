import csv
import random
import datetime

stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

with open('stock_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['date', 'symbol', 'time', 'price', 'volume'])
    
    base_date = datetime.date(2023, 1, 1)
    for day in range(365):
        current_date = base_date + datetime.timedelta(days=day)
        for stock in stocks:
            base_price = random.randint(100, 300)
            for minute in range(0, 480, 5):  # Trading day: 8 hours, every 5 minutes
                time_str = f"{9 + minute//60:02d}:{minute%60:02d}"
                price = base_price + random.randint(-10, 10)
                volume = random.randint(1000, 10000)
                writer.writerow([current_date, stock, time_str, price, volume])