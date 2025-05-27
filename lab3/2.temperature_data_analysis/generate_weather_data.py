import random
import datetime

# Generate sample weather data
with open('weather_data.txt', 'w') as f:
    for year in range(2020, 2024):
        for day in range(1, 366):
            temp = random.randint(-10, 45)  # Temperature in Celsius
            date = datetime.date(year, 1, 1) + datetime.timedelta(days=day-1)
            f.write(f"{date.strftime('%Y-%m-%d')}\t{temp}\n")