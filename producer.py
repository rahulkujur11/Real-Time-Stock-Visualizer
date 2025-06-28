import requests
from kafka import KafkaProducer
import json
import time

# Finnhub setup
API_KEY = 'd1fdqdhr01qig3h15760d1fdqdhr01qig3h1576g'
symbol = 'MSFT'
url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}'

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # adjust for your setup
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'stock_prices'

while True:
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        price = data.get('c')
        if price is not None:
            message = {
                'symbol': symbol,
                'price': price,
                'timestamp': int(time.time())
            }
            producer.send(topic, message)
            print(f"Sent to Kafka: {message}")
        else:
            print("No price data found in response:", data)

    except Exception as e:
        print("Error fetching or sending data:", e)

    time.sleep(1)  # poll every 5 seconds, adjust as needed
