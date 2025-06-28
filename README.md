# 📈 Real-Time Stock Price Tracker with Kafka + Bokeh + PySpark

This project fetches live stock prices (e.g., MSFT) from the [Finnhub API](https://finnhub.io/), publishes them to a Kafka topic, and visualizes them in real time using Bokeh. Optionally, it includes a PySpark Streaming consumer for scalable backend processing.

---
## Data Pipeline Architecture

![Real-Time Stock Price Tracker with Kafka + Bokeh + PySpark](https://github.com/user-attachments/assets/7e4c5fe2-03ab-44a0-bad4-8ef44e170a1d)

## Real Time Dashboard
https://github.com/user-attachments/assets/23384763-15f4-4384-a3c6-5f5e19f6ca1f

## 🧩 Components

### 1. Kafka Producer
- **Language**: Python
- **Task**: Polls Finnhub API every second and publishes stock price data to a Kafka topic.

### 2. Kafka + Bokeh Consumer
- **Language**: Python
- **Task**: Listens to the Kafka topic, updates a live chart with real-time stock prices.

### 3. PySpark Streaming App (Optional)
- **Language**: PySpark
- **Task**: Consumes Kafka stream, parses JSON data, and optionally performs transformations or writes to sinks like console, file, or databases.

---

## 🛠️ Setup Instructions

### Prerequisites

- Python 3.7+
- Kafka & Zookeeper (local or remote)
- Java 8+
- PySpark (`pip install pyspark`)
- Bokeh (`pip install bokeh`)
- Kafka-Python (`pip install kafka-python`)
- Requests (`pip install requests`)

---

## 🚀 Running the Project

### 1. Start Kafka + Zookeeper

```bash
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka Broker
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --topic stock_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
### 2.  Run the Kafka Producer
```bash
python kafka_producer.py
```
### 3.  Run the Bokeh Consumer (Live Plot)
```bash
bokeh serve --show bokeh_consumer.py
```
### 4.  Run PySpark Streaming Consumer
```bash
spark-submit spark_streaming_consumer.py
```

## 🔧 Configuration
Symbol: Set in kafka_producer.py (e.g., symbol = 'MSFT')

API Key: Get one from Finnhub.io and paste it in kafka_producer.py

Kafka Topic: stock_prices (can be changed in both producer and consumer)

)

## 📊 Sample Output (Console)
```bash
✅ Plotting 495.23 at 2025-06-28 13:45:01
📈 Streaming at 2025-06-28 13:45:01: {'time': [datetime.datetime(2025, 6, 28, 13, 45, 1)], 'price': [495.23]}
```

## 📎 File Structure
```bash
├── kafka_producer.py             # Fetches and sends stock data to Kafka
├── bokeh_consumer.py             # Live dashboard using Bokeh
├── spark_streaming_consumer.py   # (Optional) PySpark consumer
├── README.md
```
## 💡 Ideas for Extension
Add historical context or average lines

Integrate alerting for price thresholds

Use a WebSocket or REST API for front-end dashboard updates





