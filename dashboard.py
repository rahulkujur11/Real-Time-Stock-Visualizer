import matplotlib
matplotlib.use('Agg')  # Non-GUI backend

from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque

TOPIC = 'stock-stream'
BOOTSTRAP_SERVERS = 'localhost:9092'
SYMBOL_TO_TRACK = 'AAPL'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

prices = deque(maxlen=50)
timestamps = deque(maxlen=50)

fig, ax = plt.subplots()

def animate(i):
    for message in consumer:
        data = message.value
        if data['symbol'] == SYMBOL_TO_TRACK:
            prices.append(data['price'])
            timestamps.append(data['timestamp'])
            break

    ax.clear()
    ax.plot(timestamps, prices, label=f"{SYMBOL_TO_TRACK} Price")
    ax.set_title(f"Live Price of {SYMBOL_TO_TRACK}")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Price")
    ax.legend(loc='upper left')
    ax.grid(True)

anim = animation.FuncAnimation(fig, animate, interval=1000, save_count=50)

# Save to video file instead of showing GUI
anim.save("stock_animation.mp4", fps=1)
print("Animation saved to stock_animation.mp4")
