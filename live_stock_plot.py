import json
import threading
import datetime
from kafka import KafkaConsumer

from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, Div
from bokeh.layouts import column
from bokeh.io import curdoc
from bokeh.models import SingleIntervalTicker, NumeralTickFormatter

# Kafka Config
TOPIC = 'stock_prices'
BOOTSTRAP_SERVERS = 'localhost:9092'
SYMBOL_TO_TRACK = 'MSFT'

# Data Source
source = ColumnDataSource(data=dict(time=[], price=[]))

# Dummy initial points to make the graph show up immediately
now = datetime.datetime.now()
source.stream({
    'time': [now, now + datetime.timedelta(seconds=1)],
    'price': [494.5, 494.8]
})

# Bokeh Plot Setup
plot = figure(
    x_axis_type='datetime',
    title=f"Live Price of {SYMBOL_TO_TRACK}",
    width=1400,
    height=400,
    tools="pan,wheel_zoom,box_zoom,reset",
    active_drag="pan",
    active_scroll="wheel_zoom"
)

# Fixed Y-axis range
plot.y_range.start = 494
plot.y_range.end = 496

# Y-axis formatting: decimals and fixed interval
plot.yaxis.formatter = NumeralTickFormatter(format="0.000")
plot.yaxis.ticker = SingleIntervalTicker(interval=0.1)
plot.yaxis.axis_label = "Price"

# X-axis formatting: datetime in HH:MM:SS
plot.xaxis.formatter = DatetimeTickFormatter(
    seconds="%H:%M:%S",
    minutes="%H:%M",
    hourmin="%H:%M",
    hours="%H:%M"
)
plot.xaxis.ticker = SingleIntervalTicker(interval=5 * 60 * 1000)  # 5 minutes
plot.xaxis.axis_label = "Time"

# Price line and markers
plot.line(x='time', y='price', source=source, line_width=2)
# plot.scatter(x='time', y='price', size=4, source=source)

# Symbol info display
info = Div(text=f"<b>Tracking Symbol:</b> {SYMBOL_TO_TRACK}", styles={"font-size": "14px"})

# Stream data safely from Kafka into Bokeh
def stream_data(new_data):
    try:
        print(f"📈 Streaming at {datetime.datetime.now()}: {new_data}")
        source.stream(new_data, rollover=43200)

        # Get latest time
        latest_time = new_data['time'][-1]

        # Define display window (e.g. 5 minutes before and after)
        window_seconds = 60 * 5  # 5 minutes
        start_time = latest_time - datetime.timedelta(seconds=window_seconds)
        end_time = latest_time + datetime.timedelta(seconds=window_seconds)

        plot.x_range.start = start_time
        plot.x_range.end = end_time

    except Exception as e:
        print("❌ Error during stream_data:", e)


# Kafka Consumer Thread
def kafka_thread(doc):
    print("Kafka thread started...")
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='bokeh-consumer'
        )
        for message in consumer:
            data = message.value
            if data['symbol'] == SYMBOL_TO_TRACK:
                ts = datetime.datetime.fromtimestamp(data['timestamp'])  # timestamp must be in seconds
                price = data['price']
                print(f"✅ Plotting {price} at {ts}")
                new_data = {'time': [ts], 'price': [price]}

                def safe_callback(new_data=new_data):
                    print("🕒 Executing Bokeh callback...")
                    stream_data(new_data)

                print("📌 Scheduling Bokeh callback")
                doc.add_next_tick_callback(safe_callback)
    except Exception as e:
        print("❌ Kafka thread crashed:", e)

# Bokeh initialization hook
def modify_doc(doc):
    doc.add_root(column(info, plot))
    doc.title = "Live Stock Tracker"

    # Start Kafka consumer in background
    threading.Thread(target=kafka_thread, args=(doc,), daemon=True).start()

# Launch Bokeh app
modify_doc(curdoc())
