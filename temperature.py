import os
import random
from datetime import datetime, timedelta
from random import randint

import faust

BROKER = (
    "redpanda:9092"
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER"
    else "localhost:53231"
)


class Temperature(faust.Record, isodates=True, serializer="json"):
    ts: datetime = None
    value: int = None


class AggTemperature(faust.Record, isodates=True, serializer="json"):
    ts: datetime = None
    count: int = None
    mean: float = None
    min: int = None
    max: int = None


TOPIC = "raw-temperature"
SINK = "agg-temperature"
TABLE = "tumbling-temperature"
CLEANUP_INTERVAL = 1.0
WINDOW = 10  # 10 seconds window
WINDOW_EXPIRES = 1
PARTITIONS = 1

app = faust.App("temperature-stream", broker=f"kafka://{BROKER}")

app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(TOPIC, value_type=Temperature)
sink = app.topic(SINK, value_type=AggTemperature)


@app.timer(interval=5.0)
async def generate_temperature_data():
    temperatures_topic = app.topic(TOPIC, key_type=str, value_type=Temperature)

    # Create a loop to send data to the Redpanda topic
    # Send 20 messages every time the timer is triggered (every 5 seconds)
    for i in range(20):
        # Send the data to the Redpanda topic
        await temperatures_topic.send(
            key=random.choice(["Sensor1", "Sensor2", "Sensor3", "Sensor4", "Sensor5"]),
            value=Temperature(
                ts=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                value=randint(0, 100),
            ),
        )
    print("Producer is sleeping for 5 seconds 😴")


def window_processor(key, events):
    timestamp = key[1][0]  # key[1] is the tuple (ts, ts + window)
    values = [event.value for event in events]
    count = len(values)
    mean = sum(values) / count
    min_value = min(values)
    max_value = max(values)

    aggregated_event = AggTemperature(
        ts=timestamp, count=count, mean=mean, min=min_value, max=max_value
    )

    print(
        f"Processing window: {len(values)} events, Aggreged results: {aggregated_event}"
    )

    sink.send_soon(value=aggregated_event)


tumbling_table = (
    app.Table(
        TABLE,
        default=list,
        key_type=str,
        value_type=Temperature,
        partitions=PARTITIONS,
        on_window_close=window_processor,
    )
    .tumbling(WINDOW, expires=timedelta(seconds=WINDOW_EXPIRES))
    .relative_to_field(Temperature.ts)
)


@app.agent(app.topic(TOPIC, key_type=str, value_type=Temperature))
async def calculate_tumbling_temperatures(temperatures):
    async for temperature in temperatures:
        value_list = tumbling_table["events"].value()
        value_list.append(temperature)
        tumbling_table["events"] = value_list


if __name__ == "__main__":
    app.main()
