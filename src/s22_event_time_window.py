# src/s22_event_time_window.py

import os
import argparse
import json
from datetime import datetime
from typing import Dict, Any

from pyflink.common import Types, WatermarkStrategy, Row
from pyflink.common.time import Time
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows

# If you have a helper or model that converts Row → FlightData, import it here.
# from models import FlightData

def parse_args() -> argparse.Namespace:
    """
    Parse command‐line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Event‐time windowed aggregation of flights by departure airport"
    )
    parser.add_argument(
        "--bootstrap.servers",
        required=False,
        default=os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic",
        required=False,
        default="flightdata",
        help="Kafka topic to consume flight JSON from"
    )
    return parser.parse_args()


def extract_timestamp_and_airport(row: Row) -> Row:
    """
    Map a raw JSON Row to (departure_airport_code: str, event_ts: int).

    Assumes that `row` contains a single field 'value' which is a JSON string.
    That JSON must have 'departure_time' (ISO‐format) and 'iata_departure_code'.

    Returns:
        A Row of form (departure_airport: str, event_ts: int).
    """
    # row[0] is the raw JSON string
    raw_json: str = row[0]
    data: Dict[str, Any] = json.loads(raw_json)

    # Validate required fields
    if not isinstance(data.get("departure_time"), str):
        raise ValueError("Field 'departure_time' missing or not a string")
    if not isinstance(data.get("iata_departure_code"), str):
        raise ValueError("Field 'iata_departure_code' missing or not a string")

    # Parse ISO timestamp (e.g., "2025-06-05T12:34:56Z")
    dt_str: str = data["departure_time"]
    # You may need to tweak the format below if your timestamps differ
    event_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    event_ts = int(event_dt.timestamp() * 1000)  # milliseconds since epoch

    airport: str = data["iata_departure_code"]
    return Row(airport, event_ts)


def main() -> None:
    """
    Entry point for the event‐time window example.
    """
    args = parse_args()
    bootstrap_servers: str = args.bootstrap_servers
    topic: str = args.topic

    # 1. Set up the streaming environment.
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.get_config().set_auto_watermark_interval(1000)  # emit watermarks every second

    # 2. Create a Kafka source of JSON strings (each message.value is the JSON payload).
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id("flink_event_time_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Assume each record is a UTF‐8 JSON string; no key is used.
        .set_value_only_deserializer(
            # This yields a Python string (UTF‐8)
            lambda x: x.decode("utf-8")
        )
        .build()
    )

    # 3. Ingest the raw JSON string stream.
    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="json_flight_source"
    )

    # 4. Extract (airport, event_ts) and assign watermarks based on event_ts.
    #    We allow 10 seconds of out‐of‐orderness.
    timestamped_stream = (
        raw_stream
        .map(
            lambda json_str: Row(json_str),
            output_type=Types.ROW([Types.STRING()])
        )
        .map(
            extract_timestamp_and_airport,
            output_type=Types.ROW([Types.STRING(), Types.LONG()])
        )
        # Set the first field (index 1) as the event timestamp, and allow 10s lateness.
        .assign_timestamps_and_watermarks(
            WatermarkStrategy
            .for_bounded_out_of_orderness(Time.seconds(10))
            .with_timestamp_assigner(lambda row, ts: row[1])
        )
    )

    # 5. Key by departure airport, apply a tumbling 1‐minute window, and count flights.
    counts_per_airport = (
        timestamped_stream
        .key_by(lambda row: row[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .reduce(
            lambda a, b: Row(a[0], a[1], a[2] + 1) if len(a) == 3 else Row(a[0], a[1], 2),
            # The above reduce assumes that a Row is either (airport, ts) on first element,
            # or (airport, window_end, count) on subsequent elements.
            # For clarity, you could instead use an aggregate function below.
            output_type=Types.ROW([Types.STRING(), Types.LONG(), Types.INT()])
        )
    )

    # 6. Print the results: (airport, window_end_ts, count)
    counts_per_airport.print()

    # 7. Execute the pipeline.
    env.execute("s22_event_time_window")


if __name__ == "__main__":
    main()
