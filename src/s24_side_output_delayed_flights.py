# src/s24_side_output_delayed_flights.py

import os
import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from pyflink.common import Types, Row
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    ProcessFunction,
    OutputTag,
)
from pyflink.common import WatermarkStrategy

from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

def parse_args() -> argparse.Namespace:
    """
    Parse command‐line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Route delayed flights to a side output"
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
        help="Kafka topic containing flight JSON"
    )
    return parser.parse_args()


def is_delayed(record: Dict[str, Any]) -> bool:
    """
    Determines if a flight is delayed.
    We consider a flight delayed if its 'departure_time' timestamp
    is more than 30 minutes before now and it is not marked as departed.
    Adjust logic to fit your actual schema.

    Args:
        record: A dict with at least 'departure_time' (ISO string) and optionally 'status'.

    Returns:
        True if flight is delayed, False otherwise.
    """
    # Validate the fields
    if "departure_time" not in record:
        return False

    try:
        scheduled_dt = datetime.fromisoformat(
            record["departure_time"].replace("Z", "+00:00")
        )
    except Exception:
        return False

    now_utc = datetime.utcnow()
    # If scheduled to depart > 30 minutes ago, flag as delayed.
    return scheduled_dt + timedelta(minutes=30) < now_utc and record.get("status") != "departed"


class DelayRouter(ProcessFunction):
    """
    A ProcessFunction that inspects each flight event. If the flight
    is delayed, emits it to a side output; otherwise emits to main output.
    """

    def __init__(self):
        # Define a side‐output tag for delayed flights
        self.delayed_tag = OutputTag("delayed-flights", Types.STRING())

    def open(self, runtime_context):
        # No special initialization needed here
        pass

    def process_element(
        self,
        value: Row,
        ctx: ProcessFunction.Context,
        out: 'Collector[Row]'
    ) -> None:
        """
        Called for each incoming Row. We expect value[0] to be a JSON string.
        """
        raw_json: str = value[0]
        try:
            record: Dict[str, Any] = json.loads(raw_json)
        except json.JSONDecodeError:
            # If JSON is invalid, drop or log. Here we drop.
            return

        if is_delayed(record):
            # Emit the raw JSON (string) to the side output
            ctx.output(self.delayed_tag, raw_json)
        else:
            # Emit to the regular output
            out.collect(raw_json)

def main() -> None:
    """
    Entry point for the delayed‐flights side‐output example.
    """
    args = parse_args()
    bootstrap_servers: str = args.bootstrap_servers
    topic: str = args.topic

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # 1. Build Kafka source (values are JSON strings).
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id("flink_side_output_group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(lambda x: x.decode("utf-8"))
        .build()
    )

    # 2. Ingest the raw stream
    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="flight_json_source"
    )

    # 3. Apply our ProcessFunction to route delayed flights
    router = DelayRouter()

    processed_stream = raw_stream.process(
        router,
        output_type=Types.STRING()
    )

    # 4. Extract the delayed side output
    delayed_tag = router.delayed_tag
    delayed_stream = processed_stream.get_side_output(delayed_tag)

    # 5. Print both streams
    processed_stream.print("ON_TIME")
    delayed_stream.print("DELAYED")

    # 6. Execute
    env.execute("s24_side_output_delayed_flights")


if __name__ == "__main__":
    main()
