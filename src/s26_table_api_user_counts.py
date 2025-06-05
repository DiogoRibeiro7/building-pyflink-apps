# src/s26_table_api_user_counts.py

import os
import argparse
import json
from datetime import datetime
from typing import Dict, Any

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.table import TableEnvironment, DataTypes
from pyflink.table.descriptors import (
    Schema,
    Kafka as KafkaDescriptor,
    Json as JsonDescriptor,
    FormatDescriptor,
)
from pyflink.table.window import Tumble

def parse_args() -> argparse.Namespace:
    """
    Parse command‐line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Simple Table API example: count distinct users per 5‐minute window"
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
        default="user_events",
        help="Kafka topic containing user events JSON"
    )
    return parser.parse_args()


def main() -> None:
    """
    Entry point: read user event JSON from Kafka, register as a table,
    and run a windowed SQL query to count distinct user_ids over 5‐minute tumbling windows.
    """
    args = parse_args()
    bootstrap_servers: str = args.bootstrap_servers
    topic: str = args.topic

    # 1. Set up Stream + Table environments
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    t_env = TableEnvironment.create(env)

    # 2. Define a Kafka dynamic table for JSON user events
    #    We expect each message to be a JSON object with fields:
    #     - user_id (STRING), event_time (ISO‐format string), action (STRING)
    t_env.create_temporary_table(
        "UserEvents",
        KafkaDescriptor()
        .version("universal")  # works for Kafka 0.10+
        .property("bootstrap.servers", bootstrap_servers)
        .property("topic", topic)
        .property("scan.startup.mode", "earliest-offset")
        .start_from_earliest()
        .format(
            JsonDescriptor()
            .fail_on_missing_field(False)
            .schema(
                Schema()
                .field("user_id", DataTypes.STRING())
                .field("event_time", DataTypes.STRING())
                .field("action", DataTypes.STRING())
                # Define computed column for rowtime
                .field("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
            )
        )
        .option("properties.group.id", "flink_table_api_group")
        .build()
    )

    # 3. Define a simple SQL query: count distinct users per 5‐minute tumbling window
    query = """
        SELECT
            TUMBLE_START(rowtime, INTERVAL '5' MINUTE) AS window_start,
            COUNT(DISTINCT user_id) AS unique_users
        FROM UserEvents
        GROUP BY TUMBLE(rowtime, INTERVAL '5' MINUTE)
    """

    result_table = t_env.sql_query(query)

    # 4. Convert the result table back to a DataStream and print it
    t_env.to_append_stream(
        result_table,
        Types.ROW([Types.SQL_TIMESTAMP(), Types.LONG()])
    ).print()

    # 5. Execute
    env.execute("s26_table_api_user_counts")


if __name__ == "__main__":
    main()
