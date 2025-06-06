import os
import logging

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")


if __name__ == "__main__":
    """
    ## local execution
    python src/s08_create_source.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/s08_create_source.py -d
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # if running on local, set n_parallelism = 1
    n_parallelism = 1
    env.set_parallelism(n_parallelism)
    
    # env.add_jars("file:///D:\\Projects\\GitHub\\flink-demos-master\\building-pyflink-apps\\src\\jars\\flink-sql-connector-kafka-1.17.1.jar")

    if RUNTIME_ENV != "docker":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-sql-connector-kafka-1.17.1.jar"]
        jar_paths = tuple(
            [f"file:///{os.path.join(CURRENT_DIR, 'jars', name)}" for name in jar_files]
        )
        logging.info(f"adding local jars - {', '.join(jar_files)}")
        env.add_jars(*jar_paths)
    

    skyone_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("skyone")
        .set_group_id("group.skyone")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    skyone_stream = env.from_source(
        skyone_source, WatermarkStrategy.no_watermarks(), "skyone_source"
    )

    skyone_stream.print()

    env.execute("flight_importer")
