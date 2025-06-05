from kafka import KafkaConsumer
import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume messages from a Kafka topic"
    )
    parser.add_argument(
        "--topic",
        help="Kafka topic to read from",
        default="flightdata",
    )
    return parser.parse_args()


KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:29092"


def main() -> None:
    args = parse_args()
    topic = args.topic

    print("Kafka Consumer Application Started ... ")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
            value_deserializer=lambda x: x.decode("utf-8"),
        )
        for message in consumer:
            print(type(message))
            print("Key: ", message.key)
            message = message.value
            print("Message received: ", message)
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)


if __name__ == "__main__":
    main()
