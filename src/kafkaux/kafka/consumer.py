import uuid
from enum import StrEnum
from enum import unique

from confluent_kafka import Consumer

from kafkaux.config import Configuration
from kafkaux.kafka.filter import FILTER_REGISTRY
from kafkaux.kafka.model import Event


@unique
class ConsumerMode(StrEnum):
    TAIL = "tail"


class KafkauxConsumer:
    """KafkauxConsumer encapsulates all consuming style activities."""
    def __init__(self,
                 kconfig: Configuration,
                 mode: ConsumerMode,
                 topics: list[str],
                 filters: dict[str, str] | None = None,) -> None:
        self.kconfig = kconfig
        self.topics = topics
        self.filters = filters
        self.mode = mode
        # update the core config librdkafka settings provided by the user
        # with enforced changes based on the mode.
        self.kconfig.librdkafka.update(**self.kafka_overrides)
        # establish the consumer
        self.consumer = Consumer(self.kconfig.librdkafka)
        self.consumer.subscribe(self.topics)

    @property
    def kafka_overrides(self) -> dict[str, str]:
        """kafka_config calculates the configuration properties
        and overrides based on how the consumer was initialized."""
        consumer_group = f"kafkaux-{uuid.uuid4()}"
        return {
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }


    def tail(self) -> None:
        """tail monitors messages for the particular topics specified
        on the command line, writing the output to the configured output
        stream.  If filters are set, tail ignores messages that do not
        match predicate filter functions."""
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            reportable_message = Event.from_confluent_message(msg)
            if self.filters:
                for f in self.filters:
                    if FILTER_REGISTRY[f](reportable_message):
                        print(reportable_message.model_dump_json(), flush=True)
                        continue
            print(reportable_message.model_dump_json(), flush=True)


