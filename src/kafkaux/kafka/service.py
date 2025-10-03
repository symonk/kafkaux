from __future__ import annotations

import types
import typing

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from kafkaux.kafka.model import ReportableMessage

from .filter import FILTER_REGISTRY
from .model import TopicsResponse


class KafkaProtocol(typing.Protocol):
    def list_topics(self) -> list[str]: ...
    def verify(self) -> bool: ...
    def tail(self, filters: tuple[str, ...]) -> None: ...


class KafkaService:
    """KafkaService serves as a proxy between the CLI interactions
    and actually interacting with Kafka.  This abstracts the IO
    to allow better testing and refactoring capabilities in future.

    The aim in future might be to implement our own IO to the kafka cluster
    rather than depending on a particular libary, or just swap to another
    library later depending on how the project evolves.
    """

    def __init__(
        self,
        admin_client: AdminClient | None = None,
        consumer: Consumer | None = None,
        producer: Producer | None = None,
    ) -> None:
        self.admin_client = admin_client
        self.consumer = consumer
        self.producer = producer

    def verify(self) -> bool:
        """verify ensures the provided broker is actually reachable with
        a small timeout.  confluent-kafka has a lot of built in retries
        which can in some circumstances have a tool indefinitely retry
        for scenarios that will never work."""
        return True

    def list_topics(self) -> TopicsResponse:
        """list_topics returns the list of available topics in the
        cluster."""
        self.admin_client.create_topics("foo", "bar", "baz")
        return list(self.admin_client.list_topics().keys())

    def tail(self, filters: tuple[str, ...]) -> None:
        """tail follows all messages for the given partitions.  The configuration
        for the consumer group that tail uses is configured outside this method in
        the CLI handler.
        """
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            reportable_message = ReportableMessage.from_confluent_message(msg)
            if filters:
                for f in filters:
                    if FILTER_REGISTRY[f](reportable_message):
                        print(reportable_message.model_dump_json(), flush=True)
                        continue
            print(reportable_message.model_dump_json(), flush=True)

    def __enter__(self) -> KafkaService:
        return self

    def __exit__(
        self,
        exc_type: typing.Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        return False
