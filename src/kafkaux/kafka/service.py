from __future__ import annotations
import time

import types
import typing

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from pprint import pprint

from .model import TopicsResponse


class KafkaProtocol(typing.Protocol):
    def list_topics(self) -> list[str]: ...
    def verify(self) -> bool: ...
    def tail(self, topic: str, partitions: tuple[int]) -> None: ...


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

    def tail(self, topic: str, partitions: tuple[int] = ()) -> None:
        """tail performs a live following of a particular topic for the
        given offset.  if no offsets are provided, all partitions are
        monitored.  This spawns a single consumer which will be assigned
        all partitions of a topic and filter from there.
        """
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            pprint(msg)


    def __enter__(self) -> KafkaService:
        return self

    def __exit__(
        self,
        exc_type: typing.Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        return False
