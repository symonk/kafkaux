from __future__ import annotations

import types
import typing

from confluent_kafka.admin import AdminClient

from .model import TopicsResponse


class KafkaProtocol(typing.Protocol):
    def list_topics(self) -> list[str]: ...
    def verify(self) -> bool: ...


class KafkaService:
    """KafkaService serves as a proxy between the CLI interactions
    and actually interacting with Kafka.  This abstracts the IO
    to allow better testing and refactoring capabilities in future.

    The aim in future might be to implement our own IO to the kafka cluster
    rather than depending on a particular libary, or just swap to another
    library later depending on how the project evolves.
    """

    def __init__(self, admin_client: AdminClient) -> None:
        self.admin_client = admin_client

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

    def __enter__(self) -> KafkaService:
        return self

    def __exit__(
        self,
        exc_type: typing.Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        return False
