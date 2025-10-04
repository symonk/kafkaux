from __future__ import annotations

import json

from confluent_kafka import Message
from pydantic import BaseModel
from pydantic import Field


class TopicsResponse:
    """TopicsResponse encapsulates the response from querying
    kafka for the list of topics available."""


class Event(BaseModel):
    """ReportableMessage encapsulates a message read from a kafka partition
    including metadata about the particular message.  Kafkaux filters are
    passed an instance of ReportableMessage."""

    topic: str = Field(None)
    partition: int
    offset: int
    key: str
    headers: list[tuple[str, str]] | None = Field(default_factory=list)
    value: dict[str, str] | None = Field(default_factory=dict)

    @classmethod
    def from_confluent_message(cls, message: Message) -> Event:
        """from_confluent_message constructs a ReportableMessage from
        a kafka message read from a topic."""
        return cls(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=message.key().decode("utf-8"),
            headers=message.headers(),
            value=json.loads(message.value().decode("utf-8")),
        )
