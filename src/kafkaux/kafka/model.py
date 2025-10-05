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
    value: str | None

    @classmethod
    def from_confluent_message(cls, message: Message) -> Event:
        """from_confluent_message constructs a ReportableMessage from
        a kafka message read from a topic."""
        return cls(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=try_json_decode(message.key()),
            headers=message.headers(),
            # TODO: The key might not actually be valid json, can be TEXT etc.
            value=try_json_decode(message.value()),
        )


# TODO: Maybe a key and value individual functions is preferred, definitely
# TODO: different use cases there
def try_json_decode(data_in: bytes) -> str | None:
    """attempts to decode the kafka message initially as json
    falling back to text.  Kafka supports multiple different
    key formats, such as:

    * Null
    * JSON
    * Text
    * Binary (Base64)

    # TODO: B64 keys need handled.
    """
    if data_in is None:
        return None
    try:
        return json.loads(data_in.decode("utf-8"))
    except json.decoder.JSONDecodeError:
        return data_in.decode("utf-8")
