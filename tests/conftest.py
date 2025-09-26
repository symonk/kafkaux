import pathlib
import subprocess
import typing

import pytest
from confluent_kafka.admin import AdminClient
from testcontainers.kafka import KafkaContainer

from kafkaux.kafka.service import KafkaService

# Avoid a bump to testcontainers itself causing the version of
# kafka to change for tests, this should be explicitly controlled.
KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.0"


@pytest.fixture
def ensure_docker() -> None:
    """ensure_docker is a utility fixture that is used by
    other fixtures utilising text containers that provide
    a more helpful error to the user when docker is not
    running on their system.
    """
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.exit(
            "docker was not installed or running, make docker `docker info` works locally first."
        )


@pytest.fixture
def fx_kafka(
    ensure_docker: None, request: pytest.FixtureRequest
) -> typing.Generator[KafkaService, None, None]:
    """fx_kafka yields a new `kafkaux.KafkaService` that is wired into a function scoped
    isolated kafka instance."""
    with KafkaContainer(image=KAFKA_IMAGE).with_kraft() as kafka:
        admin_client = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
        with KafkaService(admin_client=admin_client) as service:
            yield service


@pytest.fixture()
def fx_simple_config_path(tmp_path) -> pathlib.Path:
    contents = r"""
[librdkafka]
foo=bar
hello=world
"""
    f = tmp_path / "kafkaux.ini"
    f.write_text(contents)
    return f
