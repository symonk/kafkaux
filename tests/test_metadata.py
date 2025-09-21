from kafkaux.kafka.service import KafkaService


def test_start_kafka_placeholder(fx_kafka: KafkaService):
    topics = fx_kafka.list_topics()
    assert topics == []
