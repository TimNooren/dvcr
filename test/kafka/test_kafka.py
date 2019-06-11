import unittest

import kafka

from dvcr.containers import Kafka


class TestKafka(unittest.TestCase):

    kafka = None

    @classmethod
    def setUpClass(cls):

        cls.kafka = (
            Kafka()
            .wait()
            .write_records(
                topic="test_topic", source_file_path="test/kafka/kafka_records.txt"
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.kafka.delete()

    def test_topic_creation(self):
        consumer = kafka.KafkaConsumer(
            bootstrap_servers="localhost:9092", group_id=None
        )

        self.assertTrue("test_topic" in consumer.topics())

    def test_record_format(self):

        consumer = kafka.KafkaConsumer(
            "test_topic",
            bootstrap_servers="localhost:9092",
            group_id=None,
            consumer_timeout_ms=5000,
        )

        batch = consumer.poll(timeout_ms=10000)

        topic_partition = kafka.TopicPartition(topic="test_topic", partition=0)

        message_0 = batch[topic_partition][0]

        self.assertEqual(message_0.key.decode("utf8"), "1234")
        self.assertEqual(message_0.value.decode("utf8"), "banana")

        message_1 = batch[topic_partition][1]

        self.assertEqual(message_1.key.decode("utf8"), "123✨")
        self.assertEqual(message_1.value.decode("utf8"), "báñänà")
