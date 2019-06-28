import time
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
                topic="test_topic", path_or_buf="1234|banana\n123✨|báñänà\n"*15000, key_separator="|"
            )
        )

        time.sleep(10)

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
            bootstrap_servers="localhost:9092",
            group_id=None,
            consumer_timeout_ms=5000,
            auto_offset_reset="earliest",
        )

        consumer.subscribe(topics=["test_topic"])

        batch = consumer.poll(timeout_ms=5000)

        topic_partition = kafka.TopicPartition(topic="test_topic", partition=0)

        message_0 = batch[topic_partition][0]

        self.assertEqual(message_0.key.decode("utf8"), "1234")
        self.assertEqual(message_0.value.decode("utf8"), "banana")

        message_1 = batch[topic_partition][1]

        self.assertEqual(message_1.key.decode("utf8"), "123✨")
        self.assertEqual(message_1.value.decode("utf8"), "báñänà")

    def test_num_records(self):

        consumer = kafka.KafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id=None,
            consumer_timeout_ms=5000,
            auto_offset_reset="earliest",
        )

        consumer.subscribe(topics=["test_topic"])

        i = 0
        for _ in consumer:
            i += 1

        self.assertEqual(30000, i)


if __name__ == '__main__':
    unittest.main()
