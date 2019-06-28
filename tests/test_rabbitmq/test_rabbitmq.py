import unittest

from dvcr.containers import RabbitMQ


class TestRabbitMQ(unittest.TestCase):

    rabbitmq = None

    @classmethod
    def setUpClass(cls):

        cls.rabbitmq = (
            RabbitMQ()
            .wait()
        )

    @classmethod
    def tearDownClass(cls):
        cls.rabbitmq.delete()

    def test_rabbitmq(self):
        pass


if __name__ == "__main__":
    unittest.main()
