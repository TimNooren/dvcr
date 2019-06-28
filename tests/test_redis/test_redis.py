import unittest

import redis

from dvcr.containers import Redis


class TestRedis(unittest.TestCase):

    redis = None

    @classmethod
    def setUpClass(cls):

        cls.redis = (
            Redis()
            .wait()
            .set("mykey", "banana")
        )

    @classmethod
    def tearDownClass(cls):
        cls.redis.delete()

    def test_redis(self):
        r = redis.Redis(host='localhost', port=6379, db=0)

        self.assertEqual(r.get(name="mykey").decode("utf8"), "banana")


if __name__ == "__main__":
    unittest.main()
