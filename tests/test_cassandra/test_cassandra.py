import unittest

from cassandra.cluster import Cluster
from docker.errors import DockerException

from dvcr.containers import Cassandra


class TestCassandra(unittest.TestCase):

    cassandra = None

    @classmethod
    def setUpClass(cls):

        cls.cassandra = (
            Cassandra()
            .wait()
            .create_keyspace(name="banana")
            .create_table(
                keyspace="banana",
                table="apple",
                columns=[("fruit", "text"), ("qty", "int")],
                primary_key=("fruit",),
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.cassandra.delete()

    def test_insert(self):

        self.cassandra.insert(
            keyspace="banana", table="apple", values={"fruit": "pears", "qty": 1}
        )

        cluster = Cluster()

        session = cluster.connect(keyspace="banana")
        rows = session.execute("SELECT * FROM banana.apple;")

        self.assertEqual(rows.one().fruit, "pears")
        self.assertEqual(rows.one().qty, 1)

    def test_duplicate_keyspace(self):

        with self.assertRaises(DockerException):
            self.cassandra.create_keyspace(name="banana")
