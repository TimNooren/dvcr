import datetime
import unittest

import vertica_python
from docker.errors import DockerException

from dvcr.containers import Vertica


class TestVertica(unittest.TestCase):

    vertica = None

    @classmethod
    def setUpClass(cls):

        cls.vertica = Vertica().wait()

    @classmethod
    def tearDownClass(cls):
        cls.vertica.delete()

    def test_create_new_table(self):

        self.vertica.create_table(
            schema="my_schema",
            table="my_table_1",
            columns=[("name", "VARCHAR(255)"), ("loadTs", "TIMESTAMP")],
        )

        self.vertica.copy(
            schema="my_schema",
            table="my_table_1",
            path_or_buf="string,2019-01-01 00:00:00",
            header=False,
        )

        with vertica_python.connect(
            host="localhost", database="docker", user="dbadmin", password="", port=5433
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM my_schema.my_table_1;")
            self.assertEqual(
                cursor.fetchone(), ["string", datetime.datetime(2019, 1, 1, 0, 0, 0)]
            )

    def test_create_table_schema_already_exists(self):

        self.vertica.create_table(
            schema="my_schema",
            table="my_table_2",
            columns=[("name", "VARCHAR(255)"), ("age", "INT")],
        )

        self.vertica.copy(
            schema="my_schema",
            table="my_table_2",
            path_or_buf="string,1",
            header=False,
        )

        with vertica_python.connect(
            host="localhost", database="docker", user="dbadmin", password="", port=5433
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM my_schema.my_table_2;")
            self.assertEqual(cursor.fetchone(), ["string", 1])

    def test_bad_data_raises_exception(self):

        self.vertica.create_table(
            schema="my_schema",
            table="my_table_3",
            columns=[("name", "VARCHAR(255)"), ("ts", "TIMESTAMP")],
        )

        with self.assertRaises(DockerException):
            self.vertica.copy(
                schema="my_schema",
                table="my_table_3",
                path_or_buf="string,not a timestamp",
                header=False,
            )
