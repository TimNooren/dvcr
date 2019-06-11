
import unittest
import datetime

import vertica_python

from dvcr.containers import Vertica


class TestVertica(unittest.TestCase):

    network = None
    vertica = None

    @classmethod
    def setUpClass(cls):

        cls.vertica = (
            Vertica()
            .wait()
            .create_table(
                schema="my_schema",
                table="my_table",
                columns=[("name", "varchar(255)"), ("loadTs", "timestamp")],
            )
            .copy(source_file_path="test/vertica/records.csv", schema="my_schema", table="my_table")
        )

    @classmethod
    def tearDownClass(cls):
        cls.vertica.delete()
        pass

    def test_vertica(self):

        with vertica_python.connect(host="localhost", database="docker", user="dbadmin", password="", port=5433) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM my_schema.my_table;")
            self.assertEqual(cursor.fetchone(), ["banana", datetime.datetime(2019, 1, 1, 0, 0)])
