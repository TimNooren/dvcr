
import datetime
import unittest

import psycopg2

from dvcr import Network
from dvcr.containers import Postgres


class TestPostgres(unittest.TestCase):

    postgres = None

    @classmethod
    def setUpClass(cls):

        cls.postgres = (
            Postgres()
            .wait()
            .create_table(
                schema="my_schema",
                table="my_table",
                columns=[("name", "varchar(255)"), ("loadTs", "timestamp")],
            )
            .copy(
                source_file_path="test/postgres/records.csv",
                schema="my_schema",
                table="my_table",
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.postgres.delete()

    def test_postgres(self):

        conn = psycopg2.connect(user="postgres", host="localhost", port="5432")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_schema.my_table;")
        self.assertEqual(cursor.fetchall()[0], ('banana', datetime.datetime(2019, 1, 1, 0, 0)))

        conn.close()
