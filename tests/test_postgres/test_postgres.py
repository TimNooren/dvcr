
import datetime
import os
import unittest

import psycopg2
from docker.errors import DockerException

from dvcr.containers import Postgres


class TestPostgres(unittest.TestCase):

    postgres = None

    @classmethod
    def setUpClass(cls):

        script_dir = os.path.dirname(os.path.realpath(__file__))

        cls.postgres = (
            Postgres()
            .wait()
            .create_table(
                schema="my_schema",
                table="my_table",
                columns=[("name", "VARCHAR(255)"), ("loadTs", "TIMESTAMP")],
            )
            .copy(
                schema="my_schema",
                table="my_table",
                path_or_str=os.path.join(script_dir, "records.csv"),
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.postgres.delete()

    def test_postgres(self):

        conn = psycopg2.connect(user="postgres", host="localhost", port="5432")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_schema.my_table;")
        self.assertEqual(cursor.fetchall()[0], ('banana', datetime.datetime(2019, 1, 1, 0, 0, 0)))

        conn.close()

    def test_copy_from_buf(self):

        self.postgres.copy(
                schema="my_schema",
                table="my_table",
                path_or_str="apple,2019-06-16 11:55:00",
            )

        conn = psycopg2.connect(user="postgres", host="localhost", port="5432")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_schema.my_table;")
        self.assertEqual(cursor.fetchall()[1], ('apple', datetime.datetime(2019, 6, 16, 11, 55, 0)))

        conn.close()

    def test_table_already_exists_error(self):

        schema = "my_schema"
        table = "my_duplicate_table"
        columns = [("col", "VARCHAR(1)")]

        self.postgres.create_table(schema=schema, table=table, columns=columns)

        with self.assertRaises(DockerException):
            self.postgres.create_table(schema=schema, table=table, columns=columns)
