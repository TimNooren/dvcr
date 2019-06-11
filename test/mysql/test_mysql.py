
import datetime
import unittest

import mysql.connector

from dvcr.containers import MySQL


class TestMySQL(unittest.TestCase):

    mysql = None

    @classmethod
    def setUpClass(cls):

        cls.mysql = (
            MySQL()
            .wait()
            .create_table(
                database="my_db",
                table="my_table",
                columns=[("name", "varchar(255)"), ("loadTs", "timestamp")],
            )
            .load_data(
                source_file_path="test/mysql/records.csv",
                database="my_db",
                table="my_table",
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.mysql.delete()

    def test_mysql(self):

        config = {
            'user': 'root',
            'password': 'root',
            'host': 'localhost',
            'database': 'my_db'
        }

        cnx = mysql.connector.connect(**config)
        cur = cnx.cursor()
        cur.execute('USE my_db;')
        cur.execute('SELECT * FROM my_table;')

        self.assertEqual(cur.fetchall()[0], ('banana', datetime.datetime(2019, 1, 1, 0, 0)))
