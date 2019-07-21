import os
import datetime
import unittest

import mysql.connector

from dvcr.containers import MySQL


class TestMySQL(unittest.TestCase):

    mysql = None

    @classmethod
    def setUpClass(cls):

        script_dir = os.path.dirname(os.path.realpath(__file__))

        cls.mysql = (
            MySQL()
            .wait()
            .create_table(
                database="my_db",
                table="my_table",
                columns=[("name", "VARCHAR(255)"), ("loadTs", "TIMESTAMP")],
            )
            .load_data(
                database="my_db",
                table="my_table",
                path_or_buf=os.path.join(script_dir, "records.csv"),
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


if __name__ == '__main__':
    unittest.main()
