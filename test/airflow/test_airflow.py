import unittest

from dvcr.containers import Airflow

import os


class TestAirflow(unittest.TestCase):

    airflow = None

    @classmethod
    def setUpClass(cls):

        cls.airflow = (
            Airflow(tag="latest", dags_folder=os.path.abspath("test/airflow/dags"))
            .wait()
            .trigger_dag("my_dag")
        )

    # @classmethod
    # def tearDownClass(cls):
    #     cls.airflow.delete()

    def test_func(self):
        pass
