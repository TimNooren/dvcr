import os
import time
import unittest

from dvcr.containers import Airflow, AirflowWorker, Postgres, Redis, RabbitMQ


class TestAirflowDefault(unittest.TestCase):

    airflow = None

    @classmethod
    def setUpClass(cls):

        scripts_dir = os.path.dirname(os.path.realpath(__file__))

        cls.airflow = (
            Airflow(tag="latest-3.6", dags_folder=os.path.join(scripts_dir, "dags"))
            .wait()
            .trigger_dag("my_dag")
        )

    @classmethod
    def tearDownClass(cls):
        cls.airflow.delete()

    def test_dag_result(self):

        time.sleep(20)
        exit_code, output = self.airflow.scheduler.exec_run(
            cmd=["cat", "/home/airflow/my_file.txt"]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(output.decode("utf8"), "Hello World!!")


class TestAirflowCeleryRedis(unittest.TestCase):

    airflow = None
    airflow_worker = None
    postgres = None
    redis = None

    @classmethod
    def setUpClass(cls):

        postgres_env = {
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
            "POSTGRES_DB": "airflow",
        }

        cls.postgres = Postgres(environment=postgres_env).wait()

        cls.redis = Redis().wait()

        airflow_environment = {
            "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN": cls.postgres.sql_alchemy_conn(
                dialect="postgres", driver="psycopg2"
            ),
            "AIRFLOW__CELERY__BROKER_URL": cls.redis.sql_alchemy_conn(),
            "AIRFLOW__CELERY__RESULT_BACKEND": cls.postgres.sql_alchemy_conn(
                dialect="db", driver="postgres"
            ),
        }

        scripts_dir = os.path.dirname(os.path.realpath(__file__))
        dags_folder = os.path.join(scripts_dir, "dags")

        cls.airflow_worker = AirflowWorker(
           environment=airflow_environment, dags_folder=dags_folder
        ).wait()

        cls.airflow = (
            Airflow(
                tag="latest-3.6",
                dags_folder=dags_folder,
                environment=airflow_environment,
                backend=cls.postgres,
            )
            .wait()
            .trigger_dag("my_dag")
        )

    @classmethod
    def tearDownClass(cls):
        cls.airflow_worker.delete()
        cls.airflow.delete()
        cls.redis.delete()
        cls.postgres.delete()

    def test_dag_result(self):

        time.sleep(80)
        exit_code, output = self.airflow_worker.exec_run(
            cmd=["cat", "/home/airflow/my_file.txt"]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(output.decode("utf8"), "Hello World!!")


class TestAirflowCeleryRabbitMQ(unittest.TestCase):

    airflow = None
    airflow_worker = None
    postgres = None
    rabbitmq = None

    @classmethod
    def setUpClass(cls):

        postgres_env = {
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
            "POSTGRES_DB": "airflow",
        }

        cls.postgres = Postgres(environment=postgres_env).wait()

        cls.rabbitmq = RabbitMQ().wait()

        airflow_environment = {
            "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN": cls.postgres.sql_alchemy_conn(
                dialect="postgres", driver="psycopg2"
            ),
            "AIRFLOW__CELERY__BROKER_URL": cls.rabbitmq.sql_alchemy_conn(),
            "AIRFLOW__CELERY__RESULT_BACKEND": cls.postgres.sql_alchemy_conn(
                dialect="db", driver="postgres"
            ),
        }

        scripts_dir = os.path.dirname(os.path.realpath(__file__))
        dags_folder = os.path.join(scripts_dir, "dags")

        cls.airflow_worker = AirflowWorker(
           environment=airflow_environment, dags_folder=dags_folder
        ).wait()

        cls.airflow = (
            Airflow(
                tag="latest-3.6",
                dags_folder=dags_folder,
                environment=airflow_environment,
                backend=cls.postgres,
            )
            .wait()
            .trigger_dag("my_dag")
        )

    @classmethod
    def tearDownClass(cls):
        cls.airflow_worker.delete()
        cls.airflow.delete()
        cls.rabbitmq.delete()
        cls.postgres.delete()

    def test_dag_result(self):

        time.sleep(80)
        exit_code, output = self.airflow_worker.exec_run(
            cmd=["cat", "/home/airflow/my_file.txt"]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(output.decode("utf8"), "Hello World!!")


if __name__ == "__main__":
    unittest.main()
