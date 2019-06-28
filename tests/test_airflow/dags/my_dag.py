from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_id = "my_dag"

with DAG(
    dag_id=dag_id, start_date=datetime(2018, 11, 14), schedule_interval=None
) as dag:

    def say_hello():

        with open("/home/airflow/my_file.txt", "w") as _file:
            _file.write("Hello World!!")

    PythonOperator(task_id="say_hello", python_callable=say_hello)
