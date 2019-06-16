from dvcr.containers.base import BaseContainer
from dvcr.containers.postgres import Postgres

from docker.errors import ContainerError


class Airflow(BaseContainer):
    def __init__(
        self,
        repo="apache/airflow",
        tag="latest",
        port=8080,
        dags_folder=None,
        backend=None,
        network=None,
        environment=None,
    ):
        """ Constructor for Airflow """
        super(Airflow, self).__init__(network=network, port=port, repo=repo, tag=tag)

        if backend:
            self.backend = backend
        else:
            postgres_env = {
                "POSTGRES_USER": "airflow",
                "POSTGRES_PASSWORD": "airflow",
                "POSTGRES_DB": "airflow",
            }
            self.backend = Postgres(
                network=network, tag=tag, environment=postgres_env
            ).wait()

        airflow_env = environment or {}
        airflow_env["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = self.backend.sql_alchemy_conn

        try:
            self.init_db(repo=repo, tag=tag, environment=airflow_env)
        except ContainerError as e:
            print(e.stderr.decode("utf8"))
            import sys

            sys.exit(1)

        self.create_admin_user(repo=repo, tag=tag, environment=airflow_env)

        self.start_scheduler(
            repo=repo, tag=tag, environment=airflow_env, dags_folder=dags_folder
        )

        self._container = self._client.containers.run(
            image=repo + ":" + tag,
            detach=True,
            name="airflow_webserver",
            network=self._network.name,
            environment=airflow_env,
            command=["webserver"],
            ports={port: port},
            volumes={dags_folder: {"bind": "/home/airflow/airflow/dags", "mode": "ro"}},
        )

    def init_db(self, repo, tag, environment):

        self._client.containers.run(
            image=repo + ":" + tag,
            detach=False,
            name="airflow_initdb",
            network=self._network.name,
            environment=environment,
            command=["initdb"],
            auto_remove=False,
        )

    def create_admin_user(self, repo, tag, environment):

        self._client.containers.run(
            image=repo + ":" + tag,
            detach=False,
            name="airflow_create_user",
            network=self._network.name,
            environment=environment,
            command=[
                "users",
                "-c",
                "--username",
                "admin",
                "--password",
                "admin",
                "--role",
                "Admin",
                "--firstname",
                "admin",
                "--lastname",
                "admin",
                "--email",
                "admin@admin.com,",
            ],
            auto_remove=True,
        )

    def start_scheduler(self, repo, tag, environment, dags_folder):

        self._client.containers.run(
            image=repo + ":" + tag,
            detach=True,
            name="airflow_scheduler",
            network=self._network.name,
            environment=environment,
            command=["scheduler"],
            ports={8080: 8081},
            volumes={dags_folder: {"bind": "/home/airflow/airflow/dags", "mode": "ro"}},
        )

    def trigger_dag(self, dag):

        self.unpause_dag(dag)

        self.exec(cmd=["airflow", "trigger_dag", dag])

        return self

    def unpause_dag(self, dag):

        self.exec(cmd=["airflow", "unpause", dag])

    def delete(self):
        self.backend.delete()
        self._container.stop()
        self._container.remove()
