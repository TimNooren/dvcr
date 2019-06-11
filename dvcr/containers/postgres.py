import time

from dvcr.containers.base import BaseContainer


class Postgres(BaseContainer):
    def __init__(
        self, image="postgres", tag="latest", port=5432, environment=None, network=None
    ):
        """ Constructor for Postgres """
        super(Postgres, self).__init__(port=port, image=image, tag=tag, network=network)

        if not environment:
            environment = {"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": ""}

        self.user = environment.get("POSTGRES_USER", "postgres")
        self.password = environment.get("POSTGRES_PASSWORD", "")
        self.db = environment.get("POSTGRES_DB", "postgres")

        self._container = self._client.containers.run(
            image=image + ":" + tag,
            detach=True,
            name="postgres",
            network=self._network.name,
            ports={port: port},
            environment=environment,
        )

        self.sql_alchemy_conn = "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=self.user,
            pwd=self.password,
            host=self._container.name,
            port=self.port,
            db=self.db,
        )

    def execute_query(self, query, data=None):

        response = self._container.exec_run(
            cmd=["psql", "-U", "postgres", "-c", query],
            socket=True if data else False,
            stdin=True if data else False,
        )

        if data:
            socket = response.output
            socket.settimeout(1)
            socket.sendall(string=data)

            socket.close()

        time.sleep(1)

        return self

    def create_schema(self, name):

        self.execute_query(query="CREATE SCHEMA {};".format(name))

        return self

    def create_table(self, schema, table, columns):
        self.create_schema(name=schema)

        cols = ", ".join([col + " " + dtype for col, dtype in columns])

        self.execute_query(
            query="CREATE TABLE {schema}.{table} ({columns});".format(
                schema=schema, table=table, columns=cols
            )
        )

        return self

    def copy(self, source_file_path, schema, table):

        with open(source_file_path, "rb") as _file:

            self.execute_query(
                query="COPY {schema}.{table} FROM STDIN DELIMITER ',';".format(
                    schema=schema, table=table
                ),
                data=_file.read(),
            )

        return self