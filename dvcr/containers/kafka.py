from typing import Optional

from dvcr.containers.base import BaseContainer
from dvcr.containers.zookeeper import Zookeeper
from dvcr.network import Network


class Kafka(BaseContainer):
    """Starts a single node Kafka broker."""

    def __init__(
        self,
        repo: str = "confluentinc/cp-kafka",
        tag: str = "latest",
        port: int = 9092,
        name: str = "kafka",
        network: Optional[Network] = None,
        zookeeper: Optional[Zookeeper] = None,
    ):
        """

        :param repo:        Repo for Kafka image
        :param tag:
        :param port:
        :param name:
        :param network:
        :param zookeeper:
        """
        super(Kafka, self).__init__(
            port=port, repo=repo, tag=tag, name=name, network=network
        )

        if zookeeper:
            self.zookeeper = zookeeper
        else:
            self.zookeeper = Zookeeper(network=network, tag=tag).wait()

        self._container = self._client.containers.run(
            image=repo + ":" + tag,
            environment={
                "KAFKA_BROKER_ID": 1,
                "KAFKA_ZOOKEEPER_CONNECT": self.zookeeper.name
                + ":"
                + str(self.zookeeper.port),
                "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:"
                + str(port),
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
                "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 1,
                "KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE": True,
            },
            detach=True,
            healthcheck={
                "test": ["CMD", "nc", "-z", name, str(port)],
                "interval": 1000000000,
            },
            hostname=name,
            name=name,
            network=self._network.name,
            ports={port: port},
        )

    def create_topic(self, name: str, partitions: int = 1):
        """
        :param name:            Topic name
        :param partitions:      Number of partitions
        :return:                self
        """
        self.exec(
            cmd=[
                "kafka-topics",
                "--create",
                "--zookeeper",
                "zookeeper:2181",
                "--topic",
                name,
                "--replication-factor",
                "1",
                "--partitions",
                str(partitions),
            ]
        )

        return self

    def write_records(
        self,
        topic: str,
        key_separator: Optional[str] = None,
        path_or_str: Optional[str] = None,
    ):
        """
        :param topic:           Name of topic
        :param key_separator:   Separator between key and value
        :param path_or_str:     Path to file or string containing data
        :return:                self
        """
        self._logger.info("Writing records to %s", topic)

        cmd = [
            "kafka-console-producer",
            "--broker-list",
            "kafka:9092",
            "--topic",
            topic,
            "--property",
            "parse.key={}".format(bool(key_separator)).lower(),
        ]

        if key_separator:
            cmd += ["--property", "key.separator=" + key_separator]

        self.exec(cmd=cmd, path_or_str=path_or_str, delay_secs=5)

        return self

    def delete(self):
        self.zookeeper.delete()
        super(Kafka, self).delete()
