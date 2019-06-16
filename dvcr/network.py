import uuid

import colorama
import docker
from docker.errors import NotFound, APIError

from dvcr.utils import init_logger


class Network(object):
    def __init__(self, name, driver="bridge"):
        """ Constructor for Network """
        super(Network, self).__init__()

        self.logger = init_logger(name="network")

        self.name = name

        client = docker.from_env()

        self.network = client.networks.create(name=name, driver=driver)

        self.logger.info("Created network %s", colorama.Style.BRIGHT + self.name)

    def delete(self):
        self.network.remove()
        self.logger.info("Deleted network %s ♻", colorama.Style.BRIGHT + self.name)


class DefaultNetwork(object):

    network = None

    def __init__(self, driver: str = "bridge"):
        if not DefaultNetwork.network:
            DefaultNetwork.network = Network(
                name=self._generate_network_name(), driver=driver
            )

    def __getattr__(self, name):
        return getattr(self.network, name)

    @staticmethod
    def _generate_network_name() -> str:
        return "dvcr_network_" + (uuid.uuid4().hex[:8])

    def delete(self):

        try:
            self.network.delete()
        except (NotFound, APIError):
            pass

    def __del__(self):
        self.delete()
