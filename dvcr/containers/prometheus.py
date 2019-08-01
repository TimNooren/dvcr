
from typing import Optional

import yaml

from dvcr.containers import BaseContainer
from dvcr.network import Network


class Prometheus(BaseContainer):
    def __init__(
        self,
        repo: str = "prom/prometheus",
        tag: str = "latest",
        port: int = 9090,
        name: str = "prometheus",
        network: Optional[Network] = None,
    ):
        super(Prometheus, self).__init__(
            port=port, repo=repo, tag=tag, name=name, network=network
        )

        self.port = port

        self._config = {
            "global": {"scrape_interval": "15s", "evaluation_interval": "15s"},
            "alerting": {"alertmanagers": [{"static_configs": [{"targets": None}]}]},
            "scrape_configs": [
                {
                    "job_name": "prometheus",
                    "static_configs": [{"targets": ["prometheus:9090"]}],
                }
            ],
        }

        self._container = self._client.containers.run(
            image=repo + ":" + tag,
            detach=True,
            hostname=name,
            healthcheck={
                "test": ["CMD", "wget", "-S", "-O", "-", name + ":9090/-/healthy"],
                "interval": 1000000000,
            },
            name=name,
            network=self._network.name,
            ports={port: port},
        )

    def _update_config(self):

        data = yaml.dump(self._config).encode("utf8")

        self.copy(path_or_str=data, target_path="/etc/prometheus/prometheus.yml")

        self.exec(cmd=["kill", "-HUP", "1"])

    def add_scrape_config(self, scrape_config):
        
        self._config["scrape_configs"].append(scrape_config)
        
        self._update_config()
