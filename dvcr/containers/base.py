import io
import os
import tarfile
import time
from typing import Optional, Union, List, Callable

import docker
from docker.errors import APIError, DockerException

from dvcr.network import DefaultNetwork, Network
from dvcr.socket import simpler_socket
from dvcr.utils import init_logger, bright, resolve_path_or_str


class BaseContainer(object):
    def __init__(
        self,
        repo: str,
        tag: str,
        name: str,
        port: Optional[int] = None,
        network: Optional[Network] = None,
    ):

        self._logger = init_logger(name=name)

        self.port = port

        self._network = network or DefaultNetwork()
        self._client = docker.from_env()

        self._logger.info("Pulling %s:%s", bright(repo), tag)

        try:
            image = self._client.images.pull(repository=repo, tag=tag)
            self._logger.info("Pulled image %s:%s (%s)", bright(repo), tag, image.id)
        except APIError:
            self._logger.info("Could not pull %s:%s", bright(repo), tag)
            image = self._client.images.get(name=repo + ":" + tag)
            self._logger.info("Found %s:%s locally (%s)", bright(repo), tag, image.id)

        self.post_wait_hooks = []

    def register_post_wait_hook(self, fn: Callable, *args, **kwargs):

        self.post_wait_hooks.append([fn, args, kwargs])

    def wait(self):

        self._logger.info("Waiting for %s ⏳", bright(self.name))

        for i in range(300):
            inspect_result = self._client.api.inspect_container(self._container.id)

            if inspect_result["State"]["Health"]["Status"] == "healthy":
                self._logger.info("%s is up! 🚀", bright(self.name))
                return self

            time.sleep(1)

        raise DockerException()

    def exec(
        self,
        cmd: List[str],
        path_or_str: Union[str, bytes, None] = None,
        delay_secs: int = 0,
    ):

        stdin = None

        if path_or_str:
            stdin = resolve_path_or_str(path_or_str=path_or_str)

        result = self._client.api.exec_create(container=self.id, cmd=cmd, stdin=True)
        exec_id = result["Id"]

        socket = self._client.api.exec_start(exec_id=exec_id, detach=False, socket=True)
        socket = simpler_socket(socket=socket, logger=self._logger)

        socket.write(data=stdin)

        time.sleep(delay_secs)

        for line in socket.read():
            self._logger.info(line)

        socket.close()

        exit_code = self._wait_for_cmd_completion(exec_id=exec_id)

        if exit_code != 0:
            raise DockerException("Command exited with code: {}".format(exit_code))

    def _wait_for_cmd_completion(self, exec_id):

        while True:
            result = self._client.api.exec_inspect(exec_id=exec_id)

            time.sleep(0.2)

            if not result["Running"]:
                return result["ExitCode"]

    def delete(self):
        self._container.stop()
        self._container.remove()
        self._logger.info("Deleted %s ♻", bright(self.name))

    def copy(self, path_or_str, target_path, user="root", group="root", mode="0600"):

        target_dir, filename = os.path.split(target_path)

        data = resolve_path_or_str(path_or_str)

        tarinfo = tarfile.TarInfo(name=filename)
        tarinfo.uname = user
        tarinfo.gname = group
        tarinfo.mtime = time.time()
        tarinfo.size = len(data)

        tarstream = io.BytesIO()

        with tarfile.TarFile(fileobj=tarstream, mode="w") as tar:

            tar.addfile(tarinfo, io.BytesIO(data))
            tar.close()

        tarstream.seek(0)

        success = self._container.put_archive(path=target_dir, data=tarstream)

        return success

    def __getattr__(self, item):
        return getattr(self._container, item)
