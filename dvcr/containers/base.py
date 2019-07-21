import io
import os
import sys
import tarfile
import time
from typing import Optional, Union, List, Callable

import docker
from docker.errors import APIError, DockerException
from docker.utils.socket import frames_iter

from dvcr.utils import init_logger, bright, resolve_path_or_buf
from dvcr.network import DefaultNetwork, Network

if sys.platform == "win32":
    import win32pipe


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

        for i in range(60):
            status = self._client.api.inspect_container(self._container.id)["State"][
                "Health"
            ]["Status"]

            if status == "healthy":
                self._logger.info("%s is up! 🚀", bright(self.name))
                return self

            time.sleep(1)

        raise DockerException()

    def exec(
        self,
        cmd: List[str],
        path_or_buf: Union[str, bytes, None] = None,
        delay_secs: int = 0,
    ):

        stdin = None

        if path_or_buf:
            stdin = resolve_path_or_buf(path_or_buf=path_or_buf)

        result = self._client.api.exec_create(container=self.id, cmd=cmd, stdin=True)
        exec_id = result["Id"]

        socket = self._client.api.exec_start(exec_id=exec_id, detach=False, socket=True)

        while stdin:
            if hasattr(socket, "send"):
                n_bytes_written = socket.send(string=stdin)
            else:
                n_bytes_written = os.write(socket.fileno(), stdin)

            self._logger.debug("Written %s bytes", n_bytes_written)

            stdin = stdin[n_bytes_written:]

            if not stdin:
                break

        time.sleep(delay_secs)

        read_buffer = ""

        peek_max_tries = 3
        peek_tries = 0

        for stream, frame in frames_iter(socket=socket, tty=False):

            read_buffer += frame.decode("utf8")

            if read_buffer.endswith("\n"):
                self._logger.info(read_buffer.strip("\n"))
                read_buffer = ""

            while peek_tries <= peek_max_tries:

                if sys.platform == "win32":
                    n_bytes_left = win32pipe.PeekNamedPipe(socket._handle, 2)[1]
                else:
                    print(socket.readable())
                    n_bytes_left = 1

                if not n_bytes_left:
                    self._logger.debug("No bytes left to read. Retrying...")
                    self._logger.debug("peek_tries: %s", peek_tries)
                    peek_tries += 1
                    time.sleep(1)
                    continue

                peek_tries = 0
                break

            if n_bytes_left <= 0:
                self._logger.debug("No more bytes left to read")

                if read_buffer:
                    self._logger.info(read_buffer.strip("\n"))

                break

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

    def copy(self, path_or_buf, target_path, user="root", group="root", mode="0600"):

        target_dir, filename = os.path.split(target_path)

        data = resolve_path_or_buf(path_or_buf)

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
