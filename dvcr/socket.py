import sys
import time
from socket import SHUT_WR

from docker.utils.socket import frames_iter


if sys.platform == "win32":
    import win32pipe


def simpler_socket(socket, logger):
    """
    Returns the correct SimplerSocket™ based on operating system
    :param socket:              socket object returned by DockerClient().api.exec_start(socket=True)
    :param logger:              logger for logging
    :return:
    """
    if sys.platform == "win32":
        return SimplerSocketWindows(socket=socket, logger=logger)
    else:
        return SimplerSocketUnix(socket=socket, logger=logger)


class SimplerSocketWindows(object):
    """
    Wraps the socket object returned by DockerClient().api.exec_start(socket=True),
    which in turn wraps a Windows named pipe.
    """

    def __init__(self, socket, logger):
        """
        :param socket:          socket object returned by DockerClient().api.exec_start(socket=True)
        :param logger:          logger for logging
        """
        self.socket = socket
        self.logger = logger

    def write(self, data: bytes) -> None:
        """
        Writes all of data to named pipe
        :param data:            data to send (bytes)
        :return:                None
        """
        while data:
            n_bytes_written = self.socket.send(string=data)

            self.logger.debug("Written %s bytes", n_bytes_written)

            data = data[n_bytes_written:]

            if not data:
                self.logger.debug("All data written")
                break

    def read(self) -> str:
        """
        Generator yielding lines read from named pipe. Retry mechanism tries to avoid breaking
        prematurely while waiting on the other end of the pipe to send data.
        :return:                Lines read from named pipe (str)
        """
        read_buffer = ""

        max_peek_tries = 3

        for stream, frame in frames_iter(socket=self.socket, tty=False):

            read_buffer += frame.decode("utf8")

            if read_buffer.endswith("\n"):
                yield read_buffer.strip("\n")
                read_buffer = ""

            peek_tries = 0

            while peek_tries <= max_peek_tries:

                _, n_bytes_left, _ = win32pipe.PeekNamedPipe(self.socket._handle, 2)

                if n_bytes_left:
                    break
                else:
                    self.logger.debug("No bytes left to read. Retrying...")
                    self.logger.debug("peek_tries: %s", peek_tries)
                    peek_tries += 1
                    time.sleep(1)
                    continue
            else:
                self.logger.debug("No more bytes left to read")

                if read_buffer:
                    yield read_buffer.strip("\n")

                break

    def close(self):
        self.socket.close()


class SimplerSocketUnix(object):
    """
    Wraps the socket object returned by DockerClient().api.exec_start(socket=True),
    which in turn wraps a socket.SocketIO() instance.
    """

    def __init__(self, socket, logger):
        self.socket = socket
        self.logger = logger

    def write(self, data: bytes) -> None:
        """
        Writes all of data to socket
        :param data:            data to send (bytes)
        :return:                None
        """
        while data:
            n_bytes_written = self.socket._sock.send(data)

            self.logger.debug("Written %s bytes", n_bytes_written)

            data = data[n_bytes_written:]

            if not data:
                self.socket._sock.shutdown(SHUT_WR)  # Shut down writes to the socket
                break

    def read(self) -> str:
        """
        Generator yielding lines read from socket.
        :return:                Lines read from socket (str)
        """
        read_buffer = ""

        for stream, frame in frames_iter(socket=self.socket, tty=False):

            read_buffer += frame.decode("utf8")

            if read_buffer.endswith("\n"):
                yield read_buffer.strip("\n")
                read_buffer = ""

        if read_buffer:
            yield read_buffer.strip("\n")

    def close(self):
        self.socket.close()
