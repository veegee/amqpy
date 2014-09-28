import errno
import socket
import ssl
from abc import ABCMeta, abstractmethod
from socket import SOL_TCP

from .exceptions import UnexpectedFrame
from .utils import get_errno
from .spec import FrameType, Frame


_UNAVAIL = errno.EAGAIN, errno.EINTR, errno.ENOENT

AMQP_PROTOCOL_HEADER = b'AMQP\x00\x00\x09\x01'  # bytes([65, 77, 81, 80, 0, 0, 9, 1])


class AbstractTransport(metaclass=ABCMeta):
    """Common superclass for TCP and SSL transports"""
    connected = False

    def __init__(self, host, port, connect_timeout):
        """
        :param host: hostname or IP address
        :param port: port
        :param connect_timeout: connect timeout
        :type host: str
        :type port: int
        :type connect_timeout: float or None
        """
        self.connected = True
        self._read_buffer = bytes()

        self.sock = None
        last_err = None
        for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, SOL_TCP):
            af, socktype, proto, canonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
                self.sock.settimeout(connect_timeout)
                self.sock.connect(sa)
            except socket.error as exc:
                self.sock.close()
                self.sock = None
                last_err = exc
                continue
            break

        if not self.sock:
            # didn't connect, return the most recent error message
            raise socket.error(last_err)

        try:
            self.sock.settimeout(None)
            self.sock.setsockopt(SOL_TCP, socket.TCP_NODELAY, 1)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            self._setup_transport()

            self.write(AMQP_PROTOCOL_HEADER)
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise

    def __del__(self):
        try:
            # socket module may have been collected by gc if this is called by a thread at shutdown.
            if socket is not None:
                try:
                    self.close()
                except socket.error:
                    pass
        finally:
            self.sock = None

    @abstractmethod
    def read(self, n, initial=False):
        """Read exactly `n` bytes from the peer

        :param n: number of bytes to read
        :type n: int
        :return: data read
        :rtype: bytes
        """
        pass

    @abstractmethod
    def write(self, s):
        """Completely write a string to the peer
        """

    @abstractmethod
    def _setup_transport(self):
        """Do any additional initialization of the class (used by the subclasses)
        """
        pass

    def close(self):
        if self.sock is not None:
            # call shutdown first to make sure that pending messages reach the AMQP broker if the program exits after
            # calling this method
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None
        self.connected = False

    def read_frame(self):
        """Read frame from connection

        :return: frame
        :rtype: amqpy.spec.Frame
        """
        frame = Frame()
        try:
            # read frame header: 7 bytes
            frame_header = self.read(7, True)
            frame.data.extend(frame_header)

            # read frame payload
            payload = self.read(frame.payload_size)
            frame.data.extend(payload)

            # read frame terminator byte
            frame_terminator = self.read(1)
            frame.data.extend(frame_terminator)
        except socket.timeout:
            self._read_buffer = frame.data + self._read_buffer
            raise
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise
        if frame_terminator[0] == FrameType.END:
            return frame
        else:
            raise UnexpectedFrame('Received 0x{0:02x} while expecting 0xCE (FrameType.END)'.format(frame_terminator))

    def write_frame(self, frame):
        """
        :param frame: frame
        :type frame: amqpy.spec.Frame
        """
        try:
            self.write(frame.data)
        except socket.timeout:
            raise
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise


class SSLTransport(AbstractTransport):
    """Transport that works over SSL
    """

    def __init__(self, host, port, connect_timeout, ssl_opts):
        self.ssl_opts = ssl_opts
        super().__init__(host, port, connect_timeout)

    def _setup_transport(self):
        """Wrap the socket in an SSL object
        """
        self.sock = ssl.wrap_socket(self.sock, **self.ssl_opts)
        self._quick_recv = self.sock.read

    def read(self, n, initial=False, _errnos=(errno.ENOENT, errno.EAGAIN, errno.EINTR)):
        """Read from socket

        According to SSL_read(3), it can at most return 16kb of data. Thus, we use an internal read buffer like
        TCPTransport.read to get the exact number of bytes wanted.

        :param n: number of bytes to read
        :type n: int
        :return: data read
        :rtype: bytes
        """
        recv = self._quick_recv
        rbuf = self._read_buffer
        try:
            while len(rbuf) < n:
                try:
                    s = recv(n - len(rbuf))  # see note above
                except socket.error as exc:
                    # ssl.sock.read may cause ENOENT if the operation couldn't be performed (Issue celery#1414).
                    if not initial and exc.errno in _errnos:
                        continue
                    raise
                if not s:
                    raise IOError('Socket closed')
                rbuf += s
        except:
            self._read_buffer = rbuf
            raise
        result, self._read_buffer = rbuf[:n], rbuf[n:]
        return result

    def write(self, s):
        """Write a string out to the SSL socket fully
        """
        try:
            write = self.sock.write
        except AttributeError:
            # works around a bug in python socket library
            raise IOError('Socket closed')
        else:
            while s:
                n = write(s)
                if not n:
                    raise IOError('Socket closed')
                s = s[n:]


class TCPTransport(AbstractTransport):
    """Transport that deals directly with TCP socket
    """

    def _setup_transport(self):
        """Setup to write() directly to the socket, and do our own buffered reads
        """
        self._quick_recv = self.sock.recv

    def read(self, n, initial=False, _errnos=(errno.EAGAIN, errno.EINTR)):
        """Read exactly n bytes from the socket
        """
        recv = self._quick_recv
        rbuf = self._read_buffer
        try:
            while len(rbuf) < n:
                try:
                    s = recv(n - len(rbuf))
                except socket.error as exc:
                    if not initial and exc.errno in _errnos:
                        continue
                    raise
                if not s:
                    raise IOError('Socket closed')
                rbuf += s
        except:
            self._read_buffer = rbuf
            raise

        result, self._read_buffer = rbuf[:n], rbuf[n:]
        return result

    def write(self, s):
        self.sock.sendall(s)


def create_transport(host, port, connect_timeout, ssl_opts=None):
    """Given a few parameters from the Connection constructor, select and create a subclass of AbstractTransport

    If `ssl_opts` is a dict, SSL will be used and `ssl_opts` will be passed to :func:`ssl.wrap_socket()`. In all other
    cases, SSL will not be used.

    :param host: host
    :param connect_timeout: connect timeout
    :param ssl_opts: ssl options passed to :func:`ssl.wrap_socket()`
    :type host: str
    :type connect_timeout: float or None
    :type ssl_opts: dict or None
    """
    if isinstance(ssl_opts, dict):
        return SSLTransport(host, port, connect_timeout, ssl_opts)
    else:
        return TCPTransport(host, port, connect_timeout)
