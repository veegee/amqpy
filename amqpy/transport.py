import errno
import socket
import ssl
from abc import ABCMeta, abstractmethod
from socket import SOL_TCP
from ssl import SSLError
from struct import pack, unpack

from .exceptions import UnexpectedFrame
from .utils import get_errno


_UNAVAIL = errno.EAGAIN, errno.EINTR, errno.ENOENT


# Yes, Advanced Message Queuing Protocol Protocol is redundant
AMQP_PROTOCOL_HEADER = 'AMQP\x01\x01\x00\x09'.encode('latin_1')


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

            self._write(AMQP_PROTOCOL_HEADER)
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
    def _read(self, n, initial=False):
        """Read exactly `n` bytes from the peer

        :param n: number of bytes to read
        :type n: int
        :return: data read
        :rtype: bytes
        """
        pass

    @abstractmethod
    def _write(self, s):
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

    def read_frame(self, unpack=unpack):
        read_frame_buffer = bytes()
        try:
            frame_header = self._read(7, True)
            read_frame_buffer += frame_header
            frame_type, channel, size = unpack('>BHI', frame_header)
            payload = self._read(size)
            read_frame_buffer += payload
            ch = ord(self._read(1))
        except socket.timeout:
            self._read_buffer = read_frame_buffer + self._read_buffer
            raise
        except (OSError, IOError, socket.error) as exc:
            # don't disconnect for ssl read time outs http://bugs.python.org/issue10272
            if isinstance(exc, SSLError) and 'timed out' in str(exc):
                raise socket.timeout()
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise
        if ch == 206:  # '\xce'
            return frame_type, channel, payload
        else:
            raise UnexpectedFrame('Received 0x{0:02x} while expecting 0xce'.format(ch))

    def write_frame(self, frame_type, channel, payload):
        size = len(payload)
        try:
            self._write(pack('>BHI%dsB' % size, frame_type, channel, size, payload, 0xce))
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
        self._read_buffer = bytes()
        super().__init__(host, port, connect_timeout)

    def _setup_transport(self):
        """Wrap the socket in an SSL object
        """
        self.sock = ssl.wrap_socket(self.sock, **self.ssl_opts)
        self._quick_recv = self.sock.read

    def _read(self, n, initial=False, _errnos=(errno.ENOENT, errno.EAGAIN, errno.EINTR)):
        """Read from socket

        According to SSL_read(3), it can at most return 16kb of data. Thus, we use an internal read buffer like
        TCPTransport._read to get the exact number of bytes wanted.

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

    def _write(self, s):
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
        """Setup to _write() directly to the socket, and do our own buffered reads
        """
        self._read_buffer = bytes()
        self._quick_recv = self.sock.recv

    def _read(self, n, initial=False, _errnos=(errno.EAGAIN, errno.EINTR)):
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

    def _write(self, s):
        self.sock.sendall(s)


def create_transport(host, port, connect_timeout, ssl):
    """Given a few parameters from the Connection constructor, select and create a subclass of AbstractTransport

    If `ssl` is a dict, SSL will be used and `ssl` will be passed to :func:`ssl.wrap_socket()`. In all other cases,
    SSL will not be used.

    :param host: host
    :param connect_timeout: connect timeout
    :param ssl: ssl options passed to :func:`ssl.wrap_socket()`
    :type host: str
    :type connect_timeout: float or None
    :type ssl: dict or None
    """
    if isinstance(ssl, dict):
        return SSLTransport(host, port, connect_timeout, ssl)
    else:
        return TCPTransport(host, port, connect_timeout)
