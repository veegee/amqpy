from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
import errno
import six
import socket
import ssl
from abc import ABCMeta, abstractmethod
import logging
from threading import RLock
from ssl import SSLError
import datetime
import time

from . import compat
from .proto import Frame
from .concurrency import synchronized
from .exceptions import UnexpectedFrame
from .utils import get_errno
from .spec import FrameType

log = logging.getLogger('amqpy')
compat.patch()

_UNAVAIL = {errno.EAGAIN, errno.EINTR, errno.ENOENT}

AMQP_PROTOCOL_HEADER = b'AMQP\x00\x00\x09\x01'  # bytes([65, 77, 81, 80, 0, 0, 9, 1])


class Transport:
    __metaclass__ = ABCMeta
    """Common superclass for TCP and SSL transports"""
    connected = False

    def __init__(self, host, port, connect_timeout, buf_size):
        """
        :param host: hostname or IP address
        :param port: port
        :param connect_timeout: connect timeout
        :type host: str
        :type port: int
        :type connect_timeout: float or None
        """
        self._rbuf = bytearray(buf_size)

        #: :type: datetime.datetime
        self.last_heartbeat_sent = None
        #: :type: datetime.datetime
        self.last_heartbeat_received = None

        self.last_heartbeat_sent_monotonic = 0.0

        # the purpose of the frame lock is to allow no more than one thread to read/write a frame
        # to the connection at any time
        self._frame_write_lock = RLock()
        self._frame_read_lock = RLock()

        self.sock = None

        # try to connect
        last_err = None
        for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP):
            af, socktype, proto, canonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
                self.sock.settimeout(connect_timeout)
                self.sock.connect(sa)
                break
            except socket.error as exc:
                self.sock.close()
                self.sock = None
                last_err = exc

        if not self.sock:
            # didn't connect, return the most recent error message
            raise socket.error(last_err)

        try:
            assert isinstance(self.sock, socket.socket)
            self.sock.settimeout(None)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            self._setup_transport()

            self.write(AMQP_PROTOCOL_HEADER)
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise

        self.connected = True

    def __del__(self):
        try:
            # socket module may have been collected by gc if this is called by a thread at shutdown
            if socket is not None:
                # noinspection PyBroadException
                try:
                    self.close()
                except:
                    pass
        finally:
            self.sock = None

    def _read(self, n, initial, _errnos):
        """Read from socket

        This is the default implementation. Subclasses may implement `read()` to simply call this
        method, or provide their own `read()` implementation.

        Note: According to SSL_read(3), it can at most return 16kB of data. Thus, we use an internal
        read buffer like to get the exact number of bytes wanted.

        Note: ssl.sock.read may cause ENOENT if the operation couldn't be performed (?).

        :param int n: exact number of bytes to read
        :return: data read
        :rtype: memoryview
        """
        mview = memoryview(self._rbuf)
        to_read = n
        while to_read:
            try:
                bytes_read = self.sock.recv_into(mview, to_read)
                mview = mview[bytes_read:]
                to_read -= bytes_read
            except socket.error as exc:
                if not initial and exc.errno in _errnos:
                    continue
                raise

            if not bytes_read:
                raise IOError('socket closed')

        return memoryview(self._rbuf)[:n]

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

    def _setup_transport(self):
        """Do any additional initialization of the class (used by the subclasses)
        """
        pass

    def close(self):
        if self.sock is not None:
            # call shutdown first to make sure that pending messages reach the AMQP broker if the
            # program exits after calling this method
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None
        self.connected = False

    @synchronized('_frame_read_lock')
    def read_frame(self):
        """Read frame from connection

        Note that the frame may be destined for any channel. It is permitted to interleave frames
        from different channels.

        :return: frame
        :rtype: amqpy.proto.Frame
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

            if six.PY2:
                #: :type: int
                i_last_byte = six.byte2int(frame_terminator)
            else:
                # this fixes the change in memoryview in Python 3.3 (accessing an element returns the
                #  correct type)
                #: :type: int
                i_last_byte = six.byte2int(bytes(frame_terminator))
        except (OSError, IOError, socket.error) as exc:
            # don't disconnect for ssl read time outs (Python 3.2):
            # http://bugs.python.org/issue10272
            if isinstance(exc, SSLError) and 'timed out' in str(exc):
                raise socket.timeout()
            if get_errno(exc) not in _UNAVAIL and not isinstance(exc, socket.timeout):
                self.connected = False
            raise

        if i_last_byte == FrameType.END:
            if frame.frame_type == FrameType.HEARTBEAT:
                self.last_heartbeat_received = datetime.datetime.now()
            return frame
        else:
            raise UnexpectedFrame('Received {} while expecting 0xce (FrameType.END)'.format(hex(i_last_byte)))


    @synchronized('_frame_write_lock')
    def write_frame(self, frame):
        """Write frame to connection

        Note that the frame may be destined for any channel. It is permitted to interleave frames
        from different channels.

        :param frame: frame
        :type frame: amqpy.proto.Frame
        """
        try:
            self.write(frame.data)
        except socket.timeout:
            raise
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise

    def send_heartbeat(self):
        """Send a heartbeat to the server
        """
        self.last_heartbeat_sent = datetime.datetime.now()
        self.last_heartbeat_sent_monotonic = time.monotonic()
        self.write_frame(Frame(FrameType.HEARTBEAT))

    def is_alive(self):
        """Check if connection is alive

        This method is the primary way to check if the connection is alive.

        Side effects: This method may send a heartbeat as a last resort to check if the connection
        is alive.

        :return: True if connection is alive, else False
        :rtype: bool
        """
        if not self.sock:
            # we don't have a valid socket, this connection is definitely not alive
            return False

        if not self.connected:
            # the `transport` is not connected
            return False

        # recv with MSG_PEEK to check if the connection is alive
        # note: if there is data still in the buffer, this will not tell us anything
        # if hasattr(socket, 'MSG_PEEK') and not isinstance(self.sock, ssl.SSLSocket):
        #     prev = self.sock.gettimeout()
        #     self.sock.settimeout(0.0001)
        #     try:
        #         self.sock.recv(1, socket.MSG_PEEK)
        #     except socket.timeout:
        #         pass
        #     except socket.error:
        #         # the exception is usually (always?) a ConnectionResetError in Python 3.3+
        #         log.debug('socket.error, connection is closed')
        #         return False
        #     finally:
        #         self.sock.settimeout(prev)

        # send a heartbeat to check if the connection is alive
        try:
            self.send_heartbeat()
        except socket.error:
            return False

        return True


class SSLTransport(Transport):
    """Transport that works over SSL
    """

    def __init__(self, host, port, connect_timeout, frame_max, ssl_opts):
        self.ssl_opts = ssl_opts
        super(SSLTransport, self).__init__(host, port, connect_timeout, frame_max)

    def _setup_transport(self):
        """Wrap the socket in an SSL object
        """
        self.sock = ssl.wrap_socket(self.sock, **self.ssl_opts)

    def read(self, n, initial=False):
        """Read from socket

        According to SSL_read(3), it can at most return 16kb of data. Thus, we use an internal read
        buffer like `TCPTransport.read()` to get the exact number of bytes wanted.

        :param int n: exact number of bytes to read
        :return: data read
        :rtype: bytes
        """
        return self._read(n, initial, _errnos=(errno.ENOENT, errno.EAGAIN, errno.EINTR))

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


class TCPTransport(Transport):
    """Transport that deals directly with TCP socket
    """

    def read(self, n, initial=False):
        """Read exactly n bytes from the socket

        :param int n: exact number of bytes to read
        :return: data read
        :rtype: bytes
        """
        return self._read(n, initial, _errnos=(errno.EAGAIN, errno.EINTR))

    def write(self, s):
        self.sock.sendall(s)


def create_transport(host, port, connect_timeout, frame_max, ssl_opts=None):
    """Given a few parameters from the Connection constructor, select and create a subclass of
    Transport

    If `ssl_opts` is a dict, SSL will be used and `ssl_opts` will be passed to
    :func:`ssl.wrap_socket()`. In all other cases, SSL will not be used.

    :param host: host
    :param connect_timeout: connect timeout
    :param ssl_opts: ssl options passed to :func:`ssl.wrap_socket()`
    :type host: str
    :type connect_timeout: float or None
    :type ssl_opts: dict or None
    """
    if isinstance(ssl_opts, dict):
        return SSLTransport(host, port, connect_timeout, frame_max, ssl_opts)
    else:
        return TCPTransport(host, port, connect_timeout, frame_max)
