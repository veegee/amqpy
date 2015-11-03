from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
from threading import Lock
import sys
from collections import defaultdict, deque
import six
import logging
import socket
import errno
from .utils import get_errno

if six.PY2:
    from Queue import Queue
else:
    from queue import Queue

from .concurrency import synchronized
from .exceptions import UnexpectedFrame, Timeout, METHOD_NAME_MAP
from .proto import Method
from . import spec
from .spec import FrameType

log = logging.getLogger('amqpy')

__all__ = ['MethodReader']

# these received methods are followed by content headers and bodies
_CONTENT_METHODS = [
    spec.Basic.Return,
    spec.Basic.Deliver,
    spec.Basic.GetOk,
]


class MethodReader:
    """Read frames from the server and construct complete methods

    There should be one `MethodReader` instance per connection.

    In the case of a framing error, an :exc:`AMQPConnectionError` is placed in the queue.
    In the case of unexpected frames, an :exc:`ChannelError` is placed in the queue.
    """

    def __init__(self, transport):
        """
        :param transport: transport to read from
        :type transport: amqpy.transport.Transport
        """
        self.transport = transport
        self.sock = transport.sock

        # deque[Method or Exception]
        self.method_queue = deque()

        # dict[channel_id int: PartialMessage]
        self.partial_methods = {}

        # next expected frame type for each channel
        # dict[channel_id int: frame_type int]
        self.expected_types = defaultdict(lambda: FrameType.METHOD)

        self.frames_recv = 0  # total number of frames received

        self._method_read_lock = Lock()

    def _next_method(self):
        """Read the next method from the source and process it

        Once one complete method has been assembled, it is placed in the internal queue. This
        method will block until a complete `Method` has been constructed, which may consist of one
        or more frames.
        """
        while not self.method_queue:
            # keep reading frames until we have at least one complete method in the queue
            try:
                frame = self.transport.read_frame()
            except Exception as exc:
                # connection was closed? framing error?
                if six.PY2:
                    _, _, tb = sys.exc_info()
                    exc.tb = tb
                self.method_queue.append(exc)
                break

            self.frames_recv += 1

            if frame.frame_type not in (self.expected_types[frame.channel], 8):
                msg = 'Received frame type {} while expecting type: {}' \
                    .format(frame.frame_type, self.expected_types[frame.channel])
                self.method_queue.append(UnexpectedFrame(msg, channel_id=frame.channel))
            elif frame.frame_type == FrameType.METHOD:
                self._process_method_frame(frame)
            elif frame.frame_type == FrameType.HEADER:
                self._process_content_header(frame)
            elif frame.frame_type == FrameType.BODY:
                self._process_content_body(frame)

    def _process_method_frame(self, frame):
        """Process method frame

        :param frame: incoming frame
        :type frame: amqpy.proto.Frame
        """
        channel_id = frame.channel
        method = Method()
        method.load_method_frame(frame)

        if method.method_type in _CONTENT_METHODS:
            # save what we've got so far and wait for the content header frame
            self.partial_methods[channel_id] = method
            self.expected_types[channel_id] = spec.FrameType.HEADER
        else:
            # this is a complete method
            self.method_queue.append(method)

    def _process_content_header(self, frame):
        """Process Content Header frames

        :param frame: incoming frame
        :type frame: amqpy.proto.Frame
        """
        #: :type: amqpy.proto.Method
        method = self.partial_methods[frame.channel]
        method.load_header_frame(frame)

        if method.complete:
            # a bodyless message, we're done
            self.method_queue.append(method)
            self.partial_methods.pop(frame.channel, None)
            del self.expected_types[frame.channel]  # reset expected frame type for this channel
        else:
            # next expected frame type is FrameType.BODY
            self.expected_types[frame.channel] = spec.FrameType.BODY

    def _process_content_body(self, frame):
        """Process Content Body frames

        :param frame: incoming frame
        :type frame: amqpy.proto.Frame
        """
        method = self.partial_methods[frame.channel]
        method.load_body_frame(frame)

        if method.complete:
            # message is complete, append it to the queue
            self.method_queue.append(method)
            self.partial_methods.pop(frame.channel, None)
            del self.expected_types[frame.channel]  # reset expected frame type for this channel

    @synchronized('_method_read_lock')
    def _read_method(self):
        """Read a method from the peer

        :return: method
        :rtype: amqpy.proto.Method
        """
        # fully read and process next method
        self._next_method()
        method = self.method_queue.popleft()

        # `method` may sometimes be an `Exception`, raise it here
        if isinstance(method, Exception):
            raise method

        log.debug('{:7} channel: {} {} {}'
                  .format('Read:', method.channel_id,
                          method.method_type, METHOD_NAME_MAP[method.method_type]))
        return method

    def read_method(self, timeout=None):
        """Read method

        :param timeout: timeout
        :type timeout: float
        :return: method
        :rtype: amqpy.proto.Method
        :raise amqpy.exceptions.Timeout: if the operation times out
        """
        if timeout is None:
            return self._read_method()

        orig_timeout = self.sock.gettimeout()
        if orig_timeout != timeout:
            self.sock.settimeout(timeout)

        try:
            return self._read_method()
        except socket.timeout:
            raise Timeout()
        except socket.error as e:
            if get_errno(e) == errno.EAGAIN:
                raise Timeout()
            raise
        finally:
            if orig_timeout != timeout:
                self.sock.settimeout(orig_timeout)


class MethodWriter:
    """Write methods to the server by breaking them up and constructing multiple frames

    There should be one `MethodWriter` instance per connection, and all channels share that
    instance. This class is thread-safe. Any thread may call :meth:`write_method()` as long as no
    more than one thread is writing to any given `channel_id` at a time.
    """

    def __init__(self, transport, frame_max):
        """
        :param transport: transport to write to
        :param frame_max: maximum frame payload size in bytes
        :type transport: amqpy.transport.Transport
        :type frame_max: int
        """
        self.transport = transport
        self.frame_max = frame_max
        self.methods_sent = 0  # total number of methods sent

    def write_method(self, method):
        """Write method to connection, destined for the channel as set in `method.channel_id`

        This implementation uses a queue internally to prepare all frames before writing in order
        to detect issues.

        This method is thread safe only if the `channel_id` parameter is unique across concurrent
        invocations. The AMQP protocol allows interleaving frames destined for different channels,
        but not within the same channel. This means no more than one thread may safely operate on
        any given channel.

        :param method: method to write
        :type method: amqpy.proto.Method
        """
        transport = self.transport
        log.debug('{:7} channel: {} {} {}'
                  .format('Write:', method.channel_id,
                          method.method_type, METHOD_NAME_MAP[method.method_type]))
        frames = Queue()

        # construct a method frame
        frames.put(method.dump_method_frame())

        if method.content:
            # construct a header frame
            frames.put(method.dump_header_frame())

            # construct one or more body frames, which contain the body of the `Message`
            chunk_size = self.frame_max - 8
            for frame in method.dump_body_frame(chunk_size):
                frames.put(frame)

        while not frames.empty():
            transport.write_frame(frames.get())

        self.methods_sent += 1
