import socket
from threading import Lock
from collections import defaultdict, deque
from queue import Queue
import struct
import logging

from .concurrency import synchronized
from .message import Message
from .exceptions import UnexpectedFrame, Timeout, METHOD_NAME_MAP
from .serialization import AMQPReader
from . import spec
from .spec import FrameType, Frame, Method, method_t

log = logging.getLogger('amqpy')

__all__ = ['MethodReader']

# these methods are followed by content headers and bodies
_CONTENT_METHODS = [
    spec.Basic.Return,
    spec.Basic.Deliver,
    spec.Basic.GetOk,
]


class PartialMessage:
    """Helper class to build up a multi-frame `Method` with a complete `Message`
    """

    def __init__(self, method_type, args, channel):
        """
        :param method_type: method type
        :param args: method args
        :param channel: associated channel
        :type method_type: method_t
        :type args: AMQPReader, AMQPWriter
        :type channel: int
        """
        self.method_type = method_type
        self.args = args
        self.channel = channel
        self.msg = Message()
        self.body_parts = bytearray()
        self.expected_body_size = None

    def add_header(self, payload):
        """Add header to partial message

        :param payload: frame payload of a `FrameType.HEADER` frame
        :type payload: bytes
        """
        class_id, weight, self.expected_body_size = struct.unpack('>HHQ', payload[:12])
        self.msg.load_properties(payload[12:])

    def add_payload(self, payload):
        """Add content to partial message

        :param payload: frame payload of a `FrameType.BODY` frame
        :type payload: bytes
        """
        self.body_parts.extend(payload)
        if self.complete:
            # TODO: should we bother converting this to an immutable bytes object just to make tests pass?
            self.msg.body = bytes(self.body_parts)

    @property
    def complete(self):
        """Check if message is complete

        :return: message complete?
        :rtype: bool
        """
        return self.expected_body_size == 0 or len(self.body_parts) == self.expected_body_size


class MethodReader:
    """Read frames from the server and construct complete methods

    There should be one `MethodReader` instance per connection.

    In the case of a framing error, an :exc:`AMQPConnectionError` is placed in the queue.
    In the case of unexpected frames, an :exc:`ChannelError` is placed in the queue.
    """

    def __init__(self, source):
        """
        :param source: connection source transport
        :type source: amqpy.transport.TCPTransport or amqpy.transport.SSLTransport
        """
        self.source = source
        self.sock = source.sock

        # deque[Method or Exception]
        self.method_queue = deque()
        # dict[channel_id int: PartialMessage]
        self.partial_messages = {}
        # dict[channel_id int: frame_type int]
        self.expected_types = defaultdict(lambda: FrameType.METHOD)  # next expected frame type for each channel

        self.heartbeats = 0  # total number of heartbeats received
        self.frames_recv = 0  # total number of frames received

        self._method_read_lock = Lock()

    def _next_method(self):
        """Read the next method from the source and process it

        Once one complete method has been assembled, it is placed in the internal queue. This method will block until a
        complete `Method` has been constructed, which may consist of one or more frames.
        """
        while not self.method_queue:
            # keep reading frames until we have at least one complete method in the queue
            try:
                frame = self.source.read_frame()
            except Exception as exc:
                # connection was closed? framing error?
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
            elif frame.frame_type == FrameType.HEARTBEAT:
                self._process_heartbeat()

    def _process_heartbeat(self):
        self.heartbeats += 1

    def _process_method_frame(self, frame):
        """Process method frame
        """
        channel = frame.channel
        # noinspection PyTypeChecker
        method_type = method_t(*struct.unpack('>HH', frame.payload[:4]))
        args = AMQPReader(frame.payload[4:])

        if method_type in _CONTENT_METHODS:
            # save what we've got so far and wait for the content-header
            self.partial_messages[channel] = PartialMessage(method_type, args, channel)
            self.expected_types[channel] = spec.FrameType.HEADER
        else:
            # this is a complete method
            method = Method(method_type, args, None, channel)
            self.method_queue.append(method)

    def _process_content_header(self, frame):
        """Process Content Header frames
        """
        partial = self.partial_messages[frame.channel]
        partial.add_header(frame.payload)

        if partial.complete:
            # a bodyless message, we're done
            method = Method(partial.method_type, partial.args, partial.msg, frame.channel)
            self.method_queue.append(method)
            self.partial_messages.pop(frame.channel, None)
            del self.expected_types[frame.channel]  # reset expected frame type for this channel
        else:
            # next expected frame type is FrameType.BODY
            self.expected_types[frame.channel] = spec.FrameType.BODY

    def _process_content_body(self, frame):
        """Process Content Body frames
        """
        partial = self.partial_messages[frame.channel]
        partial.add_payload(frame.payload)

        if partial.complete:
            # message is complete, append it to the queue
            method = Method(partial.method_type, partial.args, partial.msg, frame.channel)
            self.method_queue.append(method)
            self.partial_messages.pop(frame.channel, None)
            del self.expected_types[frame.channel]  # reset expected frame type for this channel

    @synchronized('_method_read_lock')
    def _read_method(self):
        """Read a method from the peer

        :return: method
        :rtype: amqpy.spec.Method
        """
        # fully read and process next method
        self._next_method()
        method = self.method_queue.popleft()

        # `method` may sometimes be an `Exception`, raise it here
        if isinstance(method, Exception):
            raise method

        log.debug('{:7} {} {}'.format('Read:', method.method_type, METHOD_NAME_MAP[method.method_type]))
        return method

    def read_method(self, timeout=None):
        """Read method

        :param timeout: timeout
        :type timeout: float
        :return: method
        :rtype: amqpy.spec.Method
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
        finally:
            if orig_timeout != timeout:
                self.sock.settimeout(orig_timeout)


class MethodWriter:
    """Write methods to the server by breaking them up and constructing multiple frames

    There should be one `MethodWriter` instance per connection, and all channels share that instance. This class is
    thread-safe. Any thread may call :meth:`write_method()` as long as no more than one thread is writing to any
    given `channel_id` at a time.
    """

    def __init__(self, dest, frame_max):
        """
        :param dest: destination transport
        :param frame_max: maximum frame payload size in bytes
        :type dest: amqpy.transport.AbstractTransport
        :type frame_max: int
        """
        self.dest = dest
        self.frame_max = frame_max
        self.methods_sent = 0  # total number of methods sent

    def write_method(self, channel_id, method):
        """Write method to connection, destined for the specified `channel_id`

        This implementation uses a queue internally to prepare all frames before writing in order to detect issues.

        This method is thread safe only if the `channel_id` parameter is unique across concurrent invocations. The AMQP
        protocol allows interleaving frames destined for different channels, but not within the same channel. This means
        no more than one thread may safely operate on any given channel.

        :param channel_id: channel
        :param method: method to write
        :type channel_id: int
        :type method: Method
        """
        log.debug('{:7} {} {}'.format('Write:', method.method_type, METHOD_NAME_MAP[method.method_type]))
        frames = Queue()

        # construct a method frame
        frame = Frame(FrameType.METHOD, channel_id, method.pack_method())
        frames.put(frame)

        if method.content:
            # construct a header frame
            frame = Frame(FrameType.HEADER, channel_id, method.pack_header())
            frames.put(frame)

            # construct one or more body frames, which contain the body of the `Message`
            chunk_size = self.frame_max - 8
            for payload in method.pack_body(chunk_size):
                frame = Frame(FrameType.BODY, channel_id, payload)
                frames.put(frame)

        while not frames.empty():
            self.dest.write_frame(frames.get())

        self.methods_sent += 1
