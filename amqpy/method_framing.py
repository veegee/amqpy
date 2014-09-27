import socket
from collections import defaultdict, deque
from queue import Queue
import struct

from .message import Message
from .exceptions import AMQPError, UnexpectedFrame, Timeout, METHOD_NAME_MAP
from .serialization import AMQPReader
from . import spec
from .spec import FrameType, Frame, Method, method_t


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
        :tye channel: int
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
        self.body_parts.extend(payload)  # TODO: should we bother converting this to an immutable bytes object?
        if self.complete:
            self.msg.body = bytes(self.body_parts)  # TODO: yes, to make tests pass, but reassess later

    @property
    def complete(self):
        """Check if message is complete

        :return: message complete?
        :rtype: bool
        """
        return self.expected_body_size == 0 or len(self.body_parts) == self.expected_body_size


class MethodReader:
    """Helper class to receive frames from the broker, combine them if necessary with content-headers and content-bodies
    into complete methods

    Normally a method is represented as a tuple containing (channel, method_sig, args, content).

    In the case of a framing error, an :exc:`ConnectionError` is placed in the queue.

    In the case of unexpected frames, a tuple made up of ``(channel, ChannelError)`` is placed in the queue.
    """

    def __init__(self, source):
        """
        :param source: connection source transport
        :type source: amqpy.transport.TCPTransport or amqpy.transport.SSLTransport
        """
        self.source = source
        self.sock = source.sock
        self.queue = deque()  # deque[Method or Exception]
        self.running = False
        self.partial_messages = {}
        self.heartbeats = 0
        self.expected_types = defaultdict(lambda: FrameType.METHOD)  # for each channel, which type is expected next
        self.bytes_recv = 0  # not an actual byte count, just incremented whenever we receive

    def _next_method(self):
        """Read the next method from the source and process it

        Once one complete method has been assembled, it is placed in the internal queue.
        """
        while not self.queue:
            try:
                frame = self.source.read_frame()
            except Exception as exc:
                # connection was closed? framing error?
                self.queue.append(exc)
                break

            self.bytes_recv += 1

            if frame.frame_type not in (self.expected_types[frame.channel], 8):
                msg = 'Received frame type {} while expecting type: {}' \
                    .format(frame.frame_type, self.expected_types[frame.channel])
                self.queue.append((frame.channel, UnexpectedFrame(msg)))  # TODO: review type
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
        payload = frame.payload
        channel = frame.channel
        method_type = method_t(*struct.unpack('>HH', payload[:4]))
        args = AMQPReader(payload[4:])

        if method_type in _CONTENT_METHODS:
            # save what we've got so far and wait for the content-header
            self.partial_messages[channel] = PartialMessage(method_type, args, channel)
            self.expected_types[channel] = spec.FrameType.HEADER
        else:
            # this is a complete method
            method = Method(method_type, args, None, channel)
            self.queue.append(method)

    def _process_content_header(self, frame):
        """Process Content Header frames
        """
        partial = self.partial_messages[frame.channel]
        partial.add_header(frame.payload)

        if partial.complete:
            # a bodyless message, we're done
            method = Method(partial.method_type, partial.args, partial.msg, frame.channel)
            self.queue.append(method)
            self.partial_messages.pop(frame.channel, None)
            self.expected_types[frame.channel] = FrameType.METHOD  # set expected frame type back to default
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
            self.queue.append(method)
            self.partial_messages.pop(frame.channel, None)
            self.expected_types[frame.channel] = FrameType.METHOD  # set expected frame type back to default

    def _read_method(self):
        """Read a method from the peer

        :return: method
        :rtype: amqpy.spec.Method
        """
        self._next_method()
        m = self.queue.popleft()
        if isinstance(m, Exception):
            raise m
        if isinstance(m, tuple) and isinstance(m[1], AMQPError):
            raise m[1]
        print(' read:', m.method_type, METHOD_NAME_MAP[m.method_type])
        return m

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
    """Convert AMQP methods into AMQP frames and send them out to the peer
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
        self.bytes_sent = 0

    def write_method(self, channel, method):
        """Write method

        This implementation uses a queue internally to prepare all frames before writing in order to detect issues.

        :param channel: channel
        :param method: method to write
        :type channel: int
        :type method: amqpy.spec.Method
        """
        print('write:', method.method_type, METHOD_NAME_MAP[method.method_type])
        frames = Queue()

        # prepare method frame
        frame = Frame(FrameType.METHOD, channel, method.pack_method())
        frames.put(frame)

        if method.content:
            # prepare header frame
            frame = Frame(FrameType.HEADER, channel, method.pack_header())
            frames.put(frame)

            # prepare body frame(s)
            chunk_size = self.frame_max - 8
            for payload in method.pack_body(chunk_size):
                frame = Frame(FrameType.BODY, channel, payload)
                frames.put(frame)

        while not frames.empty():
            self.dest.write_frame(frames.get())

        self.bytes_sent += 1
