"""High-level representations of AMQP protocol objects
"""
from __future__ import absolute_import, division, print_function

__metaclass__ = type
import six
import struct
import logging

from .serialization import AMQPReader, AMQPWriter
from .spec import FrameType, method_t
from .message import Message

log = logging.getLogger('amqpy')


class Frame:
    """AMQP frame

    A `Frame` represents the lowest-level packet of data specified by the AMQP 0.9.1
    wire-level protocol. All methods and messages are packed into one or more frames before being
    sent to the peer.

    The format of the AMQP frame is as follows::

        offset:         0      1         3         7                 size+7      size+8
                        +------+---------+---------+-------------------+-----------+
                        | type | channel |  size   |  --- payload ---  | frame-end |
                        +------+---------+---------+-------------------+-----------+
        size (bytes)        1       2         4             size             1
    """

    __slots__ = ['data', '_frame_type', '_channel', '_payload_size']

    def __init__(self, frame_type=None, channel=0, payload=bytes()):
        """Create new Frame

        Leave all three parameters as default to create an empty frame whose `data` can be manually
        written to afterwards.

        :param frame_type: frame type
        :param channel: associated channel number
        :param payload: frame payload
        :type frame_type: int
        :type channel: int
        :type payload: bytes or bytearray
        """
        #: raw frame data; can be manually manipulated at any time
        #:
        #: :type: bytearray
        self.data = bytearray()

        self._frame_type = None
        self._channel = None
        self._payload_size = None

        # create bytearray from provided data
        if frame_type is not None:
            self._frame_type = frame_type
            self._channel = channel
            self._payload_size = len(payload)
            frame_format = '>BHI{}sB'.format(self._payload_size)
            self.data = struct.pack(frame_format, frame_type, channel, self._payload_size, payload,
                                    FrameType.END)

    @property
    def frame_type(self):
        """Get frame type

        :return: frame type
        :rtype: int
        """
        if self._frame_type is not None:
            return self._frame_type
        else:
            self._frame_type = struct.unpack_from('>B', self.data)[0]
            return self._frame_type

    @property
    def channel(self):
        """Get frame channel number

        :return: channel number
        :rtype: int
        """
        if self._channel is not None:
            return self._channel
        else:
            self._channel = struct.unpack_from('>H', self.data, 1)[0]
            return self._channel

    @property
    def payload_size(self):
        """Get frame payload size

        :return: payload size
        :rtype: int
        """
        if self._payload_size is not None:
            return self._payload_size
        else:
            self._payload_size = struct.unpack_from('>I', self.data, 3)[0]
            return self._payload_size

    @property
    def payload(self):
        """Get frame payload

        :return: payload
        :rtype: bytearray
        """
        return self.data[7:-1]


class Method:
    """AMQP method

    The AMQP 0.9.1 protocol specifies communication as sending and receiving "methods". Methods
    consist of a "class-id" and "method-id" and are represented by a `method_t` namedtuple in amqpy.
    Methods are packed into the payload of a `FrameType.METHOD` frame, and most methods can be fully
    sent in a single frame. If the method specified to be carrying content (such as a message), the
    method frame is followed by additional frames: a `FrameType.HEADER` frame, then zero or more
    `FrameType.BODY` frames.

    The format of the `FrameType.METHOD` frame's payload is as follows::

        offset:         0          2           4
                        +----------+-----------+-------------- - -
                        | class-id | method-id | arguments...
                        +----------+-----------+-------------- - -
        size (bytes):         2          2         variable

    The format of the `FrameType.HEADER` frame's payload is as follows::

        offset:         0          2        4           12               14
                        +----------+--------+-----------+----------------+------------------- - -
                        | class-id | weight | body size | property flags | property list...
                        +----------+--------+-----------+----------------+------------------- - -
        size (bytes):         2         2         8              2           variable

    The format of the `FrameType.BODY` frame's payload is simply raw binary data of the message
    body.
    """
    __slots__ = ['method_type', 'args', 'content', 'channel_id', '_body_bytes', '_expected_body_size']

    def __init__(self, method_type=None, args=None, content=None, channel_id=None):
        """
        :param method_type: method type
        :param args: method args
        :param content: content
        :param channel_id: the associated channel ID, if any
        :type method_type: method_t
        :type args: AMQPReader or AMQPWriter or None
        :type content: Message or None
        :type channel_id: int or None
        """
        #: :type: amqpy.spec.method_t
        self.method_type = method_type

        if isinstance(args, (AMQPReader, AMQPWriter)):
            self.args = args
        elif args is None:
            self.args = AMQPWriter()
        else:
            raise ValueError('args must be an instance of `AMQPReader` or `AMQPWriter`')

        #: :type: Message or None
        self.content = content  # Message if this method is carrying content
        #: :type: int
        self.channel_id = channel_id

        self._body_bytes = bytearray()  # used internally to store encoded GenericContent body
        self._expected_body_size = None  # set automatically when `load_header_frame()` is called

    def load_method_frame(self, frame):
        """Load method frame payload data

        This method is intended to be called when constructing a `Method` from incoming data.

        After calling, `self.method_type`, `self.args`, and `self.channel_id` will be loaded with
        data from the frame.

        :param frame: `FrameType.METHOD` frame
        :type frame: amqpy.proto.Frame
        """
        # noinspection PyTypeChecker
        self.method_type = method_t(*struct.unpack('>HH', frame.payload[:4]))
        self.args = AMQPReader(frame.payload[4:])
        self.channel_id = frame.channel

    def load_header_frame(self, frame):
        """Add header to partial method

        This method is intended to be called when constructing a `Method` from incoming data.

        :param frame: `FrameType.HEADER` frame
        :type frame: amqpy.proto.Frame
        """
        if not self.content:
            self.content = Message()

        # noinspection PyTypeChecker
        class_id, weight, self._expected_body_size = struct.unpack('>HHQ', frame.payload[:12])
        self.content.load_properties(frame.payload[12:])

    def load_body_frame(self, frame):
        """Add content to partial method

        This method is intended to be called when constructing a `Method` from incoming data.

        :param frame: `FrameType.BODY` frame
        :type frame: amqpy.proto.Frame
        """
        self._body_bytes.extend(frame.payload)
        if self.complete:
            self.content.body = bytes(self._body_bytes)

    @property
    def complete(self):
        """Check if the message that is carried by this method has been completely assembled,
        i.e. the expected number of bytes have been loaded

        This method is intended to be called when constructing a `Method` from incoming data.

        :return: True if method is complete, else False
        :rtype: bool
        """
        return self._expected_body_size == 0 or len(self._body_bytes) == self._expected_body_size

    def _pack_method(self):
        """Pack this method into a bytes object suitable for using as a payload for
        `FrameType.METHOD` frames

        This method is intended to be called when packing an already-completed `Method` into
        outgoing frames.

        :return: bytes
        :rtype: bytes
        """
        return struct.pack('>HH', self.method_type.class_id,
                           self.method_type.method_id) + self.args.getvalue()

    def _pack_header(self):
        """Pack this method into a bytes object suitable for using as a payload for
        `FrameType.HEADER` frames

        This method is intended to be called when packing an already-completed `Method` into
        outgoing frames.

        :return: bytes
        :rtype: bytes
        """
        if not self.content:
            raise ValueError('`_pack_header()` is only meaningful if there is content to pack')

        self._body_bytes = self.content.body
        if isinstance(self._body_bytes, six.string_types):
            # encode body to bytes
            coding = self.content.properties.setdefault('content_encoding', 'UTF-8')
            try:
                self._body_bytes = self.content.body.encode(coding)
            except LookupError:
                self._body_bytes = self.content.body

        properties = self.content.serialize_properties()
        return struct.pack('>HHQ', self.method_type.class_id, 0, len(self._body_bytes)) + properties

    def _pack_body(self, chunk_size):
        """Pack this method into a bytes object suitable for using as a payload for
        `FrameType.BODY` frames

        This method is intended to be called when packing an already-completed `Method` into
        outgoing frames.

        :param chunk_size: split up body into pieces that are at most `chunk_size` bytes each
        :type chunk_size: int
        :return: bytes generator
        :rtype: generator[bytes]
        """
        if not self.content:
            raise ValueError('`_pack_body()` is only meaningful if there is content to pack')

        for i in range(0, len(self._body_bytes), chunk_size):
            yield self._body_bytes[i:i + chunk_size]

    def dump_method_frame(self):
        """Create a method frame

        This method is intended to be called when sending frames for an already-completed `Method`.

        :return: `FrameType.METHOD` frame
        :rtype: amqpy.proto.Frame
        """
        frame = Frame(FrameType.METHOD, self.channel_id, self._pack_method())
        return frame

    def dump_header_frame(self):
        """Create a header frame

        This method is intended to be called when sending frames for an already-completed `Method`.

        :return: `FrameType.HEADER` frame
        :rtype: amqpy.proto.Frame
        """
        frame = Frame(FrameType.HEADER, self.channel_id, self._pack_header())
        return frame

    def dump_body_frame(self, chunk_size):
        """Create a body frame

        This method is intended to be called when sending frames for an already-completed `Method`.

        :param chunk_size: body chunk size in bytes; this is typically the maximum frame size - 8
        :type chunk_size: int
        :return: generator of `FrameType.BODY` frames
        :rtype: generator[amqpy.proto.Frame]
        """
        for payload in self._pack_body(chunk_size):
            frame = Frame(FrameType.BODY, self.channel_id, payload)
            yield frame
