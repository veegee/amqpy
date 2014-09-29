from collections import namedtuple
import struct

from .serialization import AMQPReader, AMQPWriter

queue_declare_ok_t = namedtuple('queue_declare_ok_t', ('queue', 'message_count', 'consumer_count'))

basic_return_t = namedtuple('basic_return_t', ('reply_code', 'reply_text', 'exchange', 'routing_key', 'message'))

method_t = namedtuple('method_t', ('class_id', 'method_id'))

FRAME_MIN_SIZE = 4096


class FrameType:
    """Frame constants
    """
    METHOD = 1  # method frame
    HEADER = 2  # content header frame
    BODY = 3  # content body frame
    HEARTBEAT = 8  # heartbeat frame
    END = 206  # not actually a frame type; this is the frame terminator byte


class Connection:
    CLASS_ID = 10

    Start = method_t(10, 10)
    StartOk = method_t(10, 11)
    Secure = method_t(10, 20)
    SecureOk = method_t(10, 21)
    Tune = method_t(10, 30)
    TuneOk = method_t(10, 31)
    Open = method_t(10, 40)
    OpenOk = method_t(10, 41)
    Close = method_t(10, 50)
    CloseOk = method_t(10, 51)
    Blocked = method_t(10, 60)
    Unblocked = method_t(10, 61)


class Channel:
    CLASS_ID = 20

    Open = method_t(20, 10)
    OpenOk = method_t(20, 11)
    Flow = method_t(20, 20)
    FlowOk = method_t(20, 21)
    Close = method_t(20, 40)
    CloseOk = method_t(20, 41)


class Exchange:
    CLASS_ID = 40

    Declare = method_t(40, 10)
    DeclareOk = method_t(40, 11)
    Delete = method_t(40, 20)
    DeleteOk = method_t(40, 21)
    Bind = method_t(40, 30)
    BindOk = method_t(40, 31)
    Unbind = method_t(40, 40)
    UnbindOk = method_t(40, 51)


class Queue:
    CLASS_ID = 50

    Declare = method_t(50, 10)
    DeclareOk = method_t(50, 11)
    Bind = method_t(50, 20)
    BindOk = method_t(50, 21)
    Purge = method_t(50, 30)
    PurgeOk = method_t(50, 31)
    Delete = method_t(50, 40)
    DeleteOk = method_t(50, 41)
    Unbind = method_t(50, 50)
    UnbindOk = method_t(50, 51)


class Basic:
    CLASS_ID = 60

    Qos = method_t(60, 10)
    QosOk = method_t(60, 11)
    Consume = method_t(60, 20)
    ConsumeOk = method_t(60, 21)
    Cancel = method_t(60, 30)
    CancelOk = method_t(60, 31)
    Publish = method_t(60, 40)
    Return = method_t(60, 50)
    Deliver = method_t(60, 60)
    Get = method_t(60, 70)
    GetOk = method_t(60, 71)
    GetEmpty = method_t(60, 80)
    Ack = method_t(60, 80)
    Reject = method_t(60, 90)
    RecoverAsync = method_t(60, 100)
    Recover = method_t(60, 110)
    RecoverOk = method_t(60, 111)


class Confirm:
    CLASS_ID = 85

    Select = method_t(85, 10)
    SelectOk = method_t(85, 11)


class Tx:
    CLASS_ID = 90

    Select = method_t(90, 10)
    SelectOk = method_t(90, 11)
    Commit = method_t(90, 20)
    CommitOk = method_t(90, 21)
    Rollback = method_t(90, 30)
    RollbackOk = method_t(90, 31)


class Frame:
    """AMQP frame

    The format of the AMQP frame is as follows::

        offset:         0      1         3         7                 size+7      size+8
                        +------+---------+---------+-------------------+-----------+
                        | type | channel |  size   |  --- payload ---  | frame-end |
                        +------+---------+---------+-------------------+-----------+
        size (bytes)        1       2         4             size             1
    """

    def __init__(self, frame_type=None, channel=0, payload=bytes()):
        """Create new Frame

        Leave all three parameters as default to create an empty frame whose `data` can be manually written to
        afterwards.

        :param frame_type: frame type
        :param channel: associated channel number
        :param payload: frame payload
        :type frame_type: int
        :type channel: int
        :type payload: bytes or bytearray
        """
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
            self.data = struct.pack(frame_format, frame_type, channel, self._payload_size, payload, FrameType.END)

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

    The format of the method frame `FrameType.METHOD` payload is as follows::

        offset:         0          2           4
                        +----------+-----------+-------------- - -
                        | class-id | method-id | arguments...
                        +----------+-----------+-------------- - -
        size (bytes):         2          2           ...
    """

    def __init__(self, method_type, args=None, content=None, channel_id=None):
        """
        :param method_type: method type
        :param args: method args
        :param content: content
        :param channel_id: the associated channel ID, if any
        :type method_type: method_t
        :type args: AMQPReader or AMQPWriter or None
        :type content: amqp.message.GenericContent or None
        :type channel_id: int or None
        """
        self.method_type = method_type
        assert isinstance(method_type, method_t)
        if isinstance(args, (AMQPReader, AMQPWriter)):
            self.args = args
        elif args is None:
            self.args = AMQPWriter()
        else:
            raise ValueError('args must be an instance of `AMQPReader` or `AMQPWriter`')
        self.content = content
        self.channel_id = channel_id

        self._body_bytes = bytes()  # used internally to store encoded body

    def pack_method(self):
        """Pack this method into a bytes object suitable for using as a payload for `FrameType.METHOD` frames

        :return: bytes
        :rtype: bytes
        """
        return struct.pack('>HH', self.method_type.class_id, self.method_type.method_id) + self.args.getvalue()

    def pack_header(self):
        """Pack this method into a bytes object suitable for using as a payload for `FrameType.HEADER` frames

        :return: bytes
        :rtype: bytes
        """
        if not self.content:
            raise ValueError('`pack_header()` is only meaningful if there is content to pack')

        self._body_bytes = self.content.body
        if isinstance(self._body_bytes, str):
            # encode body to bytes
            coding = self.content.properties.setdefault('content_encoding', 'UTF-8')
            self._body_bytes = self.content.body.encode(coding)
        properties = self.content.serialize_properties()
        return struct.pack('>HHQ', self.method_type.class_id, 0, len(self._body_bytes)) + properties

    def pack_body(self, chunk_size):
        """Pack this method into a bytes object suitable for using as a payload for `FrameType.BODY` frames

        :param chunk_size: split up body into pices that are at most `chunk_size` bytes each
        :type chunk_size: int
        :return: bytes generator
        :rtype: generator[bytes]
        """
        if not self.content:
            raise ValueError('`pack_body()` is only meaningful if there is content to pack')

        for i in range(0, len(self._body_bytes), chunk_size):
            yield self._body_bytes[i:i + chunk_size]
