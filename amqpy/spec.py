from collections import namedtuple
import struct
from .serialization import AMQPReader, AMQPWriter


queue_declare_ok_t = namedtuple('queue_declare_ok_t', ('queue', 'message_count', 'consumer_count'))

basic_return_t = namedtuple('basic_return_t', ('reply_code', 'reply_text', 'exchange', 'routing_key', 'message'))


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

    Start = (10, 10)
    StartOk = (10, 11)
    Secure = (10, 20)
    SecureOk = (10, 21)
    Tune = (10, 30)
    TuneOk = (10, 31)
    Open = (10, 40)
    OpenOk = (10, 41)
    Close = (10, 50)
    CloseOk = (10, 51)
    Blocked = (10, 60)
    Unblocked = (10, 61)


class Channel:
    CLASS_ID = 20

    Open = (20, 10)
    OpenOk = (20, 11)
    Flow = (20, 20)
    FlowOk = (20, 21)
    Close = (20, 40)
    CloseOk = (20, 41)


class Exchange:
    CLASS_ID = 40

    Declare = (40, 10)
    DeclareOk = (40, 11)
    Delete = (40, 20)
    DeleteOk = (40, 21)
    Bind = (40, 30)
    BindOk = (40, 31)
    Unbind = (40, 40)
    UnbindOk = (40, 51)


class Queue:
    CLASS_ID = 50

    Declare = (50, 10)
    DeclareOk = (50, 11)
    Bind = (50, 20)
    BindOk = (50, 21)
    Purge = (50, 30)
    PurgeOk = (50, 31)
    Delete = (50, 40)
    DeleteOk = (50, 41)
    Unbind = (50, 50)
    UnbindOk = (50, 51)


class Basic:
    CLASS_ID = 60

    Qos = (60, 10)
    QosOk = (60, 11)
    Consume = (60, 20)
    ConsumeOk = (60, 21)
    Cancel = (60, 30)
    CancelOk = (60, 31)
    Publish = (60, 40)
    Return = (60, 50)
    Deliver = (60, 60)
    Get = (60, 70)
    GetOk = (60, 71)
    GetEmpty = (60, 80)
    Ack = (60, 80)
    Reject = (60, 90)
    RecoverAsync = (60, 100)
    Recover = (60, 110)
    RecoverOk = (60, 111)


class Confirm:
    CLASS_ID = 85

    Select = (85, 10)
    SelectOk = (85, 11)


class Tx:
    CLASS_ID = 90

    Select = (90, 10)
    SelectOk = (90, 11)
    Commit = (90, 20)
    CommitOk = (90, 21)
    Rollback = (90, 30)
    RollbackOk = (90, 31)


class Frame:
    """AMQP Frame

        offset:     0      1         3         7                      size+7      size+8
                    +------+---------+---------+    +-------------+     +-----------+
                    | type | channel |  size   |    |   payload   |     | frame-end |
                    +------+---------+---------+    +-------------+     +-----------+
        bytes           1       2         4              size                 1
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
    def __init__(self, method_tup, args=None, content=None, channel=None):
        """
        :param method_tup: method tuple consisting of class_id and method_id
        :param args: method args
        :param content: content
        :param channel: the associated channel, if any
        :type method_tup: tuple(int, int)
        :type args: AMQPReader or AMQPWriter or None
        :type content: amqp.message.GenericContent
        :type channel: int or None
        """
        self.method_tup = method_tup
        if isinstance(args, AMQPReader) or isinstance(args, AMQPWriter):
            self.args = args
        elif args is None:
            self.args = AMQPWriter()
        else:
            raise ValueError('args must be an `AMQPReader` or `AMQPWriter` instance')
        self.content = content
        self.channel = channel
