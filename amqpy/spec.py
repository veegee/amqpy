from __future__ import absolute_import, division, print_function

__metaclass__ = type
from collections import namedtuple

queue_declare_ok_t = namedtuple('queue_declare_ok_t', ['queue', 'message_count', 'consumer_count'])

basic_return_t = namedtuple('basic_return_t',
                            ['reply_code', 'reply_text', 'exchange', 'routing_key', 'message'])

method_t = namedtuple('method_t', ['class_id', 'method_id'])

#: The default, minimum frame size that both the client and server must be able to handle
FRAME_MIN_SIZE = 4096


class FrameType:
    """This class contains frame-related constants

    METHOD, HEADER, BODY, and HEARTBEAT are all frame type constants which make up the first byte
    of every frame. The END constant is the termination value which is the last byte of every frame.
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
    GetEmpty = method_t(60, 72)
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
