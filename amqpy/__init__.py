VERSION = (0, 4, 2)
__version__ = '.'.join(map(str, VERSION[0:3])) + ''.join(VERSION[3:])
__author__ = 'veegee'
__maintainer__ = 'veegee'
__contact__ = 'veegee@veegee.org'
__homepage__ = 'http://github.com/veegee/amqpy'
__docformat__ = 'restructuredtext'

from .message import Message
from .channel import Channel
from .connection import Connection
from .exceptions import (
    Timeout,
    AMQPError,
    AMQPConnectionError,
    RecoverableConnectionError,
    IrrecoverableConnectionError,
    ChannelError,
    RecoverableChannelError,
    IrrecoverableChannelError,
    ConsumerCancelled,
    ContentTooLarge,
    NoConsumers,
    ConnectionForced,
    InvalidPath,
    AccessRefused,
    NotFound,
    ResourceLocked,
    PreconditionFailed,
    FrameError,
    FrameSyntaxError,
    InvalidCommand,
    ChannelNotOpen,
    UnexpectedFrame,
    ResourceError,
    NotAllowed,
    AMQPNotImplementedError,
    InternalError,
    error_for_code,
    __all__ as _all_exceptions,
)

__all__ = ['Connection', 'Channel', 'Message'] + _all_exceptions
