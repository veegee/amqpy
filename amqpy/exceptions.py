"""
AMQP uses exceptions to handle errors:

* Any operational error (message queue not found, insufficient access rights, etc.) results in a
  channel exception.
* Any structural error (invalid argument, bad sequence of methods, etc.) results in a connection
  exception.

According to the AMQP specification, an exception closes the associated channel or connection,  and
returns a reply code and reply text to the client. However, amqpy will automatically re-open the
channel after a channel error.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
import struct
from collections import namedtuple

from . import compat

compat.patch()  # monkey-patch builtins.TimeoutError

method_t = namedtuple('method_t', ('class_id', 'method_id'))

__all__ = [
    'Timeout',
    'AMQPError',
    'AMQPConnectionError', 'ChannelError',
    'RecoverableConnectionError', 'IrrecoverableConnectionError',
    'RecoverableChannelError', 'IrrecoverableChannelError',
    'ConsumerCancelled', 'ContentTooLarge', 'NoConsumers',
    'ConnectionForced', 'InvalidPath', 'AccessRefused', 'NotFound',
    'ResourceLocked', 'PreconditionFailed', 'FrameError', 'FrameSyntaxError',
    'InvalidCommand', 'ChannelNotOpen', 'UnexpectedFrame', 'ResourceError',
    'NotAllowed', 'AMQPNotImplementedError', 'InternalError',
]


class Timeout(TimeoutError):
    """General AMQP operation timeout
    """
    pass


class AMQPError(Exception):
    code = 0

    def __init__(self, reply_text=None, method_type=None, method_name=None, reply_code=None,
                 channel_id=None):
        """
        :param reply_text: localized reply text
        :param method_type: method type
        :param method_name: method name
        :param reply_code: AMQP reply (exception) code
        :param channel_id: associated channel ID, if any
        :type reply_text: str or None
        :type method_type: amqpy.spec.method_t or None
        :type method_name: str or None
        :type reply_code: int or None
        :type channel_id: int or None
        """
        self.message = reply_text
        self.reply_code = reply_code or self.code
        self.reply_text = reply_text
        self.method_type = method_type
        self.method_name = method_name or ''
        if method_type and not self.method_name:
            self.method_name = METHOD_NAME_MAP.get(method_type, '')
        self.channel_id = channel_id
        super(AMQPError, self).__init__(self, reply_code, reply_text, method_type, self.method_name, channel_id)

    def __str__(self):
        if self.method:
            return '{0.method} [ch: {0.channel_id}]: ({0.reply_code}) {0.reply_text}'.format(self)
        return self.reply_text or '<AMQPError: unknown error>'

    @property
    def method(self):
        return self.method_name or self.method_type


class AMQPConnectionError(AMQPError):
    pass


class ChannelError(AMQPError):
    pass


class RecoverableChannelError(ChannelError):
    pass


class IrrecoverableChannelError(ChannelError):
    pass


class RecoverableConnectionError(AMQPConnectionError):
    pass


class IrrecoverableConnectionError(AMQPConnectionError):
    pass


class Blocked(RecoverableConnectionError):
    pass


class ConsumerCancelled(RecoverableConnectionError):
    pass


class ContentTooLarge(RecoverableChannelError):
    """The client attempted to transfer content larger than the server could accept at the present
    time. The client may retry at a later time.
    """
    code = 311


class NoConsumers(RecoverableChannelError):
    """When the exchange cannot deliver to a consumer when the immediate flag is set. As a result
    of pending data on the queue or the absence of any consumers of the queue.
    """
    code = 313


class ConnectionForced(RecoverableConnectionError):
    """An operator intervened to close the connection for some reason. The client may retry at some
    later date.
    """
    code = 320


class InvalidPath(IrrecoverableConnectionError):
    """The client tried to work with an unknown virtual host.
    """
    code = 402


class AccessRefused(IrrecoverableChannelError):
    """The client attempted to work with a server entity to which it has no access due to
    security settings.
    """
    code = 403


class NotFound(IrrecoverableChannelError):
    """The client attempted to work with a server entity that does not exist.
    """
    code = 404


class ResourceLocked(RecoverableChannelError):
    """The client attempted to work with a server entity to which it has no access because
    another client is working
    with it.
    """
    code = 405


class PreconditionFailed(IrrecoverableChannelError):
    """The client requested a method that was not allowed because some precondition failed.
    """
    code = 406


class FrameError(IrrecoverableConnectionError):
    """The sender sent a malformed frame that the recipient could not decode. This strongly implies
    a programming error in the sending peer.
    """
    code = 501


class FrameSyntaxError(IrrecoverableConnectionError):
    """The sender sent a frame that contained illegal values for one or more fields. This strongly
    implies a programming error in the sending peer.
    """
    code = 502


class InvalidCommand(IrrecoverableConnectionError):
    """The client sent an invalid sequence of frames, attempting to perform an operation that was
    considered invalid by the server. This usually implies a programming error in the client.
    """
    code = 503


class ChannelNotOpen(IrrecoverableConnectionError):
    """The client attempted to work with a channel that had not been correctly opened. This most
    likely indicates a fault in the client layer.
    """
    code = 504


class UnexpectedFrame(IrrecoverableConnectionError):
    """The peer sent a frame that was not expected, usually in the context of a content header and
    body. This strongly indicates a fault in the peer's content processing.
    """
    code = 505


class ResourceError(RecoverableConnectionError):
    """The server could not complete the method because it lacked sufficient resources. This may be
    due to the client creating too many of some type of entity.
    """
    code = 506


class NotAllowed(IrrecoverableConnectionError):
    """The client tried to work with some entity in a manner that is prohibited by the server,  due
    to security settings or by some other criteria.
    """
    code = 530


class AMQPNotImplementedError(IrrecoverableConnectionError):
    """The client tried to use functionality that is not implemented in the server.
    """
    code = 540


class InternalError(IrrecoverableConnectionError):
    """The server could not complete the method because of an internal error. The server may
    require intervention by an operator in order to resume normal operations.
    """
    code = 541


ERROR_MAP = {
    311: ContentTooLarge,
    313: NoConsumers,
    320: ConnectionForced,
    402: InvalidPath,
    403: AccessRefused,
    404: NotFound,
    405: ResourceLocked,
    406: PreconditionFailed,
    501: FrameError,
    502: FrameSyntaxError,
    503: InvalidCommand,
    504: ChannelNotOpen,
    505: UnexpectedFrame,
    506: ResourceError,
    530: NotAllowed,
    540: AMQPNotImplementedError,
    541: InternalError,
}


def error_for_code(code, text, meth_type, default, channel_id=None):
    """Get exception class associated with specified error code

    :param int code: AMQP reply code
    :param str text: localized reply text
    :param meth_type: method type
    :param default: default exception class if error code cannot be matched with an exception class
    :param channel_id: optional associated channel ID
    :type meth_type: amqpy.spec.method_t
    :type default: Callable
    :type channel_id: int or None
    :return: Exception object
    :rtype: Exception
    """
    try:
        exc = ERROR_MAP[code]
        return exc(text, meth_type, reply_code=code, channel_id=channel_id)
    except KeyError:
        return default(text, meth_type, reply_code=code, channel_id=channel_id)


METHOD_NAME_MAP = {
    method_t(10, 10): 'connection.start',
    method_t(10, 11): 'connection.start-ok',
    method_t(10, 20): 'connection.secure',
    method_t(10, 21): 'connection.secure-ok',
    method_t(10, 30): 'connection.tune',
    method_t(10, 31): 'connection.tune-ok',
    method_t(10, 40): 'connection.open',
    method_t(10, 41): 'connection.open-ok',
    method_t(10, 50): 'connection.close',
    method_t(10, 51): 'connection.close-ok',
    method_t(10, 60): 'connection.blocked',
    method_t(10, 61): 'connection.unblocked',
    method_t(20, 10): 'channel.open',
    method_t(20, 11): 'channel.open-ok',
    method_t(20, 20): 'channel.flow',
    method_t(20, 21): 'channel.flow-ok',
    method_t(20, 40): 'channel.close',
    method_t(20, 41): 'channel.close-ok',
    method_t(30, 10): 'access.request',
    method_t(30, 11): 'access.request-ok',
    method_t(40, 10): 'exchange.declare',
    method_t(40, 11): 'exchange.declare-ok',
    method_t(40, 20): 'exchange.delete',
    method_t(40, 21): 'exchange.delete-ok',
    method_t(40, 30): 'exchange.bind',
    method_t(40, 31): 'exchange.bind-ok',
    method_t(40, 40): 'exchange.unbind',
    method_t(40, 51): 'exchange.unbind-ok',
    method_t(50, 10): 'queue.declare',
    method_t(50, 11): 'queue.declare-ok',
    method_t(50, 20): 'queue.bind',
    method_t(50, 21): 'queue.bind-ok',
    method_t(50, 30): 'queue.purge',
    method_t(50, 31): 'queue.purge-ok',
    method_t(50, 40): 'queue.delete',
    method_t(50, 41): 'queue.delete-ok',
    method_t(50, 50): 'queue.unbind',
    method_t(50, 51): 'queue.unbind-ok',
    method_t(60, 10): 'basic.qos',
    method_t(60, 11): 'basic.qos-ok',
    method_t(60, 20): 'basic.consume',
    method_t(60, 21): 'basic.consume-ok',
    method_t(60, 30): 'basic.cancel',
    method_t(60, 31): 'basic.cancel-ok',
    method_t(60, 40): 'basic.publish',
    method_t(60, 50): 'basic.return',
    method_t(60, 60): 'basic.deliver',
    method_t(60, 70): 'basic.get',
    method_t(60, 71): 'basic.get-ok',
    method_t(60, 72): 'basic.get-empty',
    method_t(60, 80): 'basic.ack',
    method_t(60, 90): 'basic.reject',
    method_t(60, 100): 'basic.recover-async',
    method_t(60, 110): 'basic.recover',
    method_t(60, 111): 'basic.recover-ok',
    method_t(60, 120): 'basic.nack',
    method_t(90, 10): 'tx.select',
    method_t(90, 11): 'tx.select-ok',
    method_t(90, 20): 'tx.commit',
    method_t(90, 21): 'tx.commit-ok',
    method_t(90, 30): 'tx.rollback',
    method_t(90, 31): 'tx.rollback-ok',
    method_t(85, 10): 'confirm.select',
    method_t(85, 11): 'confirm.select-ok',
}


# insert keys which are 4-byte unsigned int representations of a method type for easy lookups
for mt, name in list(METHOD_NAME_MAP.items()):
    data = struct.pack('>HH', *mt)
    METHOD_NAME_MAP[struct.unpack('>I', data)[0]] = name
