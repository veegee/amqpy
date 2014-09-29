"""Error Handling

AMQP uses exceptions to handle errors.
* Any operational error (message queue not found, insufficient access rights, etc.) results in a channel exception.
* Any structural error (invalid argument, bad sequence of methods, etc.) results in a connection exception.

An exception closes the associated channel or connection, and returns a reply code and reply text to the client.
"""
import struct
from collections import namedtuple

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


class Timeout(Exception):
    """General AMQP operation timeout
    """

    def __init__(self):
        pass


class AMQPError(Exception):
    code = 0

    def __init__(self, reply_text=None, method_type=None, method_name=None, reply_code=None, channel_id=None):
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
        super().__init__(self, reply_code, reply_text, method_type, self.method_name, channel_id)

    def __str__(self):
        if self.method:
            return '{0.method} [ch: #{0.channel_id}]: ({0.reply_code}) {0.reply_text}'.format(self)
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
    code = 311


class NoConsumers(RecoverableChannelError):
    code = 313


class ConnectionForced(RecoverableConnectionError):
    code = 320


class InvalidPath(IrrecoverableConnectionError):
    code = 402


class AccessRefused(IrrecoverableChannelError):
    code = 403


class NotFound(IrrecoverableChannelError):
    code = 404


class ResourceLocked(RecoverableChannelError):
    code = 405


class PreconditionFailed(IrrecoverableChannelError):
    code = 406


class FrameError(IrrecoverableConnectionError):
    code = 501


class FrameSyntaxError(IrrecoverableConnectionError):
    code = 502


class InvalidCommand(IrrecoverableConnectionError):
    code = 503


class ChannelNotOpen(IrrecoverableConnectionError):
    code = 504


class UnexpectedFrame(IrrecoverableConnectionError):
    code = 505


class ResourceError(RecoverableConnectionError):
    code = 506


class NotAllowed(IrrecoverableConnectionError):
    code = 530


class AMQPNotImplementedError(IrrecoverableConnectionError):
    code = 540


class InternalError(IrrecoverableConnectionError):
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
