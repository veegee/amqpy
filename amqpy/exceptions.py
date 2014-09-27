import struct
from collections import namedtuple

method_t = namedtuple('method_t', ('class_id', 'method_id'))

__all__ = [
    'Timeout',
    'AMQPError',
    'ConnectionError', 'ChannelError',
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

    def __init__(self, reply_text=None, method_sig=None, method_name=None, reply_code=None):
        self.message = reply_text
        self.reply_code = reply_code or self.code
        self.reply_text = reply_text
        self.method_sig = method_sig
        self.method_name = method_name or ''
        if method_sig and not self.method_name:
            self.method_name = METHOD_NAME_MAP.get(method_sig, '')
        Exception.__init__(self, reply_code, reply_text, method_sig, self.method_name)

    def __str__(self):
        if self.method:
            return '{0.method}: ({0.reply_code}) {0.reply_text}'.format(self)
        return self.reply_text or '<AMQPError: unknown error>'

    @property
    def method(self):
        return self.method_name or self.method_sig


class ConnectionError(AMQPError):
    pass


class ChannelError(AMQPError):
    pass


class RecoverableChannelError(ChannelError):
    pass


class IrrecoverableChannelError(ChannelError):
    pass


class RecoverableConnectionError(ConnectionError):
    pass


class IrrecoverableConnectionError(ConnectionError):
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


def error_for_code(code, text, method, default):
    try:
        return ERROR_MAP[code](text, method, reply_code=code)
    except KeyError:
        return default(text, method, reply_code=code)


def raise_for_code(code, text, method, default):
    raise error_for_code(code, text, method, default)


METHOD_NAME_MAP = {
    method_t(10, 10): 'Connection.start',
    method_t(10, 11): 'Connection.start_ok',
    method_t(10, 20): 'Connection.secure',
    method_t(10, 21): 'Connection.secure_ok',
    method_t(10, 30): 'Connection.tune',
    method_t(10, 31): 'Connection.tune_ok',
    method_t(10, 40): 'Connection.open',
    method_t(10, 41): 'Connection.open_ok',
    method_t(10, 50): 'Connection.close',
    method_t(10, 51): 'Connection.close_ok',
    method_t(20, 10): 'Channel.open',
    method_t(20, 11): 'Channel.open_ok',
    method_t(20, 20): 'Channel.flow',
    method_t(20, 21): 'Channel.flow_ok',
    method_t(20, 40): 'Channel.close',
    method_t(20, 41): 'Channel.close_ok',
    method_t(30, 10): 'Access.request',
    method_t(30, 11): 'Access.request_ok',
    method_t(40, 10): 'Exchange.declare',
    method_t(40, 11): 'Exchange.declare_ok',
    method_t(40, 20): 'Exchange.delete',
    method_t(40, 21): 'Exchange.delete_ok',
    method_t(40, 30): 'Exchange.bind',
    method_t(40, 31): 'Exchange.bind_ok',
    method_t(40, 40): 'Exchange.unbind',
    method_t(40, 41): 'Exchange.unbind_ok',
    method_t(50, 10): 'Queue.declare',
    method_t(50, 11): 'Queue.declare_ok',
    method_t(50, 20): 'Queue.bind',
    method_t(50, 21): 'Queue.bind_ok',
    method_t(50, 30): 'Queue.purge',
    method_t(50, 31): 'Queue.purge_ok',
    method_t(50, 40): 'Queue.delete',
    method_t(50, 41): 'Queue.delete_ok',
    method_t(50, 50): 'Queue.unbind',
    method_t(50, 51): 'Queue.unbind_ok',
    method_t(60, 10): 'Basic.qos',
    method_t(60, 11): 'Basic.qos_ok',
    method_t(60, 20): 'Basic.consume',
    method_t(60, 21): 'Basic.consume_ok',
    method_t(60, 30): 'Basic.cancel',
    method_t(60, 31): 'Basic.cancel_ok',
    method_t(60, 40): 'Basic.publish',
    method_t(60, 50): 'Basic.return',
    method_t(60, 60): 'Basic.deliver',
    method_t(60, 70): 'Basic.get',
    method_t(60, 71): 'Basic.get_ok',
    method_t(60, 72): 'Basic.get_empty',
    method_t(60, 80): 'Basic.ack',
    method_t(60, 90): 'Basic.reject',
    method_t(60, 100): 'Basic.recover_async',
    method_t(60, 110): 'Basic.recover',
    method_t(60, 111): 'Basic.recover_ok',
    method_t(60, 120): 'Basic.nack',
    method_t(90, 10): 'Tx.select',
    method_t(90, 11): 'Tx.select_ok',
    method_t(90, 20): 'Tx.commit',
    method_t(90, 21): 'Tx.commit_ok',
    method_t(90, 30): 'Tx.rollback',
    method_t(90, 31): 'Tx.rollback_ok',
    method_t(85, 10): 'Confirm.select',
    method_t(85, 11): 'Confirm.select_ok',
}

for method_type, name in list(METHOD_NAME_MAP.items()):
    METHOD_NAME_MAP[struct.unpack('>I', struct.pack('>HH', *method_type))[0]] = name
