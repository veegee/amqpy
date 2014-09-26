"""Code common to Connection and Channel objects
"""
from abc import ABCMeta, abstractmethod

from .exceptions import AMQPNotImplementedError, RecoverableConnectionError
from .serialization import AMQPWriter
from .spec import Method


__all__ = ['AbstractChannel']


class AbstractChannel(metaclass=ABCMeta):
    """Superclass for both the Connection, which is treated as channel 0, and other user-created Channel objects

    The subclasses must have a _METHOD_MAP class variable, mapping between AMQP method signatures and Python methods.
    """

    # : Placeholder, implementations must override this
    _METHOD_MAP = {}

    def __init__(self, connection, channel_id):
        self.connection = connection
        self.channel_id = channel_id
        connection.channels[channel_id] = self
        self.method_queue = []  # higher level queue for methods
        self.auto_decode = False

    @abstractmethod
    def close(self):
        """Close this Channel or Connection
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _send_method(self, method):
        if self.connection is None:
            raise RecoverableConnectionError('connection already closed')

        self.connection.method_writer.write_method(self.channel_id, method)

    def wait(self, allowed_methods=None):
        """Wait for a method that matches our allowed_methods parameter and dispatch to it

        The default value of None means match any method.
        """
        method = self.connection._wait_method(self.channel_id, allowed_methods)

        return self.dispatch_method(method)

    def dispatch_method(self, method):
        content = method.content
        if content and self.auto_decode and hasattr(content, 'content_encoding'):
            try:
                content.body = content.body.decode(content.content_encoding)
            except Exception:
                pass

        try:
            callback = self._METHOD_MAP[method.method_tup]
        except KeyError:
            raise AMQPNotImplementedError('Unknown AMQP method {0}'.format(method.method_tup))

        if content is None:
            return callback(self, method.args)
        else:
            return callback(self, method.args, method.content)
