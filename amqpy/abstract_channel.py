"""Code common to Connection and Channel objects
"""
from abc import ABCMeta, abstractmethod

from .exceptions import AMQPNotImplementedError, RecoverableConnectionError
from .spec import Method
from . import spec


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
        self.method_queue = []  # list[Method] higher level queue for methods
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
        method = self._wait_method(allowed_methods)
        return self.dispatch_method(method)

    def _wait_method(self, allowed_methods):
        """Wait for a method from the server destined for a particular channel

        :return: method
        :rtype: Method
        """
        # check the channel's deferred methods
        method_queue = self.connection.channels[self.channel_id].method_queue

        for queued_method in method_queue:
            assert isinstance(queued_method, Method)
            if allowed_methods is None \
                    or queued_method.method_tup in allowed_methods \
                    or queued_method.method_tup == spec.Channel.Close:
                method_queue.remove(queued_method)
                return queued_method

                # nothing queued, need to wait for a method from the peer
        while True:
            channel, method_tup, args, content = self.connection.method_reader.read_method()
            method = Method(method_tup, args, content)

            if channel == self.channel_id \
                    and (allowed_methods is None or method_tup in allowed_methods or method_tup == spec.Channel.Close):
                return method

            # certain methods like basic_return should be dispatched immediately rather than being queued, even if
            # they're not one of the 'allowed_methods' we're looking for
            if channel and method_tup in self.connection.Channel._IMMEDIATE_METHODS:
                self.connection.channels[channel].dispatch_method(method)
                continue

            # not the channel and/or method we were looking for; queue this method for later
            self.connection.hannels[channel].method_queue.append(method)

            # If we just queued up a method for channel 0 (the Connection itself) it's probably a close method in
            # reaction to some error, so deal with it right away.
            if not channel:
                self.wait()

    def _wait_multiple(self, channels, allowed_methods, timeout=None):
        """
        :param channels: channels
        :param allowed_methods: list of allowed methods
        :param timeout: timeout
        :return: tuple of channel_id and method
        :rtype: tuple(int, Method)
        """
        for channel_id, channel in channels.items():
            method_queue = channel.method_queue
            for queued_method in method_queue:
                assert isinstance(queued_method, Method)
                if allowed_methods is None \
                        or queued_method.method_tup in allowed_methods \
                        or queued_method.method_tup == spec.Channel.Close:
                    method_queue.remove(queued_method)
                    return channel_id, queued_method

        # nothing queued, need to wait for a method from the peer
        while True:
            channel, method_tup, args, content = self.connection.method_reader.read_method(timeout)
            method = Method(method_tup, args, content)

            if channel in channels \
                    and (allowed_methods is None or method_tup in allowed_methods or method_tup == spec.Channel.Close):
                return channel, method

            # not the channel and/or method we were looking for; queue this method for later
            channels[channel].method_queue.append(method)

            # if we just queued up a method for channel 0 (the Connection itself) it's probably a close method in
            # reaction to some error, so deal with it right away
            if channel == 0:
                self.wait()

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
