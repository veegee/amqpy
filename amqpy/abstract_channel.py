"""Code common to Connection and Channel objects
"""
from abc import ABCMeta, abstractmethod
from threading import Lock

from .exceptions import AMQPNotImplementedError, RecoverableConnectionError
from .spec import Method
from . import spec

__all__ = ['AbstractChannel']


class AbstractChannel(metaclass=ABCMeta):
    """Superclass for both the Connection, which is treated as channel 0, and other user-created Channel objects

    The subclasses must have a METHOD_MAP class variable, mapping between AMQP method signatures and Python methods.
    """

    #: placeholder, implementations must override this
    METHOD_MAP = {}

    def __init__(self, connection, channel_id):
        self.connection = connection
        self.channel_id = channel_id
        connection.channels[channel_id] = self
        # list[Method]
        self.method_queue = []  # queue of incoming methods for this channel
        self.auto_decode = False
        self.lock = Lock()

    @abstractmethod
    def close(self):
        """Close this Channel or Connection
        """
        pass

    def __enter__(self):
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _send_method(self, method):
        if self.connection is None:
            raise RecoverableConnectionError('connection already closed')

        self.connection.method_writer.write_method(self.channel_id, method)

    def wait(self, allowed_methods=None, callback=None):
        """Wait for a method that matches any one of `allowed_methods`

        If `callback` is specified, `callback(channel, method)` will be called after a method has been received. If
        `callback` is `None`, the default callback for the received method will be called (as specified in the channel's
        `METHOD_MAP`).

        :param allowed_methods: list of possible methods to wait for, or `None` to wait for any method
        :param callback: callable with the following signature: callable(AbstractChannel, Method)
        :type allowed_methods: list or None
        :type callback: callable(AbstractChannel, Method)
        """
        # TODO: implement callback parameter
        method = self._wait_method(allowed_methods)
        return self.handle_method(method)

    def _wait_method(self, allowed_methods):
        """Wait for a method from the server destined for the current channel

        This method is designed to be called from a channel instance.

        :return: method
        :rtype: Method
        """
        # create a more convenient list of methods to check
        if isinstance(allowed_methods, list):
            # we should always check of the incoming method is `Channel.Close`
            allowed_methods = [spec.Channel.Close] + allowed_methods

        # check the channel's method queue
        method_queue = self.connection.channels[self.channel_id].method_queue

        for qm in method_queue:
            assert isinstance(qm, Method)
            if allowed_methods is None or qm.method_type in allowed_methods:
                # found the method we're looking for in the queue
                method_queue.remove(qm)
                return qm

        # nothing queued, need to wait for a method from the server
        while True:
            method = self.connection.method_reader.read_method()
            ch_id = method.channel_id
            m_type = method.method_type

            # check if the received method is the one we're waiting for
            if method.channel_id == self.channel_id and (allowed_methods is None or m_type in allowed_methods):
                # received the method we're waiting for
                return method

            # check if the received method needs to be handled immediately
            if ch_id != 0 and m_type in self.connection.Channel.IMMEDIATE_METHODS:
                # certain methods like basic_return should be dispatched immediately rather than being queued, even if
                # they're not one of the `allowed_methods` we're looking for
                self.connection.channels[ch_id].handle_method(method)
                continue

            # not the channel and/or method we were looking for; queue this method for later
            self.connection.channels[ch_id].method_queue.append(method)

            # if the method is destined for channel 0 (the connection itself), it's probably an exception, so handle it
            # immediately
            if ch_id == 0:
                self.connection.wait()

    def _wait_multiple(self, channels, allowed_methods, timeout=None):
        """Wait for an event on multiple channels

        It only makes sense for the `Connection` instance to call this method (as opposed to `Channel` instances).
        This method gets called by :meth:`Connection.drain_events()`.

        :param channels: dict of channels to watch
        :param allowed_methods: list of allowed methods
        :param timeout: timeout
        :type channels: dict[ch_id int: channel Channel]
        :return: tuple(channel_id, method)
        :rtype: tuple(int, Method)
        """
        # create a more convenient list of methods to check
        if isinstance(allowed_methods, list):
            # we should always check of the incoming method is `Channel.Close`
            allowed_methods = [spec.Channel.Close] + allowed_methods

        # check the method queue of each channel
        for ch_id, channel in channels.items():
            for qm in channel.method_queue:
                assert isinstance(qm, Method)
                if allowed_methods is None or qm.method_type in allowed_methods:
                    channel.method_queue.remove(qm)
                    return ch_id, qm

        # nothing queued for any channel, need to wait for a method from the server
        while True:
            method = self.connection.method_reader.read_method(timeout)
            m_type = method.method_type

            # check if the received method is the one we're waiting for
            if channel in channels and (allowed_methods is None or m_type in allowed_methods):
                return channel, method

            # not the channel and/or method we were looking for; queue this method for later
            channels[channel].method_queue.append(method)

            # if the method is destined for channel 0 (the connection itself), it's probably an exception, so handle it
            # immediately
            if channel == 0:
                self.connection.wait()

    def handle_method(self, method):
        """Handle the specified received method

        The appropriate handler as defined in `self.METHOD_MAP` will be called to handle this method.

        :param method: freshly received method from the server
        :type method: amqpy.spec.Method
        :return: the return value of the specific method handler
        """
        #: :type: GenericContent
        content = method.content

        if content and self.auto_decode and 'content_encoding' in content.properties:
            # try to decode message body
            try:
                content.body = content.body.decode(content.properties['content_encoding'])
            except Exception:
                pass

        try:
            callback = self.METHOD_MAP[method.method_type]
        except KeyError:
            raise AMQPNotImplementedError('Unknown AMQP method {0}'.format(method.method_type))

        return callback(self, method)
