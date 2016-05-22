"""AMQP Channels
"""
from __future__ import absolute_import, division, print_function

__metaclass__ = type
import logging
import six

if six.PY2:
    from Queue import Queue
else:
    from queue import Queue

from .proto import Method
from .concurrency import synchronized_connection
from .abstract_channel import AbstractChannel
from .exceptions import ChannelError, ConsumerCancelled, error_for_code
from .spec import basic_return_t, queue_declare_ok_t, method_t
from .serialization import AMQPWriter
from . import spec

__all__ = ['Channel']

log = logging.getLogger('amqpy')


class Channel(AbstractChannel):
    """
    The channel class provides methods for a client to establish and operate an AMQP channel. All
    public members are fully thread-safe.
    """
    ### constants
    #: Default channel mode
    CH_MODE_NONE = 0
    #: Transaction mode
    CH_MODE_TX = 1
    #: Publisher confirm mode (RabbitMQ extension)
    CH_MODE_CONFIRM = 2

    def __init__(self, connection, channel_id=None, auto_decode=True):
        """Create a channel bound to a connection and using the specified numeric channel_id, and
        open on the server

        If `auto_decode` is enabled (default), incoming Message bodies will be automatically decoded
        to `str` if possible.

        :param connection: the channel's associated Connection
        :param channel_id: the channel's assigned channel ID
        :param auto_decode: enable auto decoding of message bodies
        :type connection: amqpy.connection.Connection
        :type channel_id: int or None
        :type auto_decode: bool
        """
        if channel_id:
            # noinspection PyProtectedMember
            connection._claim_channel_id(channel_id)
        else:
            # noinspection PyProtectedMember
            channel_id = connection._get_free_channel_id()

        super(Channel, self).__init__(connection, channel_id)

        # auto decode received messages
        self.auto_decode = auto_decode

        ### channel state variables:

        #: Current channel open/closed state
        #:
        #: :type: bool
        self.is_open = False

        #: Current channel active state (flow control)
        #:
        #: :type: bool
        self.active = True

        #: Channel mode state (default, transactional, publisher confirm)
        #:
        #: :type: int
        self.mode = 0

        #: Returned messages that the server was unable to deliver
        #:
        #: :type: queue.Queue
        self.returned_messages = Queue()

        # consumer callbacks dict[consumer_tag str: callable]
        self.callbacks = {}

        # consumer cancel callbacks dict dict[consumer_tag str: callable]
        self.cancel_callbacks = {}

        # set of consumers that have opted for `no_ack` delivery (server will not expect an ack
        # for delivered messages)
        self.no_ack_consumers = set()

        # open the channel
        self._open()

    def _close(self):
        """Tear down this object, after we've agreed to close with the server
        """
        log.debug('Channel close #{}'.format(self.channel_id))
        self.is_open = False
        channel_id, self.channel_id = self.channel_id, None
        connection, self.connection = self.connection, None
        if connection:
            connection.channels.pop(channel_id, None)
            # noinspection PyProtectedMember
            connection._avail_channel_ids.append(channel_id)
        self.callbacks.clear()
        self.cancel_callbacks.clear()
        self.no_ack_consumers.clear()

    def _open(self):
        """Open the channel
        """
        if self.is_open:
            return

        self._send_open()

    def _revive(self):
        self.is_open = False
        self.mode = self.CH_MODE_NONE
        self._send_open()

    @synchronized_connection()
    def close(self, reply_code=0, reply_text='', method_type=method_t(0, 0)):
        """Request a channel close

        This method indicates that the sender wants to close the channel. This may be due to
        internal conditions (e.g. a forced shut-down) or due to an error handling a specific method,
        i.e. an exception When a close is due to an exception, the sender provides the class and
        method id of the method which caused the exception.

        :param reply_code: the reply code
        :param reply_text: localized reply text
        :param method_type: if close is triggered by a failing method, this is the method that
            caused it
        :type reply_code: int
        :type reply_text: str
        :type method_type: amqpy.spec.method_t
        """
        try:
            if not self.is_open or self.connection is None:
                return

            args = AMQPWriter()
            args.write_short(reply_code)
            args.write_shortstr(reply_text)
            args.write_short(method_type.class_id)
            args.write_short(method_type.method_id)
            self._send_method(Method(spec.Channel.Close, args))
            return self.wait_any([spec.Channel.Close, spec.Channel.CloseOk])
        finally:
            self.connection = None

    def _cb_close(self, method):
        """Respond to a channel close sent by the server

        This method indicates that the sender (server) wants to close the channel. This may be due
        to internal conditions (e.g. a forced shut-down) or due to an error handling a specific
        method, i.e. an exception. When a close is due to an exception, the sender provides the
        class and method id of the method which caused the exception.

        This method sends a "close-ok" to the server, then re-opens the channel.
        """
        args = method.args
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        class_id = args.read_short()
        method_id = args.read_short()

        self._send_method(Method(spec.Channel.CloseOk))
        self.is_open = False

        # re-open the channel
        self._revive()

        # get information about the method which caused the server to close the channel
        method_type = method_t(class_id, method_id)
        raise error_for_code(reply_code, reply_text, method_type, ChannelError, self.channel_id)

    def _cb_close_ok(self, method):
        """Confirm a channel close

        This method confirms a Channel.Close method and tells the recipient that it is safe to
        release resources for the channel and close the socket.
        """
        assert method
        self._close()

    @synchronized_connection()
    def flow(self, active):
        """Enable/disable flow from peer

        This method asks the peer to pause or restart the flow of content data. This is a simple
        flow-control mechanism that a peer can use to avoid overflowing its queues or otherwise
        finding itself receiving more messages than it can process. Note that this method is not
        intended for window control The peer that receives a request to stop sending content
        should finish sending the current content, if any, and then wait until it receives a Flow
        restart method.

        :param active: True: peer starts sending content frames; False: peer stops sending content
            frames
        :type active: bool
        """
        args = AMQPWriter()
        args.write_bit(active)
        self._send_method(Method(spec.Channel.Flow, args))
        return self.wait_any([spec.Channel.FlowOk, self._cb_flow_ok])

    def _cb_flow(self, method):
        """Enable/disable flow from peer

        This method asks the peer to pause or restart the flow of content data. This is a simple
        flow-control mechanism that a peer can use to avoid overflowing its queues or otherwise
        finding itself receiving more messages than it can process. Note that this method is not
        intended for window control The peer that receives a request to stop sending content
        should finish sending the current content, if any, and then wait until it receives a Flow
        restart method.
        """
        args = method.args
        self.active = args.read_bit()
        self._send_flow_ok(self.active)

    def _send_flow_ok(self, active):
        """Confirm a flow method

        Confirms to the peer that a flow command was received and processed.

        :param active: True: peer starts sending content frames; False: peer stops sending content
            frames
        :type active: bool
        """
        args = AMQPWriter()
        args.write_bit(active)
        self._send_method(Method(spec.Channel.FlowOk, args))

    def _cb_flow_ok(self, method):
        """Confirm a flow method

        Confirms to the peer that a flow command was received and processed.
        """
        args = method.args
        return args.read_bit()

    def _send_open(self):
        """Open a channel

        This method opens a channel.
        """
        args = AMQPWriter()
        args.write_shortstr('')  # reserved
        self._send_method(Method(spec.Channel.Open, args))
        return self.wait(spec.Channel.OpenOk)

    def _cb_open_ok(self, method):
        """Handle received "open-ok"

        The server sends this method to signal to the client that this channel is ready for use.
        """
        assert method
        self.is_open = True
        log.debug('Channel open')

    @synchronized_connection()
    def exchange_declare(self, exchange, exch_type, passive=False, durable=False, auto_delete=True,
                         nowait=False, arguments=None):
        """Declare exchange, create if needed

        * Exchanges cannot be redeclared with different types. The client MUST not attempt to
          redeclare an existing exchange with a different type than used in the original
          Exchange.Declare method.
        * This method creates an exchange if it does not already exist, and if the exchange
          exists, verifies that it is of the correct and expected class.
        * The server must ignore the `durable` field if the exchange already exists.
        * The server must ignore the `auto_delete` field if the exchange already exists.
        * If `nowait` is enabled and the server could not complete the method, it will raise a
          channel or connection exception.
        * `arguments` is ignored if passive is True.

        :param str exchange: exchange name
        :param str exch_type: exchange type (direct, fanout, etc.)
        :param bool passive: do not create exchange; client can use this to check whether an
            exchange exists
        :param bool durable: mark exchange as durable (remain active after server restarts)
        :param bool auto_delete: auto-delete exchange when all queues have finished using it
        :param bool nowait: if set, the server will not respond to the method and the client should
            not wait for a reply
        :param dict arguments: exchange declare arguments
        :raise AccessRefused: if attempting to declare an exchange with a reserved name (amq.*)
        :raise NotFound: if `passive` is enabled and the exchange does not exist
        :return: None
        """
        arguments = arguments or {}
        args = AMQPWriter()
        args.write_short(0)  # reserved-1
        args.write_shortstr(exchange)  # exchange name
        args.write_shortstr(exch_type)  # exchange type
        args.write_bit(passive)  # passive
        args.write_bit(durable)  # durable
        args.write_bit(auto_delete)  # auto-delete
        args.write_bit(False)  # internal
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Exchange.Declare, args))

        if not nowait:
            return self.wait(spec.Exchange.DeclareOk)

    def _cb_exchange_declare_ok(self, method):
        """Confirms an exchange declaration

        The server sends this method to confirm a Declare method and confirms the name of the
        exchange, essential for automatically-named exchanges.
        """
        pass

    @synchronized_connection()
    def exchange_delete(self, exchange, if_unused=False, nowait=False):
        """Delete an exchange

        This method deletes an exchange.

        * If the exchange does not exist, the server must raise a channel exception. When an
          exchange is deleted, all queue bindings on the exchange are cancelled.
        * If `if_unused` is set, and the exchange has queue bindings, the server must raise a
          channel exception.

        :param str exchange: exchange name
        :param bool if_unused: delete only if unused (has no queue bindings)
        :param bool nowait: if set, the server will not respond to the method and the client should
            not wait for a reply
        :raise NotFound: if exchange with `exchange` does not exist
        :raise PreconditionFailed: if attempting to delete a queue with bindings and `if_unused` is
            set
        :return: None
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(exchange)
        args.write_bit(if_unused)
        args.write_bit(nowait)
        self._send_method(Method(spec.Exchange.Delete, args))

        if not nowait:
            return self.wait(spec.Exchange.DeleteOk)

    def _cb_exchange_delete_ok(self, method):
        """Confirm deletion of an exchange

        The server sends this method to confirm that the deletion of an exchange was successful.
        """
        pass

    @synchronized_connection()
    def exchange_bind(self, dest_exch, source_exch='', routing_key='', nowait=False,
                      arguments=None):
        """Bind an exchange to an exchange

        * Both the `dest_exch` and `source_exch` must already exist. Blank exchange names mean
          the default exchange.
        * A server MUST allow and ignore duplicate bindings - that is, two or more bind methods
          for a specific exchanges, with identical arguments - without treating these as an error.
        * A server MUST allow cycles of exchange bindings to be created including allowing an
          exchange to be bound to itself.
        * A server MUST not deliver the same message more than once to a destination exchange,
          even if the topology of exchanges and bindings results in multiple (even infinite)
          routes to that exchange.

        :param str dest_exch: name of destination exchange to bind
        :param str source_exch: name of source exchange to bind
        :param str routing_key: routing key for the binding (note: not all exchanges use a
            routing key)
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :param dict arguments: binding arguments, specific to the exchange class
        """
        arguments = {} if arguments is None else arguments
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(dest_exch)
        args.write_shortstr(source_exch)
        args.write_shortstr(routing_key)
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Exchange.Bind, args))

        if not nowait:
            return self.wait(spec.Exchange.BindOk)

    @synchronized_connection()
    def exchange_unbind(self, dest_exch, source_exch='', routing_key='', nowait=False,
                        arguments=None):
        """Unbind an exchange from an exchange

        * If the unbind fails, the server must raise a connection exception. The server must not
            attempt to unbind an exchange that does not exist from an exchange.
        * Blank exchange names mean the default exchange.

        :param str dest_exch: destination exchange name
        :param str source_exch: source exchange name
        :param str routing_key: routing key to unbind
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :param dict arguments: binding arguments, specific to the exchange class
        """
        arguments = {} if arguments is None else arguments
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(dest_exch)
        args.write_shortstr(source_exch)
        args.write_shortstr(routing_key)
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Exchange.Unbind, args))

        if not nowait:
            return self.wait(spec.Exchange.UnbindOk)

    def _cb_exchange_bind_ok(self, method):
        """Confirm bind successful

        The server sends this method to confirm that the bind was successful.
        """
        pass

    def _cb_exchange_unbind_ok(self, method):
        """Confirm unbind successful

        The server sends this method to confirm that the unbind was successful.
        """
        pass

    @synchronized_connection()
    def queue_bind(self, queue, exchange='', routing_key='', nowait=False, arguments=None):
        """Bind queue to an exchange

        This method binds a queue to an exchange. Until a queue is bound it will not receive any
        messages. In a classic messaging model, store-and-forward queues are bound to a dest
        exchange and subscription queues are bound to a dest_wild exchange.

        * The server must allow and ignore duplicate bindings without treating these as an error.
        * If a bind fails, the server must raise a connection exception.
        * The server must not allow a durable queue to bind to a transient exchange. If a client
          attempts this, the server must raise a channel exception.
        * The server should support at least 4 bindings per queue, and ideally, impose no limit
          except as defined by available resources.

        * If the client did not previously declare a queue, and the `queue` is empty, the server
          must raise a connection exception with reply code 530 (not allowed).
        * If `queue` does not exist, the server must raise a channel exception with reply code
          404 (not found).
        * If `exchange` does not exist, the server must raise a channel exception with reply code
          404 (not found).

        :param str queue: name of queue to bind; blank refers to the last declared queue for this
            channel
        :param str exchange: name of exchange to bind to
        :param str routing_key: routing key for the binding
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :param dict arguments: binding arguments, specific to the exchange class
        """
        arguments = {} if arguments is None else arguments
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Queue.Bind, args))

        if not nowait:
            return self.wait(spec.Queue.BindOk)

    def _cb_queue_bind_ok(self, method):
        """Confirm bind successful

        The server sends this method to confirm that the bind was successful.
        """
        pass

    @synchronized_connection()
    def queue_unbind(self, queue, exchange, routing_key='', nowait=False, arguments=None):
        """Unbind a queue from an exchange

        This method unbinds a queue from an exchange.

        * If a unbind fails, the server MUST raise a connection exception.
        * The client must not attempt to unbind a queue that does not exist.
        * The client must not attempt to unbind a queue from an exchange that does not exist.

        :param str queue: name of queue to unbind, leave blank to refer to the last declared
            queue on this channel
        :param str exchange: name of exchange to unbind, leave blank to refer to default exchange
        :param str routing_key: routing key of binding
        :param dict arguments: binding arguments, specific to the exchange class
        """
        arguments = {} if arguments is None else arguments
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        # args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Queue.Unbind, args))

        if not nowait:
            return self.wait(spec.Queue.UnbindOk)

    def _cb_queue_unbind_ok(self, method):
        """Confirm unbind successful

        This method confirms that the unbind was successful.
        """
        pass

    @synchronized_connection()
    def queue_declare(self, queue='', passive=False, durable=False, exclusive=False,
                      auto_delete=True, nowait=False,
                      arguments=None):
        """Declare queue, create if needed

        This method creates or checks a queue. When creating a new queue the client can specify
        various properties that control the durability of the queue and its contents, and the level
        of sharing for the queue. A tuple containing the queue name, message count, and consumer
        count is returned, which is essential for declaring automatically named queues.

        * If `passive` is specified, the server state is not modified (a queue will not be
          declared), and the server only checks if the specified queue exists and returns its
          properties. If the queue does not exist, the server must raise a 404 NOT FOUND channel
          exception.
        * The server must create a default binding for a newly-created queue to the default
          exchange, which is an exchange of type 'direct'.
        * Queue names starting with 'amq.' are reserved for use by the server. If an attempt is
          made to declare a queue with such a name, and the `passive` flag is disabled, the server
          must raise a 403 ACCESS REFUSED connection exception.
        * The server must raise a 405 RESOURCE LOCKED channel exception if an attempt is made to
          access a queue declared as exclusive by another open connection.
        * The server must ignore the `auto_delete` flag if the queue already exists.

        RabbitMQ supports the following useful additional arguments:

        * x-max-length (int): maximum queue size
            * Queue length is a measure that takes into account ready messages, ignoring
              unacknowledged messages and message size. Messages will be dropped or dead-lettered
              from the front of the queue to make room for new messages once the limit is reached.

        :param str queue: queue name; leave blank to let the server generate a name automatically
        :param bool passive: do not create queue; client can use this to check whether a queue
            exists
        :param bool durable: mark as durable (remain active after server restarts)
        :param bool exclusive: mark as exclusive (can only be consumed from by this connection);
            implies `auto_delete`
        :param bool auto_delete: auto-delete queue when all consumers have finished using it
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :param dict arguments: exchange declare arguments
        :raise NotFound: if `passive` is enabled and the queue does not exist
        :raise AccessRefused: if an attempt is made to declare a queue with a reserved name
        :raise ResourceLocked: if an attempt is made to access an exclusive queue declared by
            another open connection
        :return: queue_declare_ok_t(queue, message_count, consumer_count), or None if `nowait`
        :rtype: queue_declare_ok_t or None
        """
        arguments = arguments or {}
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_bit(passive)
        args.write_bit(durable)
        args.write_bit(exclusive)
        args.write_bit(auto_delete)
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Queue.Declare, args))

        if not nowait:
            return self.wait(spec.Queue.DeclareOk)

    def _cb_queue_declare_ok(self, method):
        """Confirm a queue declare

        This method is called when the server responds to a `queue.declare`.

        :return: queue_declare_ok_t(queue, message_count, consumer_count), or None if `nowait`
        :rtype: queue_declare_ok_t or None
        """
        args = method.args
        return queue_declare_ok_t(args.read_shortstr(), args.read_long(), args.read_long())

    @synchronized_connection()
    def queue_delete(self, queue='', if_unused=False, if_empty=False, nowait=False):
        """Delete a queue

        This method deletes a queue. When a queue is deleted any pending messages are sent to a
        dead-letter queue if this is defined in the server configuration, and all consumers on the
        queue are cancelled.

        :param str queue: name of queue to delete, empty string refers to last declared queue on
            this channel
        :param bool if_unused: delete only if unused (has no consumers); raise a channel
            exception otherwise
        :param bool if_empty: delete only if empty; raise a channel exception otherwise
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :raise NotFound: if `queue` does not exist
        :raise PreconditionFailed: if `if_unused` or `if_empty` conditions are not met
        :return: number of messages deleted
        :rtype: int
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_bit(if_unused)
        args.write_bit(if_empty)
        args.write_bit(nowait)
        self._send_method(Method(spec.Queue.Delete, args))

        if not nowait:
            return self.wait(spec.Queue.DeleteOk)

    def _cb_queue_delete_ok(self, method):
        """Confirm deletion of a queue

        This method confirms the deletion of a queue.

        PARAMETERS:
            message_count: long

                number of messages purged

                Reports the number of messages purged.
        """
        args = method.args
        return args.read_long()

    @synchronized_connection()
    def queue_purge(self, queue='', nowait=False):
        """Purge a queue

        This method removes all messages from a queue. It does not cancel consumers. Purged messages
        are deleted without any formal "undo" mechanism.

        * On transacted channels the server MUST not purge messages that have already been sent
          to a client but not yet acknowledged.
        * If nowait is False, this method returns a message count.

        :param str queue: queue name to purge; leave blank to refer to last declared queue for
            this channel
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :return: message count (if nowait is False)
        :rtype: int or None
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_bit(nowait)
        self._send_method(Method(spec.Queue.Purge, args))

        if not nowait:
            return self.wait(spec.Queue.PurgeOk)

    def _cb_queue_purge_ok(self, method):
        """Confirms a queue purge

        This method confirms the purge of a queue.

        PARAMETERS:
            message_count: long

                number of messages purged

                Reports the number of messages purged.
        """
        args = method.args
        return args.read_long()

    @synchronized_connection()
    def basic_ack(self, delivery_tag, multiple=False):
        """Acknowledge one or more messages

        This method acknowledges one or more messages delivered via the Deliver or Get-Ok methods.
        The client can ask to confirm a single message or a set of messages up to and including a
        specific message.

        * The delivery tag is valid only within the same channel that the message was received.
        * Set `delivery_tag` to `0` and `multiple` to `True` to acknowledge all outstanding
          messages.
        * If the `delivery_tag` is invalid, the server must raise a channel exception.

        :param int delivery_tag: server-assigned delivery tag; 0 means "all messages received so
            far"
        :param bool multiple: if set, the `delivery_tag` is treated as "all messages up to and
            including"
        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(multiple)
        self._send_method(Method(spec.Basic.Ack, args))

    @synchronized_connection()
    def basic_cancel(self, consumer_tag, nowait=False):
        """End a queue consumer

        This method cancels a consumer. This does not affect already delivered messages, but it does
        mean the server will not send any more messages for that consumer. The client may receive an
        arbitrary number of messages in between sending the cancel method and receiving the
        cancel-ok reply.

        * If the queue no longer exists when the client sends a cancel command, or the consumer
          has been cancelled for other reasons, this command has no effect.

        :param str consumer_tag: consumer tag, valid only within the current connection and channel
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        """
        if self.connection is not None:
            self.no_ack_consumers.discard(consumer_tag)
            args = AMQPWriter()
            args.write_shortstr(consumer_tag)
            args.write_bit(nowait)
            self._send_method(Method(spec.Basic.Cancel, args))
            return self.wait(spec.Basic.CancelOk)

    def _cb_basic_cancel_notify(self, method):
        """Consumer cancelled by server.

        Most likely the queue was deleted.
        """
        args = method.args
        consumer_tag = args.read_shortstr()
        callback = self._on_cancel(consumer_tag)
        if callback:
            callback(consumer_tag)
        else:
            raise ConsumerCancelled(consumer_tag, spec.Basic.Cancel)

    def _cb_basic_cancel_ok(self, method):
        """Confirm a cancelled consumer

        This method confirms that the cancellation was completed.

        PARAMETERS: consumer_tag: shortstr

                consumer tag

                Identifier for the consumer, valid within the current connection.

                RULE:

                    The consumer tag is valid only within the channel from which the consumer was
                    created. I.e. a client
                    MUST NOT create a consumer in one channel and then use it in another.
        """
        args = method.args
        consumer_tag = args.read_shortstr()
        self._on_cancel(consumer_tag)

    def _on_cancel(self, consumer_tag):
        """
        :param consumer_tag:
        :return: callback, if any
        :rtype: callable or None
        """
        self.callbacks.pop(consumer_tag, None)
        return self.cancel_callbacks.pop(consumer_tag, None)

    @synchronized_connection()
    def basic_consume(self, queue='', consumer_tag='', no_local=False, no_ack=False,
                      exclusive=False, nowait=False, callback=None, arguments=None, on_cancel=None):
        """Start a queue consumer

        This method asks the server to start a "consumer", which is a transient request for messages
        from a specific queue. Consumers last as long as the channel they were created on, or until
        the client cancels them.

        * The `consumer_tag` is local to a connection, so two clients can use the same consumer
          tags. But on the same connection, the `consumer_tag` must be unique, or the server must
          raise a 530 NOT ALLOWED connection exception.
        * If `no_ack` is set, the server automatically acknowledges each message on behalf of the
          client.
        * If `exclusive` is set, the client asks for this consumer to have exclusive access to
          the queue. If the server cannot grant exclusive access to the queue because there are
          other consumers active, it must raise a 403 ACCESS REFUSED channel exception.
        * `callback` must be a `Callable(message)` which is called for each messaged delivered by
          the broker. If no callback is specified, messages are quietly discarded; `no_ack` should
          probably be set to True in that case.

        :param str queue: name of queue; if None, refers to last declared queue for this channel
        :param str consumer_tag: consumer tag, local to the connection
        :param bool no_local: if True: do not deliver own messages
        :param bool no_ack: server will not expect an ack for each message
        :param bool exclusive: request exclusive access
        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :param Callable callback: a callback callable(message) for each delivered message
        :param dict arguments: AMQP method arguments
        :param Callable on_cancel: a callback callable
        :return: consumer tag
        :rtype: str
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_shortstr(consumer_tag)
        args.write_bit(no_local)
        args.write_bit(no_ack)
        args.write_bit(exclusive)
        args.write_bit(nowait)
        args.write_table(arguments or {})
        self._send_method(Method(spec.Basic.Consume, args))

        if not nowait:
            consumer_tag = self.wait(spec.Basic.ConsumeOk)

        self.callbacks[consumer_tag] = callback

        if on_cancel:
            self.cancel_callbacks[consumer_tag] = on_cancel

        if no_ack:
            self.no_ack_consumers.add(consumer_tag)

        return consumer_tag

    def _cb_basic_consume_ok(self, method):
        """Confirm a new consumer

        The server provides the client with a consumer tag, which is used by the client for methods
        called on the consumer at a later stage.

        PARAMETERS:

            consumer_tag: shortstr

                Holds the consumer tag specified by the client or provided by the server.
        """
        args = method.args
        return args.read_shortstr()

    def _cb_basic_deliver(self, method):
        """Notify the client of a consumer message

        This method delivers a message to the client, via a consumer. In the asynchronous message
        delivery model, the client starts a consumer using the Consume method, then the server
        responds with Deliver methods as and when messages arrive for that consumer.

        This method can be called in a "classmethod" style static-context and is done so by
        :meth:`~amqpy.connection.Connection.drain_events()`.

        RULE:

            The server SHOULD track the number of times a message has been delivered to clients and
            when a message is redelivered a certain number of times - e.g. 5 times - without being
            acknowledged, the server SHOULD consider the message to be unprocessable (possibly
            causing client applications to abort), and move the message to a dead letter queue.

        PARAMETERS:

            consumer_tag: shortstr

                consumer tag

                Identifier for the consumer, valid within the current connection.

                RULE:

                    The consumer tag is valid only within the channel from which the consumer was
                    created. I.e. a client
                    MUST NOT create a consumer in one channel and then use it in another.

            delivery_tag: longlong

                server-assigned delivery tag

                The server-assigned and channel-specific delivery tag

                RULE:

                    The delivery tag is valid only within the channel from which the message was
                    received I.e. a
                    client MUST NOT receive a message on one channel and then acknowledge it on
                    another.

                RULE:

                    The server MUST NOT use a zero value for delivery tags Zero is reserved for
                    client use, meaning
                    "all messages so far received".

            redelivered: boolean

                message is being redelivered

                This indicates that the message has been previously delivered to this or another
                client.

            exchange: shortstr

                Specifies the name of the exchange that the message was originally published to.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the message was published.
        """
        args = method.args
        msg = method.content

        consumer_tag = args.read_shortstr()
        delivery_tag = args.read_longlong()
        redelivered = args.read_bit()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()

        msg.channel = self
        msg.delivery_info = {
            'consumer_tag': consumer_tag,
            'delivery_tag': delivery_tag,
            'redelivered': redelivered,
            'exchange': exchange,
            'routing_key': routing_key,
        }

        callback = self.callbacks.get(consumer_tag)
        if callback:
            callback(msg)
        else:
            raise Exception('No callback available for consumer tag: {}'.format(consumer_tag))

    @synchronized_connection()
    def basic_get(self, queue='', no_ack=False):
        """Directly get a message from the `queue`

        This method is non-blocking. If no messages are available on the queue, `None` is returned.

        :param str queue: queue name; leave blank to refer to last declared queue for the channel
        :param bool no_ack: if enabled, the server automatically acknowledges the message
        :return: message, or None if no messages are available on the queue
        :rtype: amqpy.message.Message or None
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_bit(no_ack)
        self._send_method(Method(spec.Basic.Get, args))
        return self.wait_any([spec.Basic.GetOk, spec.Basic.GetEmpty])

    def _cb_basic_get_empty(self, method):
        """Indicate no messages available

        This method tells the client that the queue has no messages
        available for the client.
        """
        args = method.args
        args.read_shortstr()

    def _cb_basic_get_ok(self, method):
        """Provide client with a message

        This method delivers a message to the client following a get method. A message delivered
        by 'get-ok' must be acknowledged unless the no-ack option was set in the get method.

        PARAMETERS:

            delivery_tag: longlong

                server-assigned delivery tag

                The server-assigned and channel-specific delivery tag

                RULE:

                    The delivery tag is valid only within the channel from which the message was
                    received I.e. a
                    client MUST NOT receive a message on one channel and then acknowledge it on
                    another.

                RULE:

                    The server MUST NOT use a zero value for delivery tags Zero is reserved for
                    client use, meaning
                    "all messages so far received".

            redelivered: boolean

                message is being redelivered

                This indicates that the message has been previously delivered to this or another
                client.

            exchange: shortstr

                Specifies the name of the exchange that the message was originally published to.
                If empty, the message
                was published to the default exchange.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the message was published.

            message_count: long

                number of messages pending

                This field reports the number of messages pending on the queue, excluding the
                message being delivered.
                Note that this figure is indicative, not reliable, and can change arbitrarily as
                messages are added to
                the queue and removed by other clients.
        """
        args = method.args
        msg = method.content

        delivery_tag = args.read_longlong()
        redelivered = args.read_bit()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        message_count = args.read_long()

        msg.channel = self
        msg.delivery_info = {
            'delivery_tag': delivery_tag,
            'redelivered': redelivered,
            'exchange': exchange,
            'routing_key': routing_key,
            'message_count': message_count
        }
        return msg

    def _basic_publish(self, msg, exchange='', routing_key='', mandatory=False, immediate=False):
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(mandatory)
        args.write_bit(immediate)

        self._send_method(Method(spec.Basic.Publish, args, msg))

    @synchronized_connection()
    def basic_publish(self, msg, exchange='', routing_key='', mandatory=False, immediate=False):
        """Publish a message

        This method publishes a message to a specific exchange. The message will be routed to
        queues as defined by the exchange configuration and distributed to any active consumers when
        the transaction, if any, is committed.

        If publisher confirms are enabled, this method will automatically wait to receive an "ack"
        from the server.

        .. note::

            Returned messages are sent back from the server and loaded into
            the `returned_messages` queue of the channel that sent them. In
            order to receive all returned messages, call `loop(0)` on the
            connection object before checking the channel's
            `returned_messages` queue.

        :param msg: message
        :param str exchange: exchange name, empty string means default exchange
        :param str routing_key: routing key
        :param bool mandatory: True: deliver to at least one queue, or return it; False: drop the
            unroutable message
        :param bool immediate: request immediate delivery
        :type msg: amqpy.Message
        """
        self._basic_publish(msg, exchange, routing_key, mandatory, immediate)
        if self.mode == self.CH_MODE_CONFIRM:
            self.wait(spec.Basic.Ack)

    @synchronized_connection()
    def basic_qos(self, prefetch_size=0, prefetch_count=0, a_global=False):
        """Specify quality of service

        This method requests a specific quality of service. The QoS can be specified for the
        current channel or for all channels on the connection. The particular properties and
        semantics of a qos method always depend on the content class semantics. Though the qos
        method could in principle apply to both peers, it is currently meaningful only for the
        server.

        * The client can request that messages be sent in advance so that when the client finishes
          processing a message, the following message is already held locally, rather than needing
          to be sent down the channel. Prefetching gives a performance improvement. This field
          specifies the prefetch window size in octets. The server will send a message in advance
          if it is equal to or smaller in size than the available prefetch size (and also falls
          into other prefetch limits). May be set to zero, meaning "no specific limit", although
          other prefetch limits may still apply. The prefetch-size is ignored if the no-ack option
          is set.
        * The server must ignore `prefetch_size` setting when the client is not processing any
          messages - i.e. the prefetch size does not limit the transfer of single messages to a
          client, only the sending in advance of more messages while the client still has one or
          more unacknowledged messages.
        * The `prefetch_count` specifies a prefetch window in terms of whole messages. This field
          may be used in combination with the prefetch-size field; a message will only be sent in
          advance if both prefetch windows (and those at the channel and connection level) allow
          it. The prefetch-count is ignored if the no-ack option is set.
        * The server may send less data in advance than allowed by the client's specified
          prefetch windows but it must not send more.

        :param int prefetch_size: prefetch window in octets
        :param int prefetch_count: prefetch window in messages
        :param bool a_global: apply to entire connection (default is for current channel only)
        """
        args = AMQPWriter()
        args.write_long(prefetch_size)
        args.write_short(prefetch_count)
        args.write_bit(a_global)
        self._send_method(Method(spec.Basic.Qos, args))
        return self.wait(spec.Basic.QosOk)

    def _cb_basic_qos_ok(self, method):
        """Confirm the requested qos

        This method tells the client that the requested QoS levels could be handled by the server.
        The requested QoS applies to all active consumers until a new QoS is defined.
        """
        pass

    @synchronized_connection()
    def basic_recover(self, requeue=False):
        """Redeliver unacknowledged messages

        This method asks the broker to redeliver all unacknowledged messages on a specified
        channel. Zero or more messages may be redelivered. This method is only allowed on
        non-transacted channels.

        * The server MUST set the redelivered flag on all messages that are resent.
        * The server MUST raise a channel exception if this is called on a transacted channel.

        :param bool requeue: if set, the server will attempt to requeue the message, potentially
            then delivering it to a different subscriber
        """
        args = AMQPWriter()
        args.write_bit(requeue)
        self._send_method(Method(spec.Basic.Recover, args))

    @synchronized_connection()
    def basic_recover_async(self, requeue=False):
        """Redeliver unacknowledged messages (async)

        This method asks the broker to redeliver all unacknowledged messages on a specified
        channel. Zero or more messages may be redelivered. This method is only allowed on
        non-transacted channels.

        * The server MUST set the redelivered flag on all messages that are resent.
        * The server MUST raise a channel exception if this is called on a transacted channel.

        :param bool requeue: if set, the server will attempt to requeue the message, potentially
            then delivering it to a different subscriber
        """
        args = AMQPWriter()
        args.write_bit(requeue)
        self._send_method(Method(spec.Basic.RecoverAsync, args))

    def _cb_basic_recover_ok(self, method):
        """In 0-9-1 the deprecated recover solicits a response
        """
        pass

    @synchronized_connection()
    def basic_reject(self, delivery_tag, requeue):
        """Reject an incoming message

        This method allows a client to reject a message. It can be used to interrupt and cancel
        large incoming messages,
        or return untreatable messages to their original queue.

        * The server SHOULD be capable of accepting and process the Reject method while sending
          message content with a Deliver or Get-Ok method I.e. the server should read and process
          incoming methods while sending output frames. To cancel a partially-send content, the
          server sends a content body frame of size 1 (i.e. with no data except the frame-end
          octet).
        * The server SHOULD interpret this method as meaning that the client is unable to process
          the message at this time.
        * A client MUST NOT use this method as a means of selecting messages to process A
          rejected message MAY be discarded or dead-lettered, not necessarily passed to another
          client.
        * The server MUST NOT deliver the message to the same client within the context of the
          current channel. The recommended strategy is to attempt to deliver the message to an
          alternative consumer, and if that is not possible, to move the message to a dead-letter
          queue. The server MAY use more sophisticated tracking to hold the message on the queue and
          redeliver it to the same client at a later stage.

        :param int delivery_tag: server-assigned channel-specific delivery tag
        :param bool requeue: True: requeue the message; False: discard the message
        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(requeue)
        self._send_method(Method(spec.Basic.Reject, args))

    def _cb_basic_return(self, method):
        """Return a failed message

        This method returns an undeliverable message that was published with the `immediate` flag
        set, or an unroutable message published with the `mandatory` flag set. The reply code and
        text provide information about the reason that the message was undeliverable.
        """
        args = method.args
        msg = method.content
        self.returned_messages.put(basic_return_t(
            args.read_short(),
            args.read_shortstr(),
            args.read_shortstr(),
            args.read_shortstr(),
            msg,
        ))

    @synchronized_connection()
    def tx_commit(self):
        """Commit the current transaction

        This method commits all messages published and acknowledged in the current transaction. A
        new transaction starts immediately after a commit.
        """
        self._send_method(Method(spec.Tx.Commit))
        return self.wait(spec.Tx.CommitOk)

    def _cb_tx_commit_ok(self, method):
        """Confirm a successful commit

        This method confirms to the client that the commit succeeded. Note that if a commit fails,
        the server raises a channel exception.
        """
        pass

    @synchronized_connection()
    def tx_rollback(self):
        """Abandon the current transaction

        This method abandons all messages published and acknowledged in the current transaction. A
        new transaction starts immediately after a rollback.
        """
        self._send_method(Method(spec.Tx.Rollback))
        return self.wait(spec.Tx.RollbackOk)

    def _cb_tx_rollback_ok(self, method):
        """Confirm a successful rollback

        This method confirms to the client that the rollback succeeded. Note that if an rollback
        fails, the server raises a channel exception.
        """
        pass

    @synchronized_connection()
    def tx_select(self):
        """Select standard transaction mode

        This method sets the channel to use standard transactions. The client must use this method
        at least once on a channel before using the Commit or Rollback methods.

        The channel must not be in publish acknowledge mode. If it is, the server raises a
        :exc:`PreconditionFailed` exception and closes the channel. Note that amqpy will
        automatically reopen the channel, at which point this method can be called again
        successfully.

        :raise PreconditionFailed: if the channel is in publish acknowledge mode
        """
        self._send_method(Method(spec.Tx.Select))
        #self.wait(spec.Tx.SelectOk)
        self.wait(spec.Tx.SelectOk)
        self.mode = self.CH_MODE_TX

    def _cb_tx_select_ok(self, method):
        """Confirm transaction mode

        This method confirms to the client that the channel was successfully set to use standard
        transactions.
        """
        pass

    @synchronized_connection()
    def confirm_select(self, nowait=False):
        """Enable publisher confirms for this channel (RabbitMQ extension)

        The channel must not be in transactional mode. If it is, the server raises a
        :exc:`PreconditionFailed` exception and closes the channel. Note that amqpy will
        automatically reopen the channel, at which point this method can be called again
        successfully.

        :param bool nowait: if set, the server will not respond to the method and the client
            should not wait for a reply
        :raise PreconditionFailed: if the channel is in transactional mode
        """
        args = AMQPWriter()
        args.write_bit(nowait)

        self._send_method(Method(spec.Confirm.Select, args))
        if not nowait:
            self.wait(spec.Confirm.SelectOk)
        self.mode = self.CH_MODE_CONFIRM

    def _cb_confirm_select_ok(self, method):
        """With this method, the broker confirms to the client that the channel is now using
        publisher confirms
        """
        pass

    def _cb_basic_ack_recv(self, method):
        """Callback for receiving a `spec.Basic.Ack`

        This will be called when the server acknowledges a published message (RabbitMQ extension).
        """
        # args = method.args
        # delivery_tag = args.read_longlong()
        # multiple = args.read_bit()

    METHOD_MAP = {
        spec.Channel.OpenOk: _cb_open_ok,
        spec.Channel.Flow: _cb_flow,
        spec.Channel.FlowOk: _cb_flow_ok,
        spec.Channel.Close: _cb_close,
        spec.Channel.CloseOk: _cb_close_ok,
        spec.Exchange.DeclareOk: _cb_exchange_declare_ok,
        spec.Exchange.DeleteOk: _cb_exchange_delete_ok,
        spec.Exchange.BindOk: _cb_exchange_bind_ok,
        spec.Exchange.UnbindOk: _cb_exchange_unbind_ok,
        spec.Queue.DeclareOk: _cb_queue_declare_ok,
        spec.Queue.BindOk: _cb_queue_bind_ok,
        spec.Queue.PurgeOk: _cb_queue_purge_ok,
        spec.Queue.DeleteOk: _cb_queue_delete_ok,
        spec.Queue.UnbindOk: _cb_queue_unbind_ok,
        spec.Basic.QosOk: _cb_basic_qos_ok,
        spec.Basic.ConsumeOk: _cb_basic_consume_ok,
        spec.Basic.Cancel: _cb_basic_cancel_notify,
        spec.Basic.CancelOk: _cb_basic_cancel_ok,
        spec.Basic.Return: _cb_basic_return,
        spec.Basic.Deliver: _cb_basic_deliver,
        spec.Basic.GetOk: _cb_basic_get_ok,
        spec.Basic.GetEmpty: _cb_basic_get_empty,
        spec.Basic.Ack: _cb_basic_ack_recv,
        spec.Basic.RecoverOk: _cb_basic_recover_ok,
        spec.Confirm.SelectOk: _cb_confirm_select_ok,
        spec.Tx.SelectOk: _cb_tx_select_ok,
        spec.Tx.CommitOk: _cb_tx_commit_ok,
        spec.Tx.RollbackOk: _cb_tx_rollback_ok,
    }
