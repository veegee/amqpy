"""AMQP Channels
"""
import logging
from collections import defaultdict
from queue import Queue

from .concurrency import synchronized
from .abstract_channel import AbstractChannel
from .exceptions import ChannelError, ConsumerCancelled, error_for_code
from .spec import basic_return_t, queue_declare_ok_t
from .serialization import AMQPWriter
from . import spec
from .spec import Method, method_t


__all__ = ['Channel']

log = logging.getLogger('amqpy')


class Channel(AbstractChannel):
    """Work with channels

    The channel class provides methods for a client to establish a virtual connection - a channel - to a server and for
    both peers to operate the virtual connection thereafter.
    """
   
    # TODO: finish cleaning up the documentation in this module
    # TODO: add :raise: and :return: directives in each docstring

    def __init__(self, connection, channel_id=None, auto_decode=True):
        """Create a channel bound to a connection and using the specified numeric channel_id, and open on the server

        The 'auto_decode' parameter (defaults to True), indicates whether the library should attempt to decode the body
        of Messages to a Unicode string if there's a 'content_encoding' property for the message.  If there's no
        'content_encoding' property, or the decode raises an Exception, the message body is left as plain bytes.
        """
        if channel_id:
            connection._claim_channel_id(channel_id)
        else:
            channel_id = connection._get_free_channel_id()

        super().__init__(connection, channel_id)

        self.is_open = False
        self.active = True  # flow control

        # returned messages that the server was unable to deliver
        self.returned_messages = Queue()

        # consumer callbacks dict[consumer_tag str: callable]
        self.callbacks = {}

        # consumer cancel callbacks dict dict[consumer_tag str: callable]
        self.cancel_callbacks = {}

        self.auto_decode = auto_decode  # auto decode received messages
        self.events = defaultdict(set)  # TODO: find out what this is used for

        # set of consumers that have opted for `no_ack` delivery (server will not expect an ack for delivered messages)
        self.no_ack_consumers = set()

        # set first time basic_publish_confirm is called and publisher confirms are enabled for this channel.
        self._confirm_selected = False
        if self.connection.confirm_publish:
            self.basic_publish = self.basic_publish_confirm

        self._x_open()

    def _do_close(self):
        """Tear down this object, after we've agreed to close with the server
        """
        log.debug('Closed channel #%d', self.channel_id)
        self.is_open = False
        channel_id, self.channel_id = self.channel_id, None
        connection, self.connection = self.connection, None
        if connection:
            connection.channels.pop(channel_id, None)
            connection._avail_channel_ids.append(channel_id)
        self.callbacks.clear()
        self.cancel_callbacks.clear()
        self.events.clear()
        self.no_ack_consumers.clear()

    def _do_revive(self):
        self.is_open = False
        self._x_open()

    @synchronized('lock')
    def close(self, reply_code=0, reply_text='', method_type=method_t(0, 0)):
        """Request a channel close

        This method indicates that the sender wants to close the channel. This may be due to internal conditions (e.g. a
        forced shut-down) or due to an error handling a specific method, i.e. an exception.  When a close is due to an
        exception, the sender provides the class and method id of the method which caused the exception.

        :param reply_code: the reply code
        :param reply_text: localized reply text
        :param method_type: if close is triggered by a failing method, this is the method that caused it
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
            return self.wait(allowed_methods=[spec.Channel.Close, spec.Channel.CloseOk])
        finally:
            self.connection = None

    def _close(self, method):
        """Respond to a channel close

        This method indicates that the sender (server) wants to close the channel. This may be due to internal
        conditions (e.g. a forced shut-down) or due to an error handling a specific method, i.e. an exception. When a
        close is due to an exception, the sender provides the class and method id of the method which caused the
        exception.
        """
        args = method.args
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        class_id = args.read_short()
        method_id = args.read_short()

        self._send_method(Method(spec.Channel.CloseOk))
        self._do_revive()

        method_type = method_t(class_id, method_id)
        raise error_for_code(reply_code, reply_text, method_type, ChannelError, self.channel_id)

    def _close_ok(self, method):
        """Confirm a channel close

        This method confirms a Channel.Close method and tells the recipient that it is safe to release resources for the
        channel and close the socket.
        """
        assert method
        self._do_close()

    @synchronized('lock')
    def flow(self, active):
        """Enable/disable flow from peer

        This method asks the peer to pause or restart the flow of content data. This is a simple flow-control mechanism
        that a peer can use to avoid oveflowing its queues or otherwise finding itself receiving more messages than it
        can process. Note that this method is not intended for window control.  The peer that receives a request to stop
        sending content should finish sending the current content, if any, and then wait until it receives a Flow
        restart method.

        :param active: True: peer starts sending content frames; False: peer stops sending content frames
        :type active: bool
        """
        args = AMQPWriter()
        args.write_bit(active)
        self._send_method(Method(spec.Channel.Flow, args))
        return self.wait(allowed_methods=[spec.Channel.FlowOk])

    def _flow(self, method):
        """Enable/disable flow from peer

        This method asks the peer to pause or restart the flow of content data. This is a simple flow-control mechanism
        that a peer can use to avoid oveflowing its queues or otherwise finding itself receiving more messages than it
        can process. Note that this method is not intended for window control.  The peer that receives a request to stop
        sending content should finish sending the current content, if any, and then wait until it receives a Flow
        restart method.
        """
        args = method.args
        self.active = args.read_bit()
        self._x_flow_ok(self.active)

    def _x_flow_ok(self, active):
        """Confirm a flow method

        Confirms to the peer that a flow command was received and processed.

        :param active: True: peer starts sending content frames; False: peer stops sending content frames
        :type active: bool
        """
        args = AMQPWriter()
        args.write_bit(active)
        self._send_method(Method(spec.Channel.FlowOk, args))

    def _flow_ok(self, method):
        """Confirm a flow method

        Confirms to the peer that a flow command was received and processed.
        """
        args = method.args
        return args.read_bit()

    def _x_open(self):
        """Open a channel for use

        This method opens a virtual connection (a channel).
        """
        if self.is_open:
            return

        args = AMQPWriter()
        args.write_shortstr('')  # out_of_band: deprecated
        self._send_method(Method(spec.Channel.Open, args))
        return self.wait(allowed_methods=[spec.Channel.OpenOk])

    def _open_ok(self, method):
        """Signal that the channel is ready

        The server sends this method to signal to the client that this channel is ready for use.
        """
        assert method
        self.is_open = True
        log.debug('Channel open')

    @synchronized('lock')
    def exchange_declare(self, exch_name, exch_type, passive=False, durable=False, auto_delete=False, nowait=False,
                         arguments=None):
        """Declare exchange, create if needed

        * This method creates an exchange if it does not already exist, and if the exchange exists, verifies that it
          is of the correct and expected class.
        * The server must ignore the `durable` field if the exchange already exists.
        * The server must ignore the `auto_delete` field if the exchange already exists.
        * If `nowait` is enabled and the server could not complete the method, it will raise a channel or connection
          exception.
        * `arguments` is ignored if passive is True.

        :param str exch_name: exchange name
        :param str exch_type: exchange type (direct, fanout, etc.)
        :param bool passive: do not create exchange; client can use this to check whether an exchange exists
        :param bool durable: mark exchange as durable (remain active after server restarts)
        :param bool auto_delete: auto-delete exchange when all queues have finished using it
        :param bool nowait: if set, the server will not respond to the method and the client should not wait for a reply
        :param dict arguments: exchange declare arguments
        :raise NotFound: if `passive` is enabled and the exchange does not exist
        :return: None
        """
        arguments = arguments or {}
        args = AMQPWriter()
        args.write_short(0)  # reserved-1
        args.write_shortstr(exch_name)  # exchange name
        args.write_shortstr(exch_type)  # exchange type
        args.write_bit(passive)  # passive
        args.write_bit(durable)  # durable
        args.write_bit(auto_delete)  # auto-delete
        args.write_bit(False)  # internal
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Exchange.Declare, args))

        if not nowait:
            return self.wait(allowed_methods=[spec.Exchange.DeclareOk])

    def _exchange_declare_ok(self, method):
        """Confirms an exchange declaration

        The server sends this method to confirm a Declare method and confirms the name of the exchange, essential for
        automatically-named exchanges.
        """
        pass

    @synchronized('lock')
    def exchange_delete(self, exch_name, if_unused=False, nowait=False):
        """Delete an exchange

        This method deletes an exchange.

        * If the exchange does not exist, the server must raise a channel exception. When an exchange is deleted,
          all queue bindings on the exchange are cancelled.
        * If `if_unused` is set, and the exchange has queue bindings, the server must raise a channel exception.

        :param str exch_name: exchange name
        :param bool if_unused: delete only if unused (has no queue bindings)
        :param bool nowait: if set, the server will not respond to the method and the client should not wait for a reply
        :raise NotFound: if exchange with `exch_name` does not exist
        :raise PreconditionFailed: if attempting to delete a queue with bindings and `if_unused` is set
        :return: None
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(exch_name)
        args.write_bit(if_unused)
        args.write_bit(nowait)
        self._send_method(Method(spec.Exchange.Delete, args))

        if not nowait:
            return self.wait(allowed_methods=[spec.Exchange.DeleteOk])

    def _exchange_delete_ok(self, method):
        """Confirm deletion of an exchange

        The server sends this method to confirm that the deletion of an exchange was successful.
        """
        pass

    @synchronized('lock')
    def exchange_bind(self, dest_exch, source_exch='', routing_key='', nowait=False, arguments=None):
        """Bind an exchange to an exchange

        * Both the `dest_exch` and `source_exch` must already exist. Blank exchange names mean the default exchange.
        * A server MUST allow and ignore duplicate bindings - that is, two or more bind methods for a specific
          exchanges, with identical arguments - without treating these as an error.
        * A server MUST allow cycles of exchange bindings to be created including allowing an exchange to be bound to
          itself.
        * A server MUST not deliver the same message more than once to a destination exchange, even if the topology of
          exchanges and bindings results in multiple (even infinite) routes to that exchange.

        :param str dest_exch: name of destination exchange to bind
        :param str source_exch: name of source exchange to bind
        :param str routing_key: routing key for the binding (note: not all exchanges use a routing key)
        :param bool nowait: if set, the server will not respond to the method and the client should not wait for a reply
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
            return self.wait(allowed_methods=[spec.Exchange.BindOk])

    @synchronized('lock')
    def exchange_unbind(self, dest_exch, source_exch='', routing_key='', nowait=False, arguments=None):
        """Unbind an exchange from an exchange

        * If the unbind fails, the server must raise a connection exception. The server must not attempt to unbind an
          exchange that does not exist from an exchange.
        * Blank exchange names mean the default exchange.

        :param str dest_exch: destination exchange name
        :param str source_exch: source exchange name
        :param str routing_key: routing key to unbind
        :param bool nowait: if set, the server will not respond to the method and the client should not wait for a reply
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
            return self.wait(allowed_methods=[spec.Exchange.UnbindOk])

    def _exchange_bind_ok(self, method):
        """Confirm bind successful

        The server sends this method to confirm that the bind was successful.
        """
        pass

    def _exchange_unbind_ok(self, method):
        """Confirm unbind successful

        The server sends this method to confirm that the unbind was successful.
        """
        pass

    @synchronized('lock')
    def queue_bind(self, queue_name, exch_name='', routing_key='', nowait=False, arguments=None):
        """Bind queue to an exchange

        This method binds a queue to an exchange. Until a queue is bound it will not receive any messages. In a classic
        messaging model, store-and-forward queues are bound to a dest exchange and subscription queues are bound to a
        dest_wild exchange.

        * The server must allow and ignore duplicate bindings without treating these as an error.
        * If a bind fails, the server must raise a connection exception.
        * The server must not allow a durable queue to bind to a transient exchange. If a client attempts this,
          the server must raise a channel exception.
        * The server should support at least 4 bindings per queue, and ideally, impose no limit except as defined by
          available resources.

        * If the client did not previously declare a queue, and the `queue_name` is empty, the server must raise a
          connection exception with reply code 530 (not allowed).
        * If `queue_name` does not exist, the server must raise a channel exception with reply code 404 (not found).
        * If `exch_name` does not exist, the server must raise a channel exception with reply code 404 (not found).

        :param str queue_name: name of queue to bind; blank refers to the last declared queue for this channel
        :param str exch_name: name of exchange to bind to
        :param str routing_key: routing key for the binding
        :param bool nowait: if set, the server will not respond to the method and the client should not wait for a reply
        :param dict arguments: binding arguments, specific to the exchange class
        """
        arguments = {} if arguments is None else arguments
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue_name)
        args.write_shortstr(exch_name)
        args.write_shortstr(routing_key)
        args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Queue.Bind, args))

        if not nowait:
            return self.wait(allowed_methods=[spec.Queue.BindOk])

    def _queue_bind_ok(self, method):
        """Confirm bind successful

        The server sends this method to confirm that the bind was successful.
        """
        pass

    @synchronized('lock')
    def queue_unbind(self, queue_name, exch_name, routing_key='', nowait=False, arguments=None):
        """Unbind a queue from an exchange

        This method unbinds a queue from an exchange.

        * If a unbind fails, the server MUST raise a connection exception.
        * The client must not attempt to unbind a queue that does not exist.
        * The client must not attempt to unbind a queue from an exchange that does not exist.

        :param str queue_name: name of queue to unbind, leave blank to refer to the last declared queue on this channel
        :param str exch_name: name of exchange to unbind, leave blank to refer to default exchange
        :param str routing_key: routing key of binding
        :param dict arguments: binding arguments, specific to the exchange class
        """
        arguments = {} if arguments is None else arguments
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue_name)
        args.write_shortstr(exch_name)
        args.write_shortstr(routing_key)
        # args.write_bit(nowait)
        args.write_table(arguments)
        self._send_method(Method(spec.Queue.Unbind, args))

        if not nowait:
            return self.wait(allowed_methods=[spec.Queue.UnbindOk])

    def _queue_unbind_ok(self, method):
        """Confirm unbind successful

        This method confirms that the unbind was successful.
        """
        pass

    @synchronized('lock')
    def queue_declare(self, queue='', passive=False, durable=False, exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        """Declare queue, create if needed

        This method creates or checks a queue.  When creating a new queue the client can specify various properties that
        control the durability of the queue and its contents, and the level of sharing for the queue.

        RULE:

            The server MUST create a default binding for a newly- created queue to the default exchange, which is an
            exchange of type 'direct'.

        RULE:

            The server SHOULD support a minimum of 256 queues per virtual host and ideally, impose no limit except as
            defined by available resources.

        PARAMETERS:

            queue: shortstr

                RULE:

                    The queue name MAY be empty, in which case the server MUST create a new queue with a unique
                    generated name and return this to the client in the Declare-Ok method.

                RULE:

                    Queue names starting with "amq." are reserved for predeclared and standardised server queues.  If
                    the queue name starts with "amq." and the passive option is False, the server MUST raise a
                    connection exception with reply code 403 (access refused).

            passive: boolean

                do not create queue

                If set, the server will not create the queue.  The client can use this to check whether a queue exists
                without modifying the server state.

                RULE:

                    If set, and the queue does not already exist, the server MUST respond with a reply code 404 (not
                    found) and raise a channel exception.

            durable: boolean

                request a durable queue

                If set when creating a new queue, the queue will be marked as durable.  Durable queues remain active
                when a server restarts. Non-durable queues (transient queues) are purged if/when a server restarts.
                Note that durable queues do not necessarily hold persistent messages, although it does not make sense to
                send persistent messages to a transient queue.

                RULE:

                    The server MUST recreate the durable queue after a restart.

                RULE:

                    The server MUST support both durable and transient queues.

                RULE:

                    The server MUST ignore the durable field if the queue already exists.

            exclusive: boolean

                request an exclusive queue

                Exclusive queues may only be consumed from by the current connection. Setting the 'exclusive' flag
                always implies 'auto-delete'.

                RULE:

                    The server MUST support both exclusive (private) and non-exclusive (shared) queues.

                RULE:

                    The server MUST raise a channel exception if 'exclusive' is specified and the queue already exists
                    and is owned by a different connection.

            auto_delete: boolean

                auto-delete queue when unused

                If set, the queue is deleted when all consumers have finished using it. Last consumer can be cancelled
                either explicitly or because its channel is closed. If there was no consumer ever on the queue, it won't
                be deleted.

                RULE:

                    The server SHOULD allow for a reasonable delay between the point when it determines that a queue is
                    not being used (or no longer used), and the point when it deletes the queue.  At the least it must
                    allow a client to create a queue and then create a consumer to read from it, with a small but
                    non-zero delay between these two actions.  The server should equally allow for clients that may be
                    disconnected prematurely, and wish to re- consume from the same queue without losing messages.  We
                    would recommend a configurable timeout, with a suitable default value being one minute.

                RULE:

                    The server MUST ignore the auto-delete field if the queue already exists.

            nowait: boolean

                do not send a reply method

                If set, the server will not respond to the method. The client should not wait for a reply method.  If
                the server could not complete the method it will raise a channel or connection exception.

            arguments: table

                arguments for declaration

                A set of arguments for the declaration. The syntax and semantics of these arguments depends on the
                server implementation.  This field is ignored if passive is True.

        Returns a tuple containing 3 items: the name of the queue (essential for automatically-named queues) message
        count consumer count
        """
        arguments = {} if arguments is None else arguments
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
            return self.wait(allowed_methods=[spec.Queue.DeclareOk])

    def _queue_declare_ok(self, method):
        """Confirms a queue definition

        This method confirms a Declare method and confirms the name of the queue, essential for automatically-named
        queues.

        PARAMETERS:

            queue: shortstr

                Reports the name of the queue. If the server generated a queue name, this field contains that name.

            message_count: long

                number of messages in queue

                Reports the number of messages in the queue, which will be zero for newly-created queues.

            consumer_count: long

                number of consumers

                Reports the number of active consumers for the queue. Note that consumers can suspend activity
                (Channel.Flow) in which case they do not appear in this count.
        """
        args = method.args
        return queue_declare_ok_t(args.read_shortstr(), args.read_long(), args.read_long())

    @synchronized('lock')
    def queue_delete(self, queue_name='', if_unused=False, if_empty=False, nowait=False):
        """Delete a queue

        This method deletes a queue. When a queue is deleted any pending messages are sent to a dead-letter queue if
        this is defined in the server configuration, and all consumers on the queue are cancelled.

        :param str queue_name: name of queue to delete, empty string refers to last declared queue on this channel
        :param bool if_unused: delete only if unused (has no consumers); raise a channel exception otherwise
        :param bool if_empty: delete only if empty; raise a channel exception otherwise
        :param bool nowait: if set, the server will not respond to the method and the client should not wait for a reply
        :raise NotFound: if `queue_name` does not exist
        :raise PreconditionFailed: if `if_unused` or `if_empty` conditions are not met
        :return: number of messages deleted
        :rtype: int
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue_name)
        args.write_bit(if_unused)
        args.write_bit(if_empty)
        args.write_bit(nowait)
        self._send_method(Method(spec.Queue.Delete, args))

        if not nowait:
            return self.wait(allowed_methods=[spec.Queue.DeleteOk])

    def _queue_delete_ok(self, method):
        """Confirm deletion of a queue

        This method confirms the deletion of a queue.

        PARAMETERS:
            message_count: long

                number of messages purged

                Reports the number of messages purged.
        """
        args = method.args
        return args.read_long()

    @synchronized('lock')
    def queue_purge(self, queue='', nowait=False):
        """Purge a queue

        This method removes all messages from a queue.  It does not cancel consumers.  Purged messages are deleted
        without any formal "undo" mechanism.

        RULE:

            A call to purge MUST result in an empty queue.

        RULE:

            On transacted channels the server MUST not purge messages that have already been sent to a client but not
            yet acknowledged.

        RULE:

            The server MAY implement a purge queue or log that allows system administrators to recover
            accidentally-purged messages.  The server SHOULD NOT keep purged messages in the same storage spaces as the
            live messages since the volumes of purged messages may get very large.

        PARAMETERS:

            queue: shortstr

                Specifies the name of the queue to purge.  If the queue name is empty, refers to the current queue for
                the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue, and the queue name in this method is empty, the
                    server MUST raise a connection exception with reply code 530 (not allowed).

                RULE:

                    The queue must exist. Attempting to purge a non- existing queue causes a channel exception.

            nowait: boolean

                do not send a reply method

                If set, the server will not respond to the method. The client should not wait for a reply method.  If
                the server could not complete the method it will raise a channel or connection exception.

        if nowait is False, returns a message_count
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_bit(nowait)
        self._send_method(Method(spec.Queue.Purge, args))

        if not nowait:
            return self.wait(allowed_methods=[spec.Queue.PurgeOk])

    def _queue_purge_ok(self, method):
        """Confirms a queue purge

        This method confirms the purge of a queue.

        PARAMETERS:
            message_count: long

                number of messages purged

                Reports the number of messages purged.
        """
        args = method.args
        return args.read_long()

    @synchronized('lock')
    def basic_ack(self, delivery_tag, multiple=False):
        """Acknowledge one or more messages

        This method acknowledges one or more messages delivered via the Deliver or Get-Ok methods.  The client can ask
        to confirm a single message or a set of messages up to and including a specific message.

        PARAMETERS:

            delivery_tag: longlong

                server-assigned delivery tag

                The server-assigned and channel-specific delivery tag

                RULE:

                    The delivery tag is valid only within the channel from which the message was received.  I.e. a
                    client MUST NOT receive a message on one channel and then acknowledge it on another.

                RULE:

                    The server MUST NOT use a zero value for delivery tags.  Zero is reserved for client use, meaning
                    "all messages so far received".

            multiple: boolean

                acknowledge multiple messages

                If set to True, the delivery tag is treated as "up to and including", so that the client can acknowledge
                multiple messages with a single method.  If set to False, the delivery tag refers to a single message.
                If the multiple field is True, and the delivery tag is zero, tells the server to acknowledge all
                outstanding mesages.

                RULE:

                    The server MUST validate that a non-zero delivery- tag refers to an delivered message, and raise a
                    channel exception if this is not the case.
        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(multiple)
        self._send_method(Method(spec.Basic.Ack, args))

    @synchronized('lock')
    def basic_cancel(self, consumer_tag, nowait=False):
        """End a queue consumer

        This method cancels a consumer. This does not affect already delivered messages, but it does mean the server
        will not send any more messages for that consumer.  The client may receive an abitrary number of messages in
        between sending the cancel method and receiving the cancel-ok reply.

        RULE:

            If the queue no longer exists when the client sends a cancel command, or the consumer has been cancelled for
            other reasons, this command has no effect.

        PARAMETERS:

            consumer_tag: shortstr

                consumer tag

                Identifier for the consumer, valid within the current connection.

                RULE:

                    The consumer tag is valid only within the channel from which the consumer was created. I.e. a client
                    MUST NOT create a consumer in one channel and then use it in another.

            nowait: boolean

                do not send a reply method

                If set, the server will not respond to the method. The client should not wait for a reply method.  If
                the server could not complete the method it will raise a channel or connection exception.
        """
        if self.connection is not None:
            self.no_ack_consumers.discard(consumer_tag)
            args = AMQPWriter()
            args.write_shortstr(consumer_tag)
            args.write_bit(nowait)
            self._send_method(Method(spec.Basic.Cancel, args))
            return self.wait(allowed_methods=[spec.Basic.CancelOk])

    def _basic_cancel_notify(self, method):
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

    def _basic_cancel_ok(self, method):
        """Confirm a cancelled consumer

        This method confirms that the cancellation was completed.

        PARAMETERS: consumer_tag: shortstr

                consumer tag

                Identifier for the consumer, valid within the current connection.

                RULE:

                    The consumer tag is valid only within the channel from which the consumer was created. I.e. a client
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

    @synchronized('lock')
    def basic_consume(self, queue='', consumer_tag='', no_local=False, no_ack=False, exclusive=False, nowait=False,
                      callback=None, arguments=None, on_cancel=None):
        """Start a queue consumer

        This method asks the server to start a "consumer", which is a transient request for messages from a specific
        queue. Consumers last as long as the channel they were created on, or until the client cancels them.

        :param queue: name of queue; if None, refers to last declared queue for this channel
        :param consumer_tag: consumer tag, local to the connection
        :param no_local: if True: do not deliver own messages
        :param no_ack: server will not expect an ack for each message
        :param exclusive: request exclusive access
        :param nowait: tell server not to send a
        :param callback: a callback callable(message) for each delivered message
        :param arguments: AMQP method arguments
        :param on_cancel: a callback callable

        PARAMETERS:

            queue: shortstr

                Specifies the name of the queue to consume from.  If the queue name is null, refers to the current queue
                for the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue, and the queue name in this method is empty, the
                    server MUST raise a connection exception with reply code 530 (not allowed).

            consumer_tag: shortstr

                Specifies the identifier for the consumer. The consumer tag is local to a connection, so two clients can
                use the same consumer tags. If this field is empty the server will generate a unique tag.

                RULE:

                    The tag MUST NOT refer to an existing consumer. If the client attempts to create two consumers with
                    the same non-empty tag the server MUST raise a connection exception with reply code 530 (not
                    allowed).

            no_local: boolean

                do not deliver own messages

                If the no-local field is set the server will not send messages to the client that published them.

            no_ack: boolean

                no acknowledgement needed

                If this field is set the server does not expect acknowledgments for messages.  That is, when a message
                is delivered to the client the server automatically and silently acknowledges it on behalf of the
                client.  This functionality increases performance but at the cost of reliability.  Messages can get lost
                if a client dies before it can deliver them to the application.

            exclusive: boolean

                request exclusive access

                Request exclusive consumer access, meaning only this consumer can access the queue.

                RULE:

                    If the server cannot grant exclusive access to the queue when asked, - because there are other
                    consumers active - it MUST raise a channel exception with return code 403 (access refused).

            nowait: boolean

                do not send a reply method

                If set, the server will not respond to the method. The client should not wait for a reply method.  If
                the server could not complete the method it will raise a channel or connection exception.

            callback: Python callable

                function/method called with each delivered message

                For each message delivered by the broker, the callable will be called with a Message object as the
                single argument.  If no callable is specified, messages are quietly discarded, no_ack should probably be
                set to True in that case.
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
            consumer_tag = self.wait(allowed_methods=[spec.Basic.ConsumeOk])

        self.callbacks[consumer_tag] = callback

        if on_cancel:
            self.cancel_callbacks[consumer_tag] = on_cancel

        if no_ack:
            self.no_ack_consumers.add(consumer_tag)

        return consumer_tag

    def _basic_consume_ok(self, method):
        """Confirm a new consumer

        The server provides the client with a consumer tag, which is used by the client for methods called on the
        consumer at a later stage.

        PARAMETERS:

            consumer_tag: shortstr

                Holds the consumer tag specified by the client or provided by the server.
        """
        args = method.args
        return args.read_shortstr()

    def _basic_deliver(self, method):
        """Notify the client of a consumer message

        This method delivers a message to the client, via a consumer. In the asynchronous message delivery model, the
        client starts a consumer using the Consume method, then the server responds with Deliver methods as and when
        messages arrive for that consumer.

        This method can be called in a "classmethod" style static-context and is done so by
        :meth:`~amqpy.connection.Connection.drain_events()`.

        RULE:

            The server SHOULD track the number of times a message has been delivered to clients and when a message is
            redelivered a certain number of times - e.g. 5 times - without being acknowledged, the server SHOULD
            consider the message to be unprocessable (possibly causing client applications to abort), and move the
            message to a dead letter queue.

        PARAMETERS:

            consumer_tag: shortstr

                consumer tag

                Identifier for the consumer, valid within the current connection.

                RULE:

                    The consumer tag is valid only within the channel from which the consumer was created. I.e. a client
                    MUST NOT create a consumer in one channel and then use it in another.

            delivery_tag: longlong

                server-assigned delivery tag

                The server-assigned and channel-specific delivery tag

                RULE:

                    The delivery tag is valid only within the channel from which the message was received.  I.e. a
                    client MUST NOT receive a message on one channel and then acknowledge it on another.

                RULE:

                    The server MUST NOT use a zero value for delivery tags.  Zero is reserved for client use, meaning
                    "all messages so far received".

            redelivered: boolean

                message is being redelivered

                This indicates that the message has been previously delivered to this or another client.

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

    @synchronized('lock')
    def basic_get(self, queue='', no_ack=False):
        """Direct access to a queue

        This method provides a direct access to the messages in a queue using a synchronous dialogue that is designed
        for specific types of application where synchronous functionality is more important than performance.

        PARAMETERS:

            queue: shortstr

                Specifies the name of the queue to consume from.  If the queue name is null, refers to the current queue
                for the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue, and the queue name in this method is empty, the
                    server MUST raise a connection exception with reply code 530 (not allowed).

            no_ack: boolean

                no acknowledgement needed

                If this field is set the server does not expect acknowledgments for messages.  That is, when a message
                is delivered to the client the server automatically and silently acknowledges it on behalf of the
                client.  This functionality increases performance but at the cost of reliability.  Messages can get lost
                if a client dies before it can deliver them to the application.

            Non-blocking, returns a message object, or None.
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(queue)
        args.write_bit(no_ack)
        self._send_method(Method(spec.Basic.Get, args))
        return self.wait(allowed_methods=[spec.Basic.GetOk, spec.Basic.GetEmpty])

    def _basic_get_empty(self, method):
        """Indicate no messages available

        This method tells the client that the queue has no messages
        available for the client.
        """
        args = method.args
        args.read_shortstr()

    def _basic_get_ok(self, method):
        """Provide client with a message

        This method delivers a message to the client following a get method.  A message delivered by 'get-ok' must be
        acknowledged unless the no-ack option was set in the get method.

        PARAMETERS:

            delivery_tag: longlong

                server-assigned delivery tag

                The server-assigned and channel-specific delivery tag

                RULE:

                    The delivery tag is valid only within the channel from which the message was received.  I.e. a
                    client MUST NOT receive a message on one channel and then acknowledge it on another.

                RULE:

                    The server MUST NOT use a zero value for delivery tags.  Zero is reserved for client use, meaning
                    "all messages so far received".

            redelivered: boolean

                message is being redelivered

                This indicates that the message has been previously delivered to this or another client.

            exchange: shortstr

                Specifies the name of the exchange that the message was originally published to.  If empty, the message
                was published to the default exchange.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the message was published.

            message_count: long

                number of messages pending

                This field reports the number of messages pending on the queue, excluding the message being delivered.
                Note that this figure is indicative, not reliable, and can change arbitrarily as messages are added to
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

    @synchronized('lock')
    def basic_publish(self, msg, exchange='', routing_key='', mandatory=False, immediate=False):
        """Publish a message

        This method publishes a message to a specific exchange. The message will be routed to queues as defined by the
        exchange configuration and distributed to any active consumers when the transaction, if any, is committed.

        :param msg: message
        :param exchange: exchange name, empty string means default exchange
        :param routing_key: routing key
        :param mandatory: True: deliver to at least one queue or return the message; False: drop the unroutable message
        :param immediate: request immediate delivery
        :type msg: amqpy.Message
        :type exchange: str
        :type mandatory: bool
        :type immediate: bool
        """
        args = AMQPWriter()
        args.write_short(0)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(mandatory)
        args.write_bit(immediate)

        self._send_method(Method(spec.Basic.Publish, args, msg))

    @synchronized('lock')
    def basic_publish_confirm(self, *args, **kwargs):
        if not self._confirm_selected:
            self._confirm_selected = True
            self.confirm_select()
        ret = self.basic_publish(*args, **kwargs)
        self.wait([spec.Basic.Ack])
        return ret

    @synchronized('lock')
    def basic_qos(self, prefetch_size, prefetch_count, a_global):
        """Specify quality of service

        This method requests a specific quality of service.  The QoS can be specified for the current channel or for all
        channels on the connection.  The particular properties and semantics of a qos method always depend on the
        content class semantics. Though the qos method could in principle apply to both peers, it is currently
        meaningful only for the server.

        PARAMETERS:

            prefetch_size: long

                prefetch window in octets

                The client can request that messages be sent in advance so that when the client finishes processing a
                message, the following message is already held locally, rather than needing to be sent down the channel.
                Prefetching gives a performance improvement. This field specifies the prefetch window size in octets.
                The server will send a message in advance if it is equal to or smaller in size than the available
                prefetch size (and also falls into other prefetch limits). May be set to zero, meaning "no specific
                limit", although other prefetch limits may still apply. The prefetch-size is ignored if the no-ack
                option is set.

                RULE:

                    The server MUST ignore this setting when the client is not processing any messages - i.e. the
                    prefetch size does not limit the transfer of single messages to a client, only the sending in
                    advance of more messages while the client still has one or more unacknowledged messages.

            prefetch_count: short

                prefetch window in messages

                Specifies a prefetch window in terms of whole messages.  This field may be used in combination with the
                prefetch-size field; a message will only be sent in advance if both prefetch windows (and those at the
                channel and connection level) allow it. The prefetch- count is ignored if the no-ack option is set.

                RULE:

                    The server MAY send less data in advance than allowed by the client's specified prefetch windows but
                    it MUST NOT send more.

            a_global: boolean

                apply to entire connection

                By default the QoS settings apply to the current channel only.  If this field is set, they are applied
                to the entire connection.
        """
        args = AMQPWriter()
        args.write_long(prefetch_size)
        args.write_short(prefetch_count)
        args.write_bit(a_global)
        self._send_method(Method(spec.Basic.Qos, args))
        return self.wait(allowed_methods=[spec.Basic.QosOk])

    def _basic_qos_ok(self, method):
        """Confirm the requested qos

        This method tells the client that the requested QoS levels could be handled by the server.  The requested QoS
        applies to all active consumers until a new QoS is defined.
        """
        pass

    @synchronized('lock')
    def basic_recover(self, requeue=False):
        """Redeliver unacknowledged messages

        This method asks the broker to redeliver all unacknowledged messages on a specified channel. Zero or more
        messages may be redelivered.  This method is only allowed on non-transacted channels.

        RULE:

            The server MUST set the redelivered flag on all messages that are resent.

        RULE:

            The server MUST raise a channel exception if this is called on a transacted channel.

        PARAMETERS:

            requeue: boolean

                requeue the message

                If this field is False, the message will be redelivered to the original recipient.  If this field is
                True, the server will attempt to requeue the message, potentially then delivering it to an alternative
                subscriber.
        """
        args = AMQPWriter()
        args.write_bit(requeue)
        self._send_method(Method(spec.Basic.Recover, args))

    @synchronized('lock')
    def basic_recover_async(self, requeue=False):
        args = AMQPWriter()
        args.write_bit(requeue)
        self._send_method(Method(spec.Basic.RecoverAsync, args))

    def _basic_recover_ok(self, method):
        """In 0-9-1 the deprecated recover solicits a response
        """
        pass

    @synchronized('lock')
    def basic_reject(self, delivery_tag, requeue):
        """Reject an incoming message

        This method allows a client to reject a message.  It can be used to interrupt and cancel large incoming
        messages, or return untreatable messages to their original queue.

        RULE:

            The server SHOULD be capable of accepting and process the Reject method while sending message content with a
            Deliver or Get-Ok method.  I.e. the server should read and process incoming methods while sending output
            frames.  To cancel a partially-send content, the server sends a content body frame of size 1 (i.e. with no
            data except the frame-end octet).

        RULE:

            The server SHOULD interpret this method as meaning that the client is unable to process the message at this
            time.

        RULE:

            A client MUST NOT use this method as a means of selecting messages to process.  A rejected message MAY be
            discarded or dead-lettered, not necessarily passed to another client.

        PARAMETERS:

            delivery_tag: longlong

                server-assigned delivery tag

                The server-assigned and channel-specific delivery tag

                RULE:

                    The delivery tag is valid only within the channel from which the message was received.  I.e. a
                    client MUST NOT receive a message on one channel and then acknowledge it on another.

                RULE:

                    The server MUST NOT use a zero value for delivery tags.  Zero is reserved for client use, meaning
                    "all messages so far received".

            requeue: boolean

                requeue the message

                If this field is False, the message will be discarded. If this field is True, the server will attempt to
                requeue the message.

                RULE:

                    The server MUST NOT deliver the message to the same client within the context of the current
                    channel.  The recommended strategy is to attempt to deliver the message to an alternative consumer,
                    and if that is not possible, to move the message to a dead-letter queue.  The server MAY use more
                    sophisticated tracking to hold the message on the queue and redeliver it to the same client at a
                    later stage.
        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(requeue)
        self._send_method(Method(spec.Basic.Reject, args))

    def _basic_return(self, method):
        """Return a failed message

        This method returns an undeliverable message that was published with the `immediate` flag set, or an unroutable
        message published with the `mandatory` flag set. The reply code and text provide information about the reason
        that the message was undeliverable.
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

    @synchronized('lock')
    def tx_commit(self):
        """Commit the current transaction

        This method commits all messages published and acknowledged in the current transaction. A new transaction
        starts immediately after a commit.
        """
        self._send_method(Method(spec.Tx.Commit))
        return self.wait(allowed_methods=[spec.Tx.CommitOk])

    def _tx_commit_ok(self, method):
        """Confirm a successful commit

        This method confirms to the client that the commit succeeded. Note that if a commit fails, the server raises a
        channel exception.
        """
        pass

    @synchronized('lock')
    def tx_rollback(self):
        """Abandon the current transaction

        This method abandons all messages published and acknowledged in the current transaction.  A new transaction
        starts immediately after a rollback.

        """
        self._send_method(Method(spec.Tx.Rollback))
        return self.wait(allowed_methods=[spec.Tx.RollbackOk])

    def _tx_rollback_ok(self, method):
        """Confirm a successful rollback

        This method confirms to the client that the rollback succeeded. Note that if an rollback fails, the server
        raises a channel exception.
        """
        pass

    @synchronized('lock')
    def tx_select(self):
        """Select standard transaction mode

        This method sets the channel to use standard transactions. The client must use this method at least once on a
        channel before using the Commit or Rollback methods.
        """
        self._send_method(Method(spec.Tx.Select))
        return self.wait(allowed_methods=[spec.Tx.SelectOk])

    def _tx_select_ok(self, method):
        """Confirm transaction mode

        This method confirms to the client that the channel was successfully set to use standard transactions.
        """
        pass

    @synchronized('lock')
    def confirm_select(self, nowait=False):
        """Enable publisher confirms for this channel (an RabbitMQ extension)

        Can now be used if the channel is in transactional mode.

        :param nowait: If set, the server will not respond to the method. The client should not wait for a reply method.
            If the server could not complete the method it will raise a channel or connection exception.
        """
        args = AMQPWriter()
        args.write_bit(nowait)

        self._send_method(Method(spec.Confirm.Select, args))
        if not nowait:
            self.wait(allowed_methods=[spec.Confirm.SelectOk])

    def _confirm_select_ok(self, method):
        """With this method, the broker confirms to the client that the channel is now using publisher confirms
        """
        pass

    def _basic_ack_recv(self, method):
        """Callback for receiving a `spec.Basic.Ack`

        This will be called when the server acknowledges a published message (RabbitMQ extension).
        """
        args = method.args
        delivery_tag = args.read_longlong()
        multiple = args.read_bit()
        for callback in self.events['basic_ack']:
            callback(delivery_tag, multiple)

    METHOD_MAP = {
        spec.Channel.OpenOk: _open_ok,
        spec.Channel.Flow: _flow,
        spec.Channel.FlowOk: _flow_ok,
        spec.Channel.Close: _close,
        spec.Channel.CloseOk: _close_ok,
        spec.Exchange.DeclareOk: _exchange_declare_ok,
        spec.Exchange.DeleteOk: _exchange_delete_ok,
        spec.Exchange.BindOk: _exchange_bind_ok,
        spec.Exchange.UnbindOk: _exchange_unbind_ok,
        spec.Queue.DeclareOk: _queue_declare_ok,
        spec.Queue.BindOk: _queue_bind_ok,
        spec.Queue.PurgeOk: _queue_purge_ok,
        spec.Queue.DeleteOk: _queue_delete_ok,
        spec.Queue.UnbindOk: _queue_unbind_ok,
        spec.Basic.QosOk: _basic_qos_ok,
        spec.Basic.ConsumeOk: _basic_consume_ok,
        spec.Basic.Cancel: _basic_cancel_notify,
        spec.Basic.CancelOk: _basic_cancel_ok,
        spec.Basic.Return: _basic_return,
        spec.Basic.Deliver: _basic_deliver,
        spec.Basic.GetOk: _basic_get_ok,
        spec.Basic.GetEmpty: _basic_get_empty,
        spec.Basic.Ack: _basic_ack_recv,
        spec.Basic.RecoverOk: _basic_recover_ok,
        spec.Confirm.SelectOk: _confirm_select_ok,
        spec.Tx.SelectOk: _tx_select_ok,
        spec.Tx.CommitOk: _tx_commit_ok,
        spec.Tx.RollbackOk: _tx_rollback_ok,
    }

    IMMEDIATE_METHODS = [
        spec.Basic.Return,
    ]
