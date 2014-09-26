"""AMQP Connections
"""
import logging
import socket
from array import array

from .method_framing import MethodReader, MethodWriter
from .serialization import AMQPWriter
from . import __version__
from .abstract_channel import AbstractChannel
from .channel import Channel
from .exceptions import ChannelError, ResourceError, ConnectionError, error_for_code, AMQPNotImplementedError
from .transport import create_transport
from . import spec
from .spec import Method


__all__ = ['Connection']

# client property info that gets sent to the server on connection startup
LIBRARY_PROPERTIES = {
    'product': 'amqpy',
    'product_version': __version__,
    'capabilities': {},
}

log = logging.getLogger('amqpy')


class Connection(AbstractChannel):
    """The connection class provides methods for a client to establish a network connection to a server, and for both
    peers to operate the connection thereafter
    """
    Channel = Channel

    # : Final heartbeat interval value (in float seconds) after negotiation
    heartbeat = None

    # : Original heartbeat interval value proposed by client.
    client_heartbeat = None

    # : Original heartbeat interval proposed by server.
    server_heartbeat = None

    # : Time of last heartbeat sent (in monotonic time, if available).
    last_heartbeat_sent = 0

    # : Time of last heartbeat received (in monotonic time, if available).
    last_heartbeat_received = 0

    # : Number of bytes sent to socket at the last heartbeat check.
    prev_sent = None

    # : Number of bytes received from socket at the last heartbeat check.
    prev_recv = None

    def __init__(self, host='localhost', port=5672, userid='guest', password='guest', login_method='AMQPLAIN',
                 virtual_host='/', locale='en_US', client_properties=None, ssl=None, connect_timeout=None,
                 channel_max=None, frame_max=None, heartbeat=0, on_blocked=None, on_unblocked=None,
                 confirm_publish=False, **kwargs):
        """Create a connection to the specified host

        If you are using SSL, make sure the correct port number is specified (usually 5671), as the default of 5672 is
        for non-SSL connections.

        :param str host: host
        :param int port: port
        :param str userid: username
        :param str password: password
        :param login_response: if not specified, one is built up for you from `userid` and `password` if present
        :param str virtual_host: virtual host
        :param str locale: locale
        :param dict client_properties: dict of client properties
        :param ssl: dict of SSL options passed to :func:`ssl.wrap_socket()`, None to disable SSL
        :param float connect_timeout: connect timeout
        :param int channel_max: channel max
        :param int frame_max: frame max
        :param float heartbeat: heartbeat interval in seconds, 0 disables heartbeat
        :param callable on_open: callback on connection open
        :param callable on_blocked: callback on connection blocked
        :param callable on_unblocked: callback on connection unblocked
        :param bool confirm_publish: confirm publish
        :type ssl: dict or None
        """
        channel_max = channel_max or 65535
        frame_max = frame_max or 131072

        # create login "response" to send to server
        login_response = AMQPWriter()
        login_response.write_table({'LOGIN': userid, 'PASSWORD': password})
        login_response = login_response.getvalue()[4:]  # skip the length
        # at the beginning

        d = dict(LIBRARY_PROPERTIES, **client_properties or {})
        self._method_override = {spec.Basic.Return: self._dispatch_basic_return}

        self.channels = {}
        # the connection object itself is treated as channel 0
        super(Connection, self).__init__(self, 0)

        self.transport = None

        # properties set in the Tune method
        self.channel_max = channel_max
        self.frame_max = frame_max
        self.client_heartbeat = heartbeat

        self.confirm_publish = confirm_publish

        # callbacks
        self.on_blocked = on_blocked
        self.on_unblocked = on_unblocked

        self._avail_channel_ids = array('H', range(self.channel_max, 0, -1))

        # properties set in the Start method
        self.version_major = 0
        self.version_minor = 0
        self.server_properties = {}
        self.mechanisms = []
        self.locales = []

        # let the transport.py module setup the actual socket connection to the broker
        self.transport = create_transport(host, port, connect_timeout, ssl)

        self.method_reader = MethodReader(self.transport)
        self.method_writer = MethodWriter(self.transport, self.frame_max)

        # wait for server to send the START method
        self.wait(allowed_methods=[spec.Connection.Start])

        # reply with START-OK and connection parameters
        self._x_start_ok(d, login_method, login_response, locale)

        self._wait_tune_ok = True
        while self._wait_tune_ok:
            self.wait(allowed_methods=[spec.Connection.Secure, spec.Connection.Tune])

        self._x_open(virtual_host)

    @property
    def connected(self):
        return self.transport and self.transport.connected

    def _do_close(self):
        try:
            self.transport.close()

            temp_list = [x for x in self.channels.values() if x is not self]
            for ch in temp_list:
                ch._do_close()
        except socket.error:
            pass  # connection already closed on the other end
        finally:
            self.transport = self.connection = self.channels = None

    def _get_free_channel_id(self):
        try:
            return self._avail_channel_ids.pop()
        except IndexError:
            raise ResourceError('No free channel ids, current={0}, channel_max={1}'.format(
                len(self.channels), self.channel_max), spec.Channel.Open)

    def _claim_channel_id(self, channel_id):
        try:
            return self._avail_channel_ids.remove(channel_id)
        except ValueError:
            raise ConnectionError(
                'Channel %r already open' % (channel_id, ))

    def _wait_method(self, channel_id, allowed_methods):
        """Wait for a method from the server destined for a particular channel
        """
        # check the channel's deferred methods
        method_queue = self.channels[channel_id].method_queue

        for queued_method in method_queue:
            method_sig = queued_method[0]
            if (allowed_methods is None) or (method_sig in allowed_methods) or (method_sig == spec.Channel.Close):
                method_queue.remove(queued_method)
                return queued_method

        # nothing queued, need to wait for a method from the peer
        while True:
            channel, method_sig, args, content = self.method_reader.read_method()

            if channel == channel_id \
                    and (allowed_methods is None or method_sig in allowed_methods or method_sig == spec.Channel.Close):
                return method_sig, args, content

            # certain methods like basic_return should be dispatched immediately rather than being queued, even if
            # they're not one of the 'allowed_methods' we're looking for
            if channel and method_sig in self.Channel._IMMEDIATE_METHODS:
                self.channels[channel].dispatch_method(method_sig, args, content)
                continue

            # not the channel and/or method we were looking for; queue this method for later
            self.channels[channel].method_queue.append((method_sig, args, content))

            # If we just queued up a method for channel 0 (the Connection itself) it's probably a close method in
            # reaction to some error, so deal with it right away.
            if not channel:
                self.wait()

    def channel(self, channel_id=None):
        """Fetch a Channel object identified by the numeric channel_id, or create that object if it doesn't already
        exist
        """
        try:
            return self.channels[channel_id]
        except KeyError:
            return self.Channel(self, channel_id)

    def is_alive(self):
        if hasattr(socket, 'MSG_PEEK'):
            sock = self.sock
            prev = sock.gettimeout()
            sock.settimeout(0.0001)
            try:
                sock.recv(1, socket.MSG_PEEK)
            except socket.timeout:
                pass
            except socket.error:
                return False
            finally:
                sock.settimeout(prev)
        return True

    def drain_events(self, timeout=None):
        """Wait for an event on a channel

        :param timeout: maximum allowed time wait for an event
        :type timeout: float or None
        :raise amqpy.exceptions.Timeout: if the operation times out
        """
        chanmap = self.channels
        chanid, method_sig, args, content = self._wait_multiple(chanmap, None, timeout=timeout)

        channel = chanmap[chanid]

        if content and channel.auto_decode and hasattr(content, 'content_encoding'):
            try:
                content.body = content.body.decode(content.content_encoding)
            except Exception:
                pass

        amqp_method = self._method_override.get(method_sig) or channel._METHOD_MAP.get(method_sig, None)

        if amqp_method is None:
            raise AMQPNotImplementedError('Unknown AMQP method {0!r}'.format(method_sig))

        if content is None:
            return amqp_method(channel, args)
        else:
            return amqp_method(channel, args, content)

    def _wait_multiple(self, channels, allowed_methods, timeout=None):
        for channel_id, channel in channels.items():
            method_queue = channel.method_queue
            for queued_method in method_queue:
                method_sig = queued_method[0]
                if allowed_methods is None or method_sig in allowed_methods or method_sig == spec.Channel.Close:
                    method_queue.remove(queued_method)
                    method_sig, args, content = queued_method
                    return channel_id, method_sig, args, content

        # nothing queued, need to wait for a method from the peer
        while True:
            channel, method_sig, args, content = self.method_reader.read_method(timeout)

            if channel in channels \
                    and (allowed_methods is None or method_sig in allowed_methods or method_sig == spec.Channel.Close):
                return channel, method_sig, args, content

            # not the channel and/or method we were looking for; queue this method for later
            channels[channel].method_queue.append((method_sig, args, content))

            # if we just queued up a method for channel 0 (the Connection itself) it's probably a close method in
            # reaction to some error, so deal with it right away
            if channel == 0:
                self.wait()

    def _dispatch_basic_return(self, channel, args, msg):
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()

        exc = error_for_code(reply_code, reply_text, (50, 60), ChannelError)
        handlers = channel.events.get('basic_return')
        if not handlers:
            raise exc
        for callback in handlers:
            callback(exc, exchange, routing_key, msg)

    def close(self, reply_code=0, reply_text='', method_sig=(0, 0)):
        """Request a connection close

        This method indicates that the sender wants to close the connection. This may be due to internal conditions
        (e.g. a forced shut-down) or due to an error handling a specific method, i.e. an exception.  When a close is due
        to an exception, the sender provides the class and method id of the method which caused the exception.

        RULE:

            After sending this method any received method except the Close-OK method MUST be discarded.

        RULE:

            The peer sending this method MAY use a counter or timeout to detect failure of the other peer to respond
            correctly with the Close-OK method.

        RULE:

            When a server receives the Close method from a client it MUST delete all server-side resources associated
            with the client's context.  A client CANNOT reconnect to a context after sending or receiving a Close
            method.

        PARAMETERS:

            reply_code: short

                The reply code. The AMQ reply codes are defined in AMQ RFC 011.

            reply_text: shortstr

                The localised reply text.  This text can be logged as an aid to resolving issues.

            class_id: short

                failing method class

                When the close is provoked by a method exception, this is the class of the method.

            method_id: short

                failing method ID

                When the close is provoked by a method exception, this is the ID of the method.

        """
        if self.transport is None:
            # already closed
            return

        args = AMQPWriter()
        args.write_short(reply_code)
        args.write_shortstr(reply_text)
        args.write_short(method_sig[0])  # class_id
        args.write_short(method_sig[1])  # method_id
        self._send_method(Method(spec.Connection.Close, args.getvalue()))
        return self.wait(allowed_methods=[spec.Connection.Close, spec.Connection.CloseOk])

    def _close(self, args):
        """Request a connection close

        This method indicates that the sender wants to close the connection. This may be due to internal conditions
        (e.g. a forced shut-down) or due to an error handling a specific method, i.e. an exception.  When a close is due
        to an exception, the sender provides the class and method id of the method which caused the exception.

        RULE:

            After sending this method any received method except the Close-OK method MUST be discarded.

        RULE:

            The peer sending this method MAY use a counter or timeout to detect failure of the other peer to respond
            correctly with the Close-OK method.

        RULE:

            When a server receives the Close method from a client it MUST delete all server-side resources associated
            with the client's context.  A client CANNOT reconnect to a context after sending or receiving a Close
            method.

        PARAMETERS: reply_code: short

                The reply code. The AMQ reply codes are defined in AMQ RFC 011.

            reply_text: shortstr

                The localised reply text.  This text can be logged as an aid to resolving issues.

            class_id: short

                failing method class

                When the close is provoked by a method exception, this is the class of the method.

            method_id: short

                failing method ID

                When the close is provoked by a method exception, this is the ID of the method.
        """
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        class_id = args.read_short()
        method_id = args.read_short()

        self._x_close_ok()

        raise error_for_code(reply_code, reply_text, (class_id, method_id), ConnectionError)

    def _blocked(self, args):
        """RabbitMQ Extension."""
        reason = args.read_shortstr()
        if self.on_blocked:
            return self.on_blocked(reason)

    def _unblocked(self, *args):
        if self.on_unblocked:
            return self.on_unblocked()

    def _x_close_ok(self):
        """Confirm a connection close

        This method confirms a Connection.Close method and tells the recipient that it is safe to release resources for
        the connection and close the socket. RULE: A peer that detects a socket closure without having received a
        Close-Ok handshake method SHOULD log the error.
        """
        self._send_method(Method(spec.Connection.CloseOk))
        self._do_close()

    def _close_ok(self, args):
        """Confirm a connection close

        This method confirms a Connection.Close method and tells the recipient that it is safe to release resources for
        the connection and close the socket. A peer that detects a socket closure without having received a Close-Ok
        handshake method SHOULD log the error.
        """
        self._do_close()

    def _x_open(self, virtual_host, capabilities=''):
        """Open connection to virtual host

        This method opens a connection to a virtual host, which is a collection of resources, and acts to separate
        multiple application domains within a server. RULE: The client MUST open the context before doing any work on
        the connection.

        :param virtual_host: virtual host path
        :param capabilities: required capabilities
        :type virtual_host: str
        :type capabilities: str
        """
        args = AMQPWriter()
        args.write_shortstr(virtual_host)
        args.write_shortstr(capabilities)
        args.write_bit(False)
        self._send_method(Method(spec.Connection.Open, args.getvalue()))
        return self.wait(allowed_methods=[spec.Connection.OpenOk])

    def _open_ok(self, args):
        """Signal that the connection is ready

        This method signals to the client that the connection is ready for use.
        """
        log.debug('Open OK')

    def _secure(self, args):
        """Security mechanism challenge

        The SASL protocol works by exchanging challenges and responses until both peers have received sufficient
        information to authenticate each other.  This method challenges the client to provide more information.

        PARAMETERS:
            challenge: longstr
                security challenge data
                Challenge information, a block of opaque binary data passed to the security mechanism.
        """
        challenge = args.read_longstr()

    def _x_secure_ok(self, response):
        """Security mechanism response

        This method attempts to authenticate, passing a block of SASL data for the security mechanism at the server
        side.

        PARAMETERS:
            response: longstr
                security response data
                A block of opaque data passed to the security mechanism. The contents of this data are defined by the
                SASL security mechanism.
        """
        args = AMQPWriter()
        args.write_longstr(response)
        self._send_method(Method(spec.Connection.SecureOk, args.getvalue()))

    def _start(self, args):
        """Start connection negotiation

        This method starts the connection negotiation process by telling the client the protocol version that the server
        proposes, along with a list of security mechanisms which the client can use for authentication.

        RULE: If the client cannot handle the protocol version suggested by the server it MUST close the socket
        connection.

        RULE: The server MUST provide a protocol version that is lower than or equal to that requested by the client in
        the protocol header. If the server cannot support the specified protocol it MUST NOT send this method, but MUST
        close the socket connection.

        PARAMETERS:
            version_major: octet
                protocol major version
                The protocol major version that the server agrees to use, which cannot be higher than the client's major
                version.
            version_minor: octet
                protocol major version
                The protocol minor version that the server agrees to use, which cannot be higher than the client's minor
                version.
            server_properties: table
                server properties
            mechanisms: longstr
                available security mechanisms
                A list of the security mechanisms that the server supports, delimited by spaces.  Currently ASL supports
                these mechanisms: PLAIN.
            locales: longstr
                available message locales
                A list of the message locales that the server supports, delimited by spaces.  The locale defines the
                language in which the server will send reply texts.
                RULE:
                    All servers MUST support at least the en_US locale.
        """
        self.version_major = args.read_octet()
        self.version_minor = args.read_octet()
        self.server_properties = args.read_table()
        self.mechanisms = args.read_longstr().split(' ')
        self.locales = args.read_longstr().split(' ')

        debug_msg = 'Start from server, version: {}.{}, properties: {}, mechanisms: {}, locales: {}'.strip()
        log.debug(
            debug_msg.format(self.version_major, self.version_minor, self.server_properties, self.mechanisms,
                             self.locales))

    def _x_start_ok(self, client_properties, mechanism, response, locale):
        """Select security mechanism and locale

        This method selects a SASL security mechanism. ASL uses SASL (RFC2222) to negotiate authentication and
        encryption.

        PARAMETERS:
            client_properties: table
                client properties
            mechanism: shortstr
                selected security mechanism
                A single security mechanisms selected by the client, which must be one of those specified by the server.
                RULE:
                    The client SHOULD authenticate using the highest- level security profile it can handle from the list
                    provided by the server.
                RULE:
                    The mechanism field MUST contain one of the security mechanisms proposed by the server in the Start
                    method. If it doesn't, the server MUST close the socket.
            response: longstr
                security response data
                A block of opaque data passed to the security mechanism. The contents of this data are defined by the
                SASL security mechanism.  For the PLAIN security mechanism this is defined as a field table holding two
                fields, LOGIN and PASSWORD.
            locale: shortstr
                selected message locale
                A single message local selected by the client, which must be one of those specified by the server.
        """
        if self.server_capabilities.get('consumer_cancel_notify'):
            if 'capabilities' not in client_properties:
                client_properties['capabilities'] = {}
            client_properties['capabilities']['consumer_cancel_notify'] = True
        if self.server_capabilities.get('connection.blocked'):
            if 'capabilities' not in client_properties:
                client_properties['capabilities'] = {}
            client_properties['capabilities']['connection.blocked'] = True
        args = AMQPWriter()
        args.write_table(client_properties)
        args.write_shortstr(mechanism)
        args.write_longstr(response)
        args.write_shortstr(locale)
        self._send_method(Method(spec.Connection.StartOk, args.getvalue()))

    def _tune(self, args):
        """Propose connection tuning parameters
        This method proposes a set of connection configuration values to the client.  The client can accept and/or
        adjust these.

        PARAMETERS:
            channel_max: short
                proposed maximum channels
                The maximum total number of channels that the server allows per connection. Zero means that the server
                does not impose a fixed limit, but the number of allowed channels may be limited by available server
                resources.
            frame_max: long
                proposed maximum frame size
                The largest frame size that the server proposes for the connection. The client can negotiate a lower
                value.  Zero means that the server does not impose any specific limit but may reject very large frames
                if it cannot allocate resources for them.
                RULE:
                    Until the frame-max has been negotiated, both peers MUST accept frames of up to 4096 octets large.
                    The minimum non-zero value for the frame- max field is 4096.
            heartbeat: short
                desired heartbeat delay
                The delay, in seconds, of the connection heartbeat that the server wants.  Zero means the server does
                not want a heartbeat.
        """
        client_heartbeat = self.client_heartbeat or 0
        self.channel_max = args.read_short() or self.channel_max
        self.frame_max = args.read_long() or self.frame_max
        self.method_writer.frame_max = self.frame_max
        self.server_heartbeat = args.read_short() or 0

        # negotiate the heartbeat interval to the smaller of the specified values
        if self.server_heartbeat == 0 or client_heartbeat == 0:
            self.heartbeat = max(self.server_heartbeat, client_heartbeat)
        else:
            self.heartbeat = min(self.server_heartbeat, client_heartbeat)

        # Ignore server heartbeat if client_heartbeat is disabled
        if not self.client_heartbeat:
            self.heartbeat = 0

        self._x_tune_ok(self.channel_max, self.frame_max, self.heartbeat)

    def send_heartbeat(self):
        self.transport.write_frame(8, 0, bytes())

    def _x_tune_ok(self, channel_max, frame_max, heartbeat):
        """Negotiate connection tuning parameters

        This method sends the client's connection tuning parameters to the server. Certain fields are negotiated, others
        provide capability information.

        PARAMETERS:
            channel_max: short
                negotiated maximum channels
                The maximum total number of channels that the client will use per connection.  May not be higher than
                the value specified by the server.
                RULE:
                    The server MAY ignore the channel-max value or MAY use it for tuning its resource allocation.
            frame_max: long
                negotiated maximum frame size
                The largest frame size that the client and server will use for the connection.  Zero means that the
                client does not impose any specific limit but may reject very large frames if it cannot allocate
                resources for them. Note that the frame-max limit applies principally to content frames, where large
                contents can be broken into frames of arbitrary size.
                RULE:
                    Until the frame-max has been negotiated, both peers must accept frames of up to 4096 octets large.
                    The minimum non-zero value for the frame- max field is 4096.
            heartbeat: short
                desired heartbeat delay
                The delay, in seconds, of the connection heartbeat that the client wants. Zero means the client does not
                want a heartbeat.
        """
        args = AMQPWriter()
        args.write_short(channel_max)
        args.write_long(frame_max)
        args.write_short(heartbeat or 0)
        self._send_method(Method(spec.Connection.TuneOk, args.getvalue()))
        self._wait_tune_ok = False

    @property
    def sock(self):
        return self.transport.sock

    @property
    def server_capabilities(self):
        return self.server_properties.get('capabilities') or {}

    _METHOD_MAP = {
        spec.Connection.Start: _start,
        spec.Connection.Secure: _secure,
        spec.Connection.Tune: _tune,
        spec.Connection.OpenOk: _open_ok,
        spec.Connection.Close: _close,
        spec.Connection.CloseOk: _close_ok,
        spec.Connection.Blocked: _blocked,
        spec.Connection.Unblocked: _unblocked,
    }
