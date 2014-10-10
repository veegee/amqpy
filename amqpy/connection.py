"""AMQP Connections
"""
import ssl
import logging
import socket
from array import array
import pprint
from threading import Event, Thread

from .method_framing import MethodReader, MethodWriter
from .serialization import AMQPWriter
from . import __version__
from .abstract_channel import AbstractChannel
from .channel import Channel
from .exceptions import ResourceError, AMQPConnectionError, error_for_code
from .transport import create_transport
from . import spec
from .spec import Method, Frame, FrameType, method_t
from .concurrency import synchronized

__all__ = ['Connection']

# client property info that gets sent to the server on connection startup
LIBRARY_PROPERTIES = {
    'product': 'amqpy',
    'product_version': __version__,
    'version': __version__,
    'capabilities': {},
}

log = logging.getLogger('amqpy')


class Connection(AbstractChannel):
    """The connection class provides methods for a client to establish a network connection to a server, and for both
    peers to operate the connection thereafter
    """

    def __init__(self, *, host='localhost', port=5672, ssl=None, connect_timeout=None,
                 userid='guest', password='guest', login_method='AMQPLAIN', virtual_host='/', locale='en_US',
                 channel_max=65535, frame_max=131072, heartbeat=0, auto_heartbeat=False,
                 publisher_confirms=False, client_properties=None,
                 on_blocked=None, on_unblocked=None):
        """Create a connection to the specified host

        If you are using SSL, make sure the correct port number is specified (usually 5671), as the default of 5672 is
        for non-SSL connections.

        :param str host: host
        :param int port: port
        :param ssl: dict of SSL options passed to :func:`ssl.wrap_socket()`, None to disable SSL
        :param float connect_timeout: connect timeout
        :param str userid: username
        :param str password: password
        :param str login_method: login method (this is server-specific); default is for RabbitMQ
        :param str virtual_host: virtual host
        :param str locale: locale
        :param int channel_max: maximum number of channels
        :param int frame_max: maximum frame payload size in bytes
        :param float heartbeat: heartbeat interval in seconds, 0 disables heartbeat
        :param bool auto_heartbeat: enable automatic heartbeats thread
        :param bool publisher_confirms: enable publisher confirms by default for new channels
        :param client_properties: dict of client properties
        :param on_blocked: callback on connection blocked
        :param on_unblocked: callback on connection unblocked
        :type connect_timeout: float or None
        :type client_properties: dict or None
        :type ssl: dict or None
        :type on_blocked: Callable or None
        :type on_unblocked: Callable or None
        """
        # `channels` map stores references to all active channels
        self.channels = {}  # dict of {channel_id int: Channel}

        # the connection object itself is treated as channel 0
        super().__init__(self, 0)

        # properties set in the Tune method
        self.channel_max = channel_max
        self.frame_max = frame_max
        # final heartbeat interval value (in float seconds) after negotiation
        self.heartbeat = 0
        # original heartbeat interval value proposed by client
        self.client_heartbeat = heartbeat
        # original heartbeat interval proposed by server
        self.server_heartbeat = None

        self.publisher_confirms_enabled = publisher_confirms

        # callbacks
        self.on_blocked = on_blocked
        self.on_unblocked = on_unblocked

        self._avail_channel_ids = array('H', range(self.channel_max, 0, -1))

        # properties set in the start method, after a connection is established
        self.version_major = 0
        self.version_minor = 0
        self.server_properties = {}
        self.mechanisms = []
        self.locales = []

        # start the connection; this also sends the connection protocol header
        self.transport = create_transport(host, port, connect_timeout, frame_max, ssl)

        # create global instances of `MethodReader` and `MethodWriter` which can be used by all channels
        self.method_reader = MethodReader(self.transport)
        self.method_writer = MethodWriter(self.transport, self.frame_max)

        # wait for server to send the 'start' method
        self.wait(allowed_methods=[spec.Connection.Start])

        # create 'login response' to send to server
        login_response = AMQPWriter()
        login_response.write_table({'LOGIN': userid, 'PASSWORD': password})
        login_response = login_response.getvalue()[4:]  # skip the length

        # reply with 'start-ok' and connection parameters
        # noinspection PyArgumentList
        client_props = dict(LIBRARY_PROPERTIES, **client_properties or {})
        self._send_start_ok(client_props, login_method, login_response, locale)

        self._wait_tune_ok = True
        while self._wait_tune_ok:
            self.wait(allowed_methods=[spec.Connection.Secure, spec.Connection.Tune])

        self._send_open(virtual_host)

        # set up automatic heartbeats
        self._close_event = Event()
        if auto_heartbeat:
            log.debug('Start automatic heartbeat thread')
            t = Thread(target=self._heartbeat_thread, name='HeartbeatThread')
            t.start()

    @property
    def connected(self):
        """Check if connection is connected

        :return: True if connected, else False
        :rtype: bool
        """
        return bool(self.transport and self.transport.connected)

    @property
    def sock(self):
        """Access underlying TCP socket

        :return: socket
        :rtype: socket.socket
        """
        if self.transport and self.transport.sock:
            return self.transport.sock

    @property
    def server_capabilities(self):
        """Get server capabilities

        These properties are set only after successfully connecting.

        :return: server capabilities
        :rtype: dict
        """
        return self.server_properties.get('capabilities') or {}

    @synchronized('lock')
    def channel(self, channel_id=None):
        """Create a new channel, or fetch the channel associated with `channel_id` if specified

        :param channel_id: channel ID number
        :type channel_id: int or None
        :return: Channel
        :rtype: amqpy.channel.Channel
        """
        return self.channels.get(channel_id, Channel(self, channel_id))

    def send_heartbeat(self):
        """Send a heartbeat to the server
        """
        self.transport.write_frame(Frame(FrameType.HEARTBEAT))

    def is_alive(self):
        """Check if connection is alive

        This method is the primary way to check if the connection is alive.

        Side effects: This method may send a heartbeat as a last resort to check if the connection is alive.

        :return: True if connection is alive, else False
        :rtype: bool
        """
        if not self.sock:
            # we don't have a valid socket, this connection is definitely not alive
            return False

        if not self.connected:
            # the `transport` is not connected
            return False

        # recv with MSG_PEEK to check if the connection is alive
        # note: if there is data still in the buffer, this will not tell us anything
        if hasattr(socket, 'MSG_PEEK') and not isinstance(self.sock, ssl.SSLSocket):
            prev = self.sock.gettimeout()
            self.sock.settimeout(0.0001)
            try:
                self.sock.recv(1, socket.MSG_PEEK)
            except socket.timeout:
                pass
            except socket.error:
                # the exception is usually (always?) a ConnectionResetError in Python 3.3+
                return False
            finally:
                self.sock.settimeout(prev)

        # send a heartbeat to check if the connection is alive
        try:
            self.send_heartbeat()
        except socket.error:
            return False

        return True

    def _wait_any(self, timeout=None):
        """Wait for any event on the connection (for any channel)

        This method is called by :meth:`Connection.drain_events()`.

        :param float timeout: timeout
        :return: method
        :rtype: amqpy.spec.Method
        """
        # check the method queue of each channel
        for ch_id, channel in self.channels.items():
            if channel.method_queue:
                return channel.method_queue.pop(0)

        # do a blocking read for any incoming method
        method = self.method_reader.read_method(timeout)
        return method

    def drain_events(self, timeout=None):
        """Wait for an event on all channels

        This method should be called after creating consumers in order to receive delivered messages and execute
        consumer callbacks.

        :param timeout: maximum allowed time wait for an event
        :type timeout: float or None
        :raise amqpy.exceptions.Timeout: if the operation times out
        """
        method = self._wait_any(timeout)
        assert isinstance(method, Method)
        channel = self.channels[method.channel_id]
        return self.handle_method(method, channel)

    def close(self, reply_code=0, reply_text='', method_type=method_t(0, 0)):
        """Close connection to the server

        This method performs a connection close handshake with the server, then closes the underlying connection.

        If this connection close is due to a client error, the client may provide a `reply_code`, `reply_text`,
        and `method_type` to indicate to the server the reason for closing the connection.

        :param int reply_code: the reply code
        :param str reply_text: localized reply text
        :param method_type: if close is triggered by a failing method, this is the method that caused it
        :type method_type: amqpy.spec.method_t
        """
        # signal to the heartbeat thread to stop sending heartbeats
        self._close_event.set()

        if not self.is_alive():
            # already closed
            log.debug('Already closed')
            return

        args = AMQPWriter()
        args.write_short(reply_code)
        args.write_shortstr(reply_text)
        args.write_short(method_type.class_id)
        args.write_short(method_type.method_id)
        self._send_method(Method(spec.Connection.Close, args))
        return self.wait(allowed_methods=[spec.Connection.Close, spec.Connection.CloseOk])

    def _heartbeat_thread(self):
        # `is_alive()` sends heartbeats if the connection is alive
        while self.is_alive():
            # `close` is set to true if the `close_event` is signalled
            close = self._close_event.wait(self.heartbeat / 1.5)
            if close:
                break

    def _close(self):
        try:
            self.transport.close()

            channels = [x for x in self.channels.values() if x is not self]
            for ch in channels:
                # noinspection PyProtectedMember
                ch._close()
        except socket.error:
            pass  # connection already closed on the other end
        finally:
            self.transport = self.connection = self.channels = None

    def _get_free_channel_id(self):
        """Get next free channel ID

        :return: next free channel_id
        :rtype: int
        """
        try:
            return self._avail_channel_ids.pop()
        except IndexError:
            raise ResourceError('No free channel ids, current={0}, channel_max={1}'.format(
                len(self.channels), self.channel_max), spec.Channel.Open)

    def _claim_channel_id(self, channel_id):
        """Claim channel ID

        :param channel_id: channel ID
        :type channel_id: int
        """
        try:
            return self._avail_channel_ids.remove(channel_id)
        except ValueError:
            raise AMQPConnectionError('Channel {} already open'.format(channel_id))

    def _cb_close(self, method):
        """Handle received connection close

        This method indicates that the sender (server) wants to close the connection. This may be due to internal
        conditions (e.g. a forced shut-down) or due to an error handling a specific method, i.e. an exception. When a
        close is due to an exception, the sender provides the class and method id of the method which caused the
        exception.
        """
        args = method.args
        reply_code = args.read_short()  # the AMQP reply code
        reply_text = args.read_shortstr()  # the localized reply text
        class_id = args.read_short()  # class_id of method
        method_id = args.read_short()  # method_id of method

        self._send_close_ok()  # send a close-ok to the server, to confirm that we've acknowledged the close request

        method_type = method_t(class_id, method_id)
        raise error_for_code(reply_code, reply_text, method_type, AMQPConnectionError, self.channel_id)

    def _cb_blocked(self, method):
        """RabbitMQ Extension
        """
        reason = method.args.read_shortstr()
        if callable(self.on_blocked):
            # noinspection PyCallingNonCallable
            return self.on_blocked(reason)

    def _cb_unblocked(self, method):
        assert method
        if callable(self.on_unblocked):
            # noinspection PyCallingNonCallable
            return self.on_unblocked()

    def _send_close_ok(self):
        """Confirm a connection close that has been requested by the server

        This method confirms a Connection.Close method and tells the recipient that it is safe to release resources for
        the connection and close the socket. RULE: A peer that detects a socket closure without having received a
        Close-Ok handshake method SHOULD log the error.
        """
        self._send_method(Method(spec.Connection.CloseOk))
        self._close()

    def _cb_close_ok(self, method):
        """Confirm a connection close

        This method is called when the server send a close-ok in response to our close request. It is now safe to
        close the underlying connection.
        """
        assert method
        self._close()

    def _send_open(self, virtual_host, capabilities=''):
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
        self._send_method(Method(spec.Connection.Open, args))
        return self.wait(allowed_methods=[spec.Connection.OpenOk])

    def _cb_open_ok(self, method):
        """Signal that the connection is ready

        This method signals to the client that the connection is ready for use.
        """
        assert method
        log.debug('Open OK')

    def _cb_secure(self, method):
        """Security mechanism challenge

        The SASL protocol works by exchanging challenges and responses until both peers have received sufficient
        information to authenticate each other.  This method challenges the client to provide more information.

        PARAMETERS:
            challenge: longstr
                security challenge data
                Challenge information, a block of opaque binary data passed to the security mechanism.
        """
        challenge = method.args.read_longstr()
        assert challenge

    def _send_secure_ok(self, response):
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
        self._send_method(Method(spec.Connection.SecureOk, args))

    def _cb_start(self, method):
        """Start connection negotiation callback

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
        args = method.args
        self.version_major = args.read_octet()
        self.version_minor = args.read_octet()
        self.server_properties = args.read_table()
        self.mechanisms = args.read_longstr().split(' ')
        self.locales = args.read_longstr().split(' ')

        properties = pprint.pformat(self.server_properties)
        log.debug('Start from server')
        log.debug('Version: {}.{}'.format(self.version_major, self.version_minor))
        log.debug('Server properties:\n{}'.format(properties))
        log.debug('Security mechanisms: {}'.format(self.mechanisms))
        log.debug('Locales: {}'.format(self.locales))

    def _send_start_ok(self, client_properties, mechanism, response, locale):
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
        self._send_method(Method(spec.Connection.StartOk, args))

    def _cb_tune(self, method):
        """Handle received "tune" method

        This method is the handler for receiving a "tune" method. `channel_max` and `frame_max` are set to the lower
        of the values proposed by each party.

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
                    The minimum non-zero value for the frame-max field is 4096.
            heartbeat: short
                desired heartbeat delay
                The delay, in seconds, of the connection heartbeat that the server wants.  Zero means the server does
                not want a heartbeat.
        """
        args = method.args
        client_heartbeat = self.client_heartbeat or 0
        # maximum number of channels that the server supports
        self.channel_max = min(args.read_short(), self.channel_max)
        # largest frame size the server proposes for the connection
        self.frame_max = min(args.read_long(), self.frame_max)
        self.method_writer.frame_max = self.frame_max
        # heartbeat interval proposed by server
        self.server_heartbeat = args.read_short() or 0

        # negotiate the heartbeat interval to the smaller of the specified values
        if self.server_heartbeat == 0 or client_heartbeat == 0:
            self.heartbeat = max(self.server_heartbeat, client_heartbeat)
        else:
            self.heartbeat = min(self.server_heartbeat, client_heartbeat)

        # Ignore server heartbeat if client_heartbeat is disabled
        if not self.client_heartbeat:
            self.heartbeat = 0

        self._send_tune_ok(self.channel_max, self.frame_max, self.heartbeat)

    def _send_tune_ok(self, channel_max, frame_max, heartbeat):
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
        self._send_method(Method(spec.Connection.TuneOk, args))
        self._wait_tune_ok = False

    METHOD_MAP = {
        spec.Connection.Start: _cb_start,
        spec.Connection.Secure: _cb_secure,
        spec.Connection.Tune: _cb_tune,
        spec.Connection.OpenOk: _cb_open_ok,
        spec.Connection.Close: _cb_close,
        spec.Connection.CloseOk: _cb_close_ok,
        spec.Connection.Blocked: _cb_blocked,
        spec.Connection.Unblocked: _cb_unblocked,
    }
