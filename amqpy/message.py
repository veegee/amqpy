"""Messages for AMQP
"""
from . import spec
from amqpy.serialization import AMQPReader, AMQPWriter

__all__ = ['Message']


class GenericContent:
    """Abstract base class for AMQP content

    Subclasses should override the PROPERTIES attribute.
    """
    PROPERTIES = [('dummy', 'shortstr')]

    def __init__(self, **props):
        """Save the properties appropriate to this AMQP content type in a 'properties' dictionary
        """
        d = {}
        for propname, _ in self.PROPERTIES:
            if propname in props:
                d[propname] = props[propname]
                # FIXME: should we ignore unknown properties?

        self.properties = d

    def __eq__(self, other):
        """Check if this object has the same properties as another content object
        """
        try:
            return self.properties == other.properties
        except AttributeError:
            return False

    def __getattr__(self, name):
        """Look for additional properties in the 'properties' dictionary, and if present - the 'delivery_info'
        dictionary
        """
        if name == '__setstate__':
            # allows pickling/unpickling to work
            raise AttributeError('__setstate__')

        if name in self.properties:
            return self.properties[name]

        if 'delivery_info' in self.__dict__ \
                and name in self.delivery_info:
            return self.delivery_info[name]

        raise AttributeError(name)

    def _load_properties(self, raw_bytes):
        """Given the raw bytes containing the property-flags and property-list from a content-frame-header, parse and
        insert into a dictionary stored in this object as an attribute named 'properties'
        """
        r = AMQPReader(raw_bytes)

        # read 16-bit shorts until we get one with a low bit set to zero
        flags = []
        while True:
            flag_bits = r.read_short()
            flags.append(flag_bits)
            if flag_bits & 1 == 0:
                break

        shift = 0
        d = {}
        flag_bits = None
        for key, proptype in self.PROPERTIES:
            if shift == 0:
                if not flags:
                    break
                flag_bits, flags = flags[0], flags[1:]
                shift = 15
            if flag_bits & (1 << shift):
                d[key] = getattr(r, 'read_' + proptype)()
            shift -= 1

        self.properties = d

    def serialize_properties(self):
        """Serialize the 'properties' attribute (a dictionary) into the raw bytes making up a set of property flags and
        a property list, suitable for putting into a content frame header
        """
        shift = 15
        flag_bits = 0
        flags = []
        raw_bytes = AMQPWriter()
        for key, proptype in self.PROPERTIES:
            val = self.properties.get(key, None)
            if val is not None:
                if shift == 0:
                    flags.append(flag_bits)
                    flag_bits = 0
                    shift = 15

                flag_bits |= (1 << shift)
                if proptype != 'bit':
                    getattr(raw_bytes, 'write_' + proptype)(val)

            shift -= 1

        flags.append(flag_bits)
        result = AMQPWriter()
        for flag_bits in flags:
            result.write_short(flag_bits)
        result.write(raw_bytes.getvalue())

        return result.getvalue()


class Message(GenericContent):
    """A Message for use with the `Channel.basic_*` methods
    """
    CLASS_ID = spec.Basic.CLASS_ID

    # : Instances of this class have these attributes, which are passed back and forth as message properties between
    # : client and server
    PROPERTIES = [
        ('content_type', 'shortstr'),
        ('content_encoding', 'shortstr'),
        ('application_headers', 'table'),
        ('delivery_mode', 'octet'),
        ('priority', 'octet'),
        ('correlation_id', 'shortstr'),
        ('reply_to', 'shortstr'),
        ('expiration', 'shortstr'),
        ('message_id', 'shortstr'),
        ('timestamp', 'timestamp'),
        ('type', 'shortstr'),
        ('user_id', 'shortstr'),
        ('app_id', 'shortstr'),
        ('cluster_id', 'shortstr')
    ]

    def __init__(self, body='', channel=None, **properties):
        """
        :param body: message body
        :param channel: associated channel
        :type body: bytes or str
        :type channel: amqpy.Channel

        `properties` can include:

            * content_type (shortstr): MIME content type
            * content_encoding (shortstr): MIME content encoding
            * application_headers: (table): Message header field table: dict[str, str|int|Decimal|datetime|dict]
            * delivery_mode: (octet): Non-persistent (1) or persistent (2)
            * priority (octet): The message priority, 0 to 9
            * correlation_id (shortstr) The application correlation identifier
            * reply_to (shortstr) The destination to reply to
            * expiration (shortstr): Message expiration specification
            * message_id (shortstr): The application message identifier
            * timestamp (datetime.datetime): The message timestamp
            * type (shortstr): The message type name
            * user_id (shortstr): The creating user id
            * app_id (shortstr): The creating application id
            * cluster_id (shortstr): Intra-cluster routing identifier

        example::

            msg = Message('hello world', content_type='text/plain', application_headers={'foo': 7})
        """
        super().__init__(**properties)
        self.body = body
        self.channel = channel

    def __eq__(self, other):
        """Check if the properties and bodies of this Message and another Message are the same

        Received messages may contain a 'delivery_info' attribute, which isn't compared.
        """
        try:
            return super().__eq__(other) and self.body == other.body
        except AttributeError:
            return False

    @property
    def headers(self):
        return self.properties.get('application_headers')

    @property
    def delivery_tag(self):
        return self.delivery_info.get('delivery_tag')

    def ack(self):
        """`ack` message with basic_ack()

        This is a convenience method which calls :meth:`self.channel.basic_ack()`
        """
        self.channel.basic_ack(self.delivery_tag, False)
