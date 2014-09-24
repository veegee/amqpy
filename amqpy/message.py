"""Messages for AMQP
"""
from .serialization import GenericContent

__all__ = ['Message']


class Message(GenericContent):
    """A Message for use with the Channnel.basic_* methods."""

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

    def __init__(self, body='', children=None, channel=None, **properties):
        """
        :param body: message body
        :type body: bytes or str

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
        super(Message, self).__init__(**properties)
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
