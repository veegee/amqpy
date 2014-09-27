import pytest

from .. import ChannelError, Message, FrameSyntaxError


class TestChannel:
    def test_defaults(self, ch):
        """Test how a queue defaults to being bound to an AMQP default exchange, and how publishing defaults to the
        default exchange, and basic_get defaults to getting from the most recently declared queue, and queue_delete
        defaults to deleting the most recently declared queue
        """
        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})

        qname, _, _ = ch.queue_declare()
        ch.basic_publish(msg, routing_key=qname)

        msg2 = ch.basic_get(no_ack=True)
        assert msg == msg2

        n = ch.queue_purge()
        assert n == 0

        n = ch.queue_delete()
        assert n == 0

    def test_encoding(self, ch):
        my_routing_key = 'funtest.test_queue'

        qname, _, _ = ch.queue_declare()
        ch.queue_bind(qname, 'amq.direct', routing_key=my_routing_key)

        # no encoding, body passed through unchanged
        msg = Message('hello world')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert isinstance(msg2.body, str)
        assert msg2.body == 'hello world'

        # default UTF-8 encoding of unicode body, returned as unicode
        msg = Message('hello world')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.content_encoding == 'UTF-8'
        assert msg2.body == 'hello world'

        # Explicit latin_1 encoding, still comes back as unicode
        msg = Message('hello world', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.content_encoding == 'latin_1'
        assert msg2.body == 'hello world'

        # plain string with specified encoding comes back as unicode
        msg = Message('hello w\xf6rld', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)

        assert msg2.content_encoding == 'latin_1'
        assert msg2.body == 'hello w\u00f6rld'

        # plain string (bytes in Python 3.x) with bogus encoding
        # don't really care about latin_1, just want bytes
        test_bytes = 'hello w\xd6rld'.encode('latin_1')
        msg = Message(test_bytes, content_encoding='I made this up')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.content_encoding == 'I made this up'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == test_bytes

        # turn off auto_decode for remaining tests
        ch.auto_decode = False

        # unicode body comes back as utf-8 encoded str
        msg = Message('hello w\u00f6rld')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.content_encoding == 'UTF-8'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == 'hello w\xc3\xb6rld'.encode('latin_1')

        # plain string with specified encoding stays plain string
        msg = Message('hello w\xf6rld', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.content_encoding == 'latin_1'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == 'hello w\xf6rld'.encode('latin_1')

        # explicit latin_1 encoding, comes back as str
        msg = Message('hello w\u00f6rld', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.content_encoding == 'latin_1'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == 'hello w\xf6rld'.encode('latin_1')

    def test_queue_delete_empty(self, ch):
        assert ch.queue_delete('bogus_queue_that_does_not_exist') == 0

    def test_survives_channel_error(self, ch):
        with pytest.raises(ChannelError) as execinfo:
            ch.queue_declare('krjqheewq_bogus', passive=True)
        ch.queue_declare('funtest_survive')
        ch.queue_declare('funtest_survive', passive=True)
        assert ch.queue_delete('funtest_survive') == 0

    def test_invalid_header(self, ch):
        """Test sending a message with an unserializable object in the header

        http://code.google.com/p/py-amqplib/issues/detail?id=17
        """
        qname, _, _ = ch.queue_declare()

        msg = Message(application_headers={'test': object()})

        with pytest.raises(FrameSyntaxError) as execinfo:
            ch.basic_publish(msg, routing_key=qname)

    def test_large(self, ch):
        """Test sending some extra large messages
        """
        qname, _, _ = ch.queue_declare()

        for multiplier in [100, 1000, 10000, 50000, 100000, 500000]:
            msg = Message('funtest message' * multiplier, content_type='text/plain',
                          application_headers={'foo': 7, 'bar': 'baz'})

            ch.basic_publish(msg, routing_key=qname)

            msg2 = ch.basic_get(no_ack=True)
            assert msg == msg2

    def test_publish(self, ch):
        ch.exchange_declare('funtest.fanout', 'fanout', auto_delete=True)
        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})
        ch.basic_publish(msg, 'funtest.fanout')

    def test_queue(self, ch):
        my_routing_key = 'funtest.test_queue'
        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})

        qname, _, _ = ch.queue_declare()
        ch.queue_bind(qname, 'amq.direct', routing_key=my_routing_key)

        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)

        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg == msg2

    def test_unbind(self, ch):
        my_routing_key = 'funtest.test_queue'

        qname, _, _ = ch.queue_declare()
        ch.queue_bind(qname, 'amq.direct', routing_key=my_routing_key)
        ch.queue_unbind(qname, 'amq.direct', routing_key=my_routing_key)

    def test_basic_return(self, ch):
        ch.exchange_declare('funtest.fanout', 'fanout', auto_delete=True)

        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})

        ch.basic_publish(msg, 'funtest.fanout')
        ch.basic_publish(msg, 'funtest.fanout', mandatory=True)
        ch.basic_publish(msg, 'funtest.fanout', mandatory=True)
        ch.basic_publish(msg, 'funtest.fanout', mandatory=True)
        ch.close()

        # 3 of the 4 messages we sent should have been returned
        assert ch.returned_messages.qsize() == 3

    def test_exchange_bind(self, ch):
        """Test exchange binding

        Network configuration is as follows (-> is forwards to : source_exchange -> dest_exchange -> queue The test
        checks that once the message is publish to the destination exchange(funtest.topic_dest) it is delivered to the
        queue.
        """
        test_routing_key = 'unit_test__key'
        dest_exchange = 'funtest.topic_dest_bind'
        source_exchange = 'funtest.topic_source_bind'

        ch.exchange_declare(dest_exchange, 'topic', auto_delete=True)
        ch.exchange_declare(source_exchange, 'topic', auto_delete=True)

        qname, _, _ = ch.queue_declare()
        ch.exchange_bind(dest_exch=dest_exchange, source_exch=source_exchange, routing_key=test_routing_key)
        ch.queue_bind(qname, dest_exchange, routing_key=test_routing_key)

        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})

        ch.basic_publish(msg, source_exchange, routing_key=test_routing_key)

        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg == msg2

    def test_exchange_unbind(self, ch):
        dest_exchange = 'funtest.topic_dest_unbind'
        source_exchange = 'funtest.topic_source_unbind'
        test_routing_key = 'unit_test__key'

        ch.exchange_declare(dest_exchange, 'topic', auto_delete=True)
        ch.exchange_declare(source_exchange, 'topic', auto_delete=True)

        ch.exchange_bind(dest_exch=dest_exchange, source_exch=source_exchange, routing_key=test_routing_key)

        ch.exchange_unbind(dest_exch=dest_exchange, source_exch=source_exchange, routing_key=test_routing_key)
