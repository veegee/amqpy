import uuid

import pytest

from .. import ChannelError, Channel, Message, FrameSyntaxError, NotFound, AccessRefused, PreconditionFailed
from .conftest import get_server_props


class TestExchange:
    def test_exchange_declare_and_delete(self, ch):
        exch_name = 'test_exchange_{}'.format(uuid.uuid4())
        ch.exchange_declare(exch_name, 'direct')
        ch.exchange_delete(exch_name)

    def test_exchange_declare_passive_raises(self, ch):
        with pytest.raises(NotFound):
            exch_name = 'test_exchange_{}'.format(uuid.uuid4())
            ch.exchange_declare(exch_name, 'direct', passive=True)

    def test_exchange_delete_nonexistent_raises(self, ch, rand_exch):
        """Test to ensure that deleting a nonexistent exchange raises `NotFound`

        Note: starting with RabbitMQ 3.2 (?), exchange.delete is an idempotent assertion that the exchange must not
        exist.

             We have made queue.delete into an idempotent assertion that the queue must not exist, in the same way that
             queue.declare asserts that it must. See https://www.rabbitmq.com/specification.html

        This means that the RabbitMQ server will not raise a 404 NOT FOUND channel exception when attempting to
        delete a nonexistent exchange.
        """
        server_props = get_server_props(ch.connection)

        if server_props[0] == 'RabbitMQ' and server_props[1] >= (3, 2, 0):
            ch.exchange_delete(rand_exch)
        else:
            with pytest.raises(NotFound):
                ch.exchange_delete(rand_exch)

    def test_exchange_delete_default(self, ch):
        with pytest.raises(AccessRefused):
            ch.exchange_delete('')

    def test_exchange_delete_in_use(self, ch, rand_exch, rand_queue, rand_rk):
        ch.exchange_declare(rand_exch, 'direct')
        ch.queue_declare(rand_queue)
        ch.queue_bind(rand_queue, rand_exch, rand_rk)

        with pytest.raises(PreconditionFailed):
            ch.exchange_delete(rand_exch, if_unused=True)

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


class TestQueue:
    def test_queue_delete_nonexistent(self, ch):
        """Test to ensure that deleting a nonexistent queue raises `NotFound`

        Note: starting with RabbitMQ 3.2 (?), queue.delete is an idempotent assertion that the queue must not exist.

            We have made exchange.delete into an idempotent assertion that the exchange must not exist, in the same way
            that exchange.declare asserts that it must. See https://www.rabbitmq.com/specification.html

        This means that the RabbitMQ server will not raise a 404 NOT FOUND channel exception when attempting to
        delete a nonexistent queue.
        """
        server_props = get_server_props(ch.connection)
        if server_props[0] == 'RabbitMQ' and server_props[1] >= (3, 2, 0):
            assert ch.queue_delete('bogus_queue_that_does_not_exist') == 0
        else:
            with pytest.raises(NotFound):
                ch.queue_delete('bogus_queue_that_does_not_exist')

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

    def test_unbind_nonexistent(self, ch, rand_exch, rand_queue, rand_rk):
        """Test to ensure that unbinding a nonexistent binding raises `NotFound`

        Note: starting with RabbitMQ 3.2 (?), queue.delete is an idempotent assertion that the queue must not exist.

            We have made exchange.delete into an idempotent assertion that the exchange must not exist, in the same way
            that exchange.declare asserts that it must. See https://www.rabbitmq.com/specification.html

        This means that the RabbitMQ server will not raise a 404 NOT FOUND channel exception when attempting to
        delete a nonexistent queue.
        """
        server_props = get_server_props(ch.connection)

        if server_props[0] == 'RabbitMQ' and server_props[1] >= (3, 2, 0):
            ch.queue_unbind(rand_queue, rand_exch, rand_rk)
        else:
            with pytest.raises(NotFound):
                ch.queue_unbind(rand_queue, rand_exch, rand_rk)


class TestPublish:
    def test_publish(self, ch):
        ch.exchange_declare('funtest.fanout', 'fanout', auto_delete=True)
        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})
        ch.basic_publish(msg, 'funtest.fanout')

    def test_publish_large(self, ch):
        """Test sending some extra large messages
        """
        qname, _, _ = ch.queue_declare()

        for multiplier in [100, 1000, 10000, 50000, 100000, 500000]:
            msg = Message('this is a test message' * multiplier, content_type='text/plain',
                          application_headers={'foo': 7, 'bar': 'baz'})

            ch.basic_publish(msg, routing_key=qname)

            msg2 = ch.basic_get(no_ack=True)
            assert msg == msg2

    def test_publish_confirm(self, ch: Channel, rand_exch):
        queue_name = 'test.queue.publish'
        rk = queue_name
        ch.exchange_declare(rand_exch, 'direct', auto_delete=True)
        ch.queue_declare(queue_name)
        ch.queue_bind(queue_name, rand_exch, rk)
        msg = Message('funtest message', content_type='text/plain', application_headers={'foo': 7, 'bar': 'baz'})
        ch.confirm_select()
        ch.basic_publish_confirm(msg, rand_exch, rk)


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
        assert msg2.properties['content_encoding'] == 'UTF-8'
        assert msg2.body == 'hello world'

        # Explicit latin_1 encoding, still comes back as unicode
        msg = Message('hello world', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.properties['content_encoding'] == 'latin_1'
        assert msg2.body == 'hello world'

        # plain string with specified encoding comes back as unicode
        msg = Message('hello w\xf6rld', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)

        assert msg2.properties['content_encoding'] == 'latin_1'
        assert msg2.body == 'hello w\u00f6rld'

        # plain string (bytes in Python 3.x) with bogus encoding
        # don't really care about latin_1, just want bytes
        test_bytes = 'hello w\xd6rld'.encode('latin_1')
        msg = Message(test_bytes, content_encoding='I made this up')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.properties['content_encoding'] == 'I made this up'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == test_bytes

        # turn off auto_decode for remaining tests
        ch.auto_decode = False

        # unicode body comes back as utf-8 encoded str
        msg = Message('hello w\u00f6rld')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.properties['content_encoding'] == 'UTF-8'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == 'hello w\xc3\xb6rld'.encode('latin_1')

        # plain string with specified encoding stays plain string
        msg = Message('hello w\xf6rld', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.properties['content_encoding'] == 'latin_1'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == 'hello w\xf6rld'.encode('latin_1')

        # explicit latin_1 encoding, comes back as str
        msg = Message('hello w\u00f6rld', content_encoding='latin_1')
        ch.basic_publish(msg, 'amq.direct', routing_key=my_routing_key)
        msg2 = ch.basic_get(qname, no_ack=True)
        assert msg2.properties['content_encoding'] == 'latin_1'
        assert isinstance(msg2.body, bytes)
        assert msg2.body == 'hello w\xf6rld'.encode('latin_1')

    def test_survives_channel_error(self, ch):
        with pytest.raises(ChannelError):
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

        with pytest.raises(FrameSyntaxError):
            ch.basic_publish(msg, routing_key=qname)

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
