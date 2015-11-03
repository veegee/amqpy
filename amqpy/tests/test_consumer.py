from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
import logging

from .. import Message, AbstractConsumer

from ..exceptions import Timeout

log = logging.getLogger('amqpy')


class Consumer(AbstractConsumer):
    def run(self, msg):
        log.info('{.consumer_tag} received message: {} {}'.format(self, msg.properties, msg.body))
        msg.ack()


class TestConsumer:
    def test_basic_consume(self, conn, ch, rand_exch, rand_queue):
        self.consume_count = 0

        def consumer(msg):
            """Consume message
            """
            global consume_count
            log.info('Received message: {}'.format(msg.body))
            msg.ack()
            self.consume_count += 1

        q = rand_queue
        rk = q
        exch = rand_exch

        log.info('Declare exchange')
        ch.exchange_declare(exch, 'direct')

        log.info('Declare queue')
        ch.queue_declare(q)

        log.info('Bind queue')
        ch.queue_bind(q, exch, rk)

        log.info('Enable publisher confirms')
        ch.confirm_select()

        log.info('Publish messages')
        ch.basic_publish(Message('Hello, world!', content_type='text/plain'), exch, rk)
        ch.basic_publish(Message('Hello, world!', content_type='text/plain'), exch, rk)

        log.info('Declare consumer')
        ch.basic_consume(q, callback=consumer)

        log.info('Publish messages')
        ch.basic_publish(Message('Hello, world!', content_type='text/plain'), exch, rk)
        ch.basic_publish(Message('Hello, world!', content_type='text/plain'), exch, rk)

        log.info('Begin draining events')
        while True:
            try:
                conn.drain_events(0.1)
            except Timeout:
                break

        assert self.consume_count == 4

    def test_consumer(self, conn, ch, rand_exch, rand_queue):
        q = rand_queue
        rk = q
        exch = rand_exch

        log.info('Declare exchange')
        ch.exchange_declare(exch, 'direct')

        log.info('Declare queue')
        ch.queue_declare(q)

        log.info('Bind queue')
        ch.queue_bind(q, exch, rk)

        log.info('Enable publisher confirms')
        ch.confirm_select()

        log.info('Set QOS')
        ch.basic_qos(prefetch_count=1, a_global=True)

        log.info('Publish messages')
        for i in range(10):
            ch.basic_publish(Message('{}: Hello, world!'.format(i)), exch, rk)

        log.info('Declare consumer')
        c1 = Consumer(ch, q)
        c1.declare()

        log.info('Begin draining events')
        while True:
            try:
                conn.drain_events(0.1)
            except Timeout:
                break

        assert c1.consume_count == 10
