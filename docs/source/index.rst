amqpy Documentation
===================

API documentation:

.. toctree::
    :maxdepth: 8

    amqpy.connection
    amqpy.channel
    amqpy.message
    amqpy.spec


Introduction
============

amqpy is a pure-Python AMQP 0.9.1 client library for Python >= 3.2.0 (including PyPy3) with a focus on:

* stability and reliability
* well-tested and thoroughly documented code
* clean, correct design
* 100% compliance with the AMQP 0.9.1 protocol specification

It has very good performance, as AMQP 0.9.1 is a very simple binary protocol, but does not sacrifice clean design and
testability to save a few extra CPU cycles. This library has a strict zero-bug policy, so all submitted issues and bugs
are always fixed as soon as possible.


Quickstart
==========

amqpy is easy to install, and there are no dependencies::

    pip install amqpy

amqpy is easy to use::

    from amqpy import Connection, Message

    conn = Connection()  # connect to guest:guest@localhost:5672 by default

    ch = conn.channel()

    # declare an exchange and queue, and bind the queue to the exchange
    ch.exchange_declare('test.exchange', 'direct')
    ch.queue_declare('test.q')
    ch.queue_bind('test.q', exch_name='test.exchange', routing_key='test.q')

    # publish a few messages, which will get routed to the queue bound to the routing key "test.q"
    ch.basic_publish(Message('hello world 1'), exchange='test.exchange', routing_key='test.q')
    ch.basic_publish(Message('hello world 2'), exchange='test.exchange', routing_key='test.q')
    ch.basic_publish(Message('hello world 3'), exchange='test.exchange', routing_key='test.q')

    # get a message from the queue
    msg = ch.basic_get('test.q')

    # don't forget to acknowledge it
    msg.ack()

Let's create a consumer::

    def consumer(msg: Message):
        print('Received a message: {}'.format(msg.body))
        msg.ack()

    # declare a consumer
    ch.basic_consume(queue='test.q', consumer_tag='test.consumer', callback=consumer)

    # wait for events, which will receive delivered messages and call any consumer callbacks
    conn.drain_events(timeout=None)


Notes
=====

Any AMQP 0.9.1-compliant server is supported, but RabbitMQ is our primary target. Apache Qpid is confirmed to work,
but only with "anonymous" authentication. A CRAM-MD5 auth mechanism is currently being developed and will be released
shortly.


Features
========

* Draining events from multiple channels (`Connection.drain_events()`)
* SSL is fully supported, it is highly recommended to use SSL when connecting to servers over the Internet.
* Support for timeouts
* Support for heartbeats (client must manually send heartbeats)
* Fully thread-safe
* Supports RabbitMQ extensions:
    * Consumer Cancel Notifications
        * by default a cancel results in `ChannelError` being raised
        * but not if a `on_cancel` callback is passed to `basic_consume`
    * Publisher confirms
        * `Channel.confirm_select()` enables publisher confirms
        * `Channel.events['basic_ack'].append(my_callback)` adds a callback to
          be called when a message is confirmed. This callback is then called
          with the signature `(delivery_tag, multiple)`
    * Exchange to exchange bindings: `exchange_bind()` and `exchange_unbind()`
        * `Channel.confirm_select()` enables publisher confirms
        * `Channel.events['basic_ack'].append(my_callback)` adds a callback to
          be called when a message is confirmed. This callback is then called
          with the signature `(delivery_tag, multiple)`


Testing
=======

amqpy uses the excellent `pytest` framework. To run all tests, simply install a local RabbitMQ server. No additional
configuration is necessary for RabbitMQ. Then install pytest and run in the project root::

    pip install pytest
    py.test


Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

