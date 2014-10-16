amqpy Documentation
===================

API documentation:

.. toctree::
    :maxdepth: 8

    amqpy.connection
    amqpy.channel
    amqpy.message
    amqpy.consumer
    amqpy.spec
    amqpy.exceptions


Introduction
============

amqpy is a pure-Python AMQP 0.9.1 client library for Python >= 3.2.0 (including PyPy3) with a focus
on:

- stability and reliability
- well-tested and thoroughly documented code
- clean, correct design
- 100% compliance with the AMQP 0.9.1 protocol specification

It has very good performance, as AMQP 0.9.1 is a very efficient binary protocol, but does not
sacrifice clean design and testability to save a few extra CPU cycles.

This library is actively maintained and has a zero bug policy. Please submit issues and pull
requests, and bugs will be fixed immediately.

The current API is not final, but will progressively get more stable as version 1.0.0 is approached.


Guarantees
----------

This library makes the following guarantees:

* `Semantic versioning`_ is strictly followed
* Compatible with Python >= 3.2.0 and PyPy3 >= 2.3.1 (Python 3.2.5)
* AMQP 0.9.1 compliant


Quickstart
==========

amqpy is easy to install, and there are no dependencies::

    pip install amqpy

amqpy is easy to use::

    from amqpy import Connection, Message, AbstractConsumer, Timeout

    conn = Connection()  # connect to guest:guest@localhost:5672 by default

    ch = conn.channel()

    # declare an exchange and queue, and bind the queue to the exchange
    ch.exchange_declare('test.exchange', 'direct')
    ch.queue_declare('test.q')
    ch.queue_bind('test.q', exchange='test.exchange', routing_key='test.q')

    # publish a few messages, which will get routed to the queue bound to the routing key "test.q"
    ch.basic_publish(Message('hello world 1'), exchange='test.exchange', routing_key='test.q')
    ch.basic_publish(Message('hello world 2'), exchange='test.exchange', routing_key='test.q')
    ch.basic_publish(Message('hello world 3'), exchange='test.exchange', routing_key='test.q')

    # get a message from the queue
    msg = ch.basic_get('test.q')

    # don't forget to acknowledge it
    msg.ack()

Let's create a consumer::

    class Consumer(AbstractConsumer):
        def run(msg: Message):
            print('Received a message: {}'.format(msg.body))
            msg.ack()

    consumer = Consumer(ch, 'test.q')
    consumer.declare()

    # wait for events, which will receive delivered messages and call any consumer callbacks
    while True:
        conn.drain_events(timeout=None)



Notes
=====

Any AMQP 0.9.1-compliant server is supported, but RabbitMQ is our primary target. Apache Qpid is
confirmed to work, but only with "anonymous" authentication. A CRAM-MD5 auth mechanism is currently
being developed and will be released shortly.


Features
========

* Draining events from multiple channels ``Connection.drain_events()``
* SSL is fully supported, it is highly recommended to use SSL when connecting to servers over the
  Internet.
* Support for timeouts
* Support for manual and automatic heartbeats
* Fully thread-safe

Supports RabbitMQ extensions:

* Publisher confirms: enable with ``Channel.confirm_select()``, then use
  ``Channel.basic_publish_confirm``
* Exchange to exchange bindings: ``Channel.exchange_bind()`` and ``Channel.exchange_unbind()``
* Consumer cancel notifications: by default a cancel results in ``ChannelError`` being raised,
  but not if an ``on_cancel`` callback is passed to ``basic_consume``


Testing
=======

amqpy uses the excellent **tox** and **pytest** frameworks. To run all tests, simply install a local
RabbitMQ server. No additional configuration is necessary for RabbitMQ. Then run in the project
root::

    pip install pytest
    py.test


Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _Semantic versioning: http://semver.org
