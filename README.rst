Python 3 AMQP 0.9.1 client library
==================================

:Version: 0.8.10
:Web: http://amqpy.readthedocs.org/
:Download: http://pypi.python.org/pypi/amqpy/
:Source: http://github.com/veegee/amqpy
:Keywords: amqp, rabbitmq, qpid


About
=====

amqpy is a pure-Python AMQP 0.9.1 client library for Python >= 3.2.0 (including
PyPy3) with a focus on:

- stability and reliability
- well-tested and thoroughly documented code
- clean, correct design
- 100% compliance with the AMQP 0.9.1 protocol specification

It has very good performance, as AMQP 0.9.1 is a very efficient binary protocol,
but does not sacrifice clean design and testability to save a few extra CPU
cycles.

This library is actively maintained and has a zero bug policy. Please submit
issues and pull requests, and bugs will be fixed immediately.

The current API is not final, but will progressively get more stable as version
1.0.0 is approached.


Guarantees
----------

This library makes the following guarantees:

- `Semantic versioning`_ is strictly followed
- Compatible with Python >= 3.2.0 and PyPy3 >= 2.3.1 (Python 3.2.5)
- AMQP 0.9.1 compliant


Features
========

- Draining events from multiple channels: ``Connection.drain_events()``
- SSL is fully supported, it is highly recommended to use SSL when connecting to
  servers over the Internet.
- Support for timeouts
- Support for manual and automatic heartbeats
- Fully thread-safe

Supports RabbitMQ extensions:

- Publisher confirms: enable with ``Channel.confirm_select()``, then use
  ``Channel.basic_publish_confirm``
- Exchange to exchange bindings: ``Channel.exchange_bind()`` and
  ``Channel.exchange_unbind()``
- Consumer Cancel Notifications: by default a cancel results in ``ChannelError``
  being raised, but not if a ``on_cancel`` callback is passed to
  ``basic_consume``


To do (goals for  v1.0.0)
=========================

- Add support for multiple authentication mechanisms such as CRAM-MD5 for Apache
  Qpid
- `Kombu`_ compatibility
- Add examples directory and update Sphinx docs with improved examples


Tests
-----

- Test with SSL (parametrize the connection fixture to use SSL)


Low priority (goals for v1.x)
-----------------------------

- Asynchronous operation, Python 3.4 asyncio support
- Add support for channel-level event draining, so that multiple threads can
  block while waiting for events on their own channels.


.. _Kombu: https://github.com/celery/kombu
.. _Semantic versioning: http://semver.org
