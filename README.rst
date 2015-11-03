Python 2 & 3 AMQP 0.9.1 client library
==================================

|Version| |PyPI|

:Web: http://amqpy.readthedocs.org/
:Source: http://github.com/veegee/amqpy
:Keywords: amqp, rabbitmq, qpid


About
=====

amqpy is a pure-Python AMQP 0.9.1 client library for Python 2 >= 2.7.0 and
Python 3 >= 3.2.0 (including PyPy and PyPy3) with a focus on:

- stability and reliability
- well-tested and thoroughly documented code
- clean, correct design
- 100% compliance with the AMQP 0.9.1 protocol specification

It has very good performance, as AMQP 0.9.1 is a very efficient binary protocol,
but does not sacrifice clean design and testability to save a few extra CPU
cycles.

This library is actively maintained and has a zero bug policy. Please submit
issues and pull requests, and bugs will be fixed immediately.


Guarantees
----------

This library makes the following guarantees:

- `Semantic versioning`_ is strictly followed
- Compatible with Python >= 2.7.0 and PyPy
- Compatible with Python >= 3.2.0 and PyPy3 >= 2.3.1 (Python 3.2.5)
- AMQP 0.9.1 compliant


Features
========

- Draining events from multiple channels: ``Connection.drain_events()``
- SSL is fully supported, it is highly recommended to use SSL when connecting to
  servers over the Internet.
- Support for timeouts
- Support for manual and automatic heartbeats
- Fully thread-safe. Use one global connection and open one channel per thread.

Supports RabbitMQ extensions:

- Publisher confirms: enable with ``Channel.confirm_select()``, then use
  ``Channel.basic_publish_confirm()``
- Exchange to exchange bindings: ``Channel.exchange_bind()`` and
  ``Channel.exchange_unbind()``
- Consumer Cancel Notifications: by default a cancel results in ``ChannelError``
  being raised, but not if a ``on_cancel`` callback is passed to
  ``basic_consume``

.. _Semantic versioning: http://semver.org

.. |Version| image:: https://img.shields.io/github/tag/veegee/amqpy.svg

.. |PyPI| image:: https://img.shields.io/pypi/v/amqpy.svg
    :target: https://pypi.python.org/pypi/amqpy/
    :alt: Latest Version
