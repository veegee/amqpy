Python 3 AMQP 0.9.1 client library
=====================================

* Version: 0.7.4
* Web: http://amqpy.readthedocs.org/
* Download: http://pypi.python.org/pypi/amqpy/
* Source: http://github.com/veegee/amqpy
* Keywords: amqp, rabbitmq, qpid


About
=====

amqpy is a pure-Python AMQP 0.9.1 client library for Python >= 3.2.0 (including PyPy3) with a focus on:

* stability
* well-tested and thoroughly documented code
* clean, correct design
* 100% compliance with the AMQP protocol specification

This library is actively maintained. Please submit issues and pull requests. Bugs will be fixed immediately.

The current API is not final, but will progressively get more stable as version 1.0.0 is approached.

This library is NOT Python 2 compatible, and will never support Python 2. If you are using Python 2, you should upgrade
to Python 3 >= 3.2.0. However, this library aims to maintain backwards compatibility for all Python 3 >= 3.2.0.


Features
========

* Draining events from multiple channels (`Connection.drain_events()`)
* SSL is fully supported, it is highly recommended to use SSL when connecting to servers over the Internet.
* Support for timeouts
* Support for heartbeats (client must manually send heartbeats)
* Fully thread-safe

Supports RabbitMQ extensions:

* Consumer Cancel Notifications: by default a cancel results in `ChannelError` being raised, but not if a `on_cancel`
  callback is passed to `basic_consume`
* Publisher confirms: enable with `Channel.confirm_select()`, then use Channel.basic_publish_confirm
* Exchange to exchange bindings: `Channel.exchange_bind()` and `Channel.exchange_unbind()`


To do (goals for version 1.0.0)
===============================

* Add support for channel-level event draining, so that multiple threads can block while waiting for events on their
  own channels.
* Add support for multiple authentication mechanisms such as CRAM-MD5 for Apache Qpid
* Add support for automatic heartbeats in a separate thread
* [Kombu](https://github.com/celery/kombu) compatibility

* Add `.json()` method to `Message` for convenience
* Add icons to Sphinx doc

# Tests

* Test for strict protocol compliance and behaviour
* Test for thread safety, use PyTest monkey patching facilities to patch
  `socket` to slow down reads and writes

# Low priority

* Asynchronous operation, Python 3.4 asyncio support
