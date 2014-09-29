Python 3 AMQP >= 0.9.1 client library
=====================================

* Version: 0.4.1
* Web: http://amqpy.readthedocs.org/
* Download: http://pypi.python.org/pypi/amqpy/
* Source: http://github.com/veegee/amqpy
* Keywords: amqp, rabbitmq, python 3


About
=====

amqpy is an AMQP >= 0.9.1 library for Python >= 3.2.0 (including PyPy3) with a
focus on:

* stability
* well-tested and thoroughly documented code
* clean, correct design
* 100% compliance with the AMQP protocol specification

This library is actively maintained. Please submit issues and pull requests.
Bugs will be fixed immediately.

The current API is not final, but will progressively get more stable as version
1.0.0 is approached.

This library is NOT Python 2 compatible, and will never support Python 2. If you
are using Python 2, you should upgrade to Python 3 >= 3.2.0. However, this
library aims to maintain backwards compatibility for all Python 3 >= 3.2.0, and
all AMQP >= 0.9.1.


Features
========

* Draining events from multiple channels (`Connection.drain_events`)
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


To do (goals for version 1.0.0)
===============================

* Add support for automatic heartbeats in a separate thread
* [Kombu](https://github.com/celery/kombu) compatibility
* Publish on PyPi

# Tests

* Test for strict protocol compliance and behaviour
* Test for thread safety, use PyTest monkey patching facilities to patch
  `socket` to slow down reads and writes
*

# Documentation

* Ensure all public methods and fields have consistent and complete docstrings
* Create Sphinx documents, then publish on readthedocs

# Low priority

* Asynchronous operation, Python 3.4 asyncio support
