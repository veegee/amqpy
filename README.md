Python 3 AMQP 0.9.1 client library
==================================

* Version: 0.3.1
* Web: http://amqpy.readthedocs.org/
* Download: http://pypi.python.org/pypi/amqpy/
* Source: http://github.com/veegee/amqpy
* Keywords: amqp, rabbitmq

About
=====

amqpy is an AMQP library for Python >= 3.2.0 (including PyPy3) with a focus on
stability, well-tested and documented code, and clean, correct design. This
library is actively maintained. Please submit issues and pull requests.

The current API is not final, but will get progressively more stable as version
1.0.0 is approached.

Forked from the Celery py-amqp 1.4.6 sources. Note that this library is NOT
Python 2 compatible.


Features
========

* Draining events from multiple channels (`Connection.drain_events`)
* Support for timeouts
* Support for heartbeats (client must manually send heartbeats)
* Thread-safe: all frame reads and writes acquire and release a lock
* Supports RabbitMQ extensions:
    * Consumer Cancel Notifications
        * by default a cancel results in `ChannelError` being raised
        * but not if a `on_cancel` callback is passed to `basic_consume`
    * Publisher confirms
        * `Channel.confirm_select()` enables publisher confirms
        * `Channel.events['basic_ack'].append(my_callback)` adds a callback to
          be called when a message is confirmed. This callback is then called
          with the signature `(delivery_tag, multiple)`
    * Exchange to exchange bindings: `exchange_bind` / `exchange_unbind`
        * `Channel.confirm_select()` enables publisher confirms
        * `Channel.events['basic_ack'].append(my_callback)` adds a callback to
          be called when a message is confirmed. This callback is then called
          with the signature `(delivery_tag, multiple)`

To do
=====

* Completely document all functions and create Sphinx docs
* Asynchronous operation
* Python 3.4 asyncio support
* Automatic heartbeats as long as the amqpy `Connection` is alive
