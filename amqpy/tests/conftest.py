from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
import uuid
import logging
import sys
import six

import pytest

from .. import Connection


class ColouredFormatter(logging.Formatter):
    RESET = '\x1B[0m'
    RED = '\x1B[31m'
    YELLOW = '\x1B[33m'
    BRGREEN = '\x1B[01;32m'  # grey in solarized for terminals

    def format(self, record, colour=False):
        message = super(ColouredFormatter, self).format(record)

        if not colour:
            return message

        level_no = record.levelno
        if level_no >= logging.CRITICAL:
            colour = self.RED
        elif level_no >= logging.ERROR:
            colour = self.RED
        elif level_no >= logging.WARNING:
            colour = self.YELLOW
        elif level_no >= logging.INFO:
            colour = self.RESET
        elif level_no >= logging.DEBUG:
            colour = self.BRGREEN
        else:
            colour = self.RESET

        message = colour + message + self.RESET

        return message


class ColouredHandler(logging.StreamHandler):
    def __init__(self, stream=sys.stdout):
        super(ColouredHandler, self).__init__(stream)
        if six.PY2:
            self.terminator = '\n'

    def format(self, record, colour=False):
        if not isinstance(self.formatter, ColouredFormatter):
            self.formatter = ColouredFormatter()

        return self.formatter.format(record, colour)

    def emit(self, record):
        stream = self.stream
        try:
            #msg = self.format(record, stream.isatty())
            msg = self.format(record, True)  # force coloured output for pytest

            stream.write(msg)
            stream.write(self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


h = ColouredHandler()

if six.PY2:
    h.formatter = ColouredFormatter('%(asctime)s %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S')
    logging.basicConfig(level=logging.DEBUG)
    logging.root.handlers[0] = h
else:
    h.formatter = ColouredFormatter('{asctime} {levelname:8} {message}', '%Y-%m-%d %H:%M:%S', '{')
    if sys.version_info <= (3, 2):
        # Python 3.2 doesn't support the `handlers` parameter for `logging.basicConfig()`
        logging.basicConfig(level=logging.DEBUG)
        logging.root.handlers[0] = h
    else:
        logging.basicConfig(level=logging.DEBUG, handlers=[h])


def get_server_props(cxn):
    """Parse server properties from `cxn`

    An example result::

        ('RabbitMQ', (3, 3, 5))

    :param cxn: connection object
    :type cxn: amqpy.connection.Connection
    :return: tuple(server product name, version)
    :rtype: tuple(str, tuple(int...))
    """
    product = cxn.server_properties['product']
    version = cxn.server_properties['version'].split('.')
    version = tuple([int(i) for i in version])
    x = cxn.server_capabilities
    assert x

    return product, version


@pytest.fixture(scope='function')
def conn(request):
    """Create a connection

    :return: Connection object
    :rtype: amqpy.Connection
    """
    c = Connection(heartbeat=10)

    def fin():
        c.close()

    request.addfinalizer(fin)
    return c


@pytest.fixture(scope='function')
def ch(request, conn):
    """Create a channel

    :return: Channel object
    :rtype: amqpy.Channel
    """
    chan = conn.channel()

    def fin():
        chan.close()

    request.addfinalizer(fin)
    return chan


@pytest.fixture(scope='function')
def rand_queue():
    """Generate a random queue name

    :return: random queue name
    :rtype: str
    """
    queue_name = 'amqpy.test.queue.{}'.format(uuid.uuid4())
    return queue_name


@pytest.fixture(scope='function')
def rand_exch():
    """Generate a random exchange name

    :return: random exchange name
    :rtype: str
    """
    exch_name = 'amqpy.test.exchange.{}'.format(uuid.uuid4())
    return exch_name


@pytest.fixture(scope='function')
def rand_rk():
    """Generate a random routing key

    :return: random routing key
    :rtype: str
    """
    routing_key = 'amqpy.test.rk.{}'.format(uuid.uuid4())
    return routing_key
