import uuid

import pytest

from .. import Connection


def get_server_props(cxn):
    """Parse server properties from `cxn`

    An example result::

        ('RabbitMQ', (3, 3, 5))

    :param cxn: connection object
    :type cxn: amqpy.Connection
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
    queue_name = 'test_queue_{}'.format(uuid.uuid4())
    return queue_name


@pytest.fixture(scope='function')
def rand_exch():
    """Generate a random exchange name

    :return: random exchange name
    :rtype: str
    """
    exch_name = 'test_exchange_{}'.format(uuid.uuid4())
    return exch_name


@pytest.fixture(scope='function')
def rand_rk():
    """Generate a random routing key

    :return: random routing key
    :rtype: str
    """
    routing_key = 'test_routing_key_{}'.format(uuid.uuid4())
    return routing_key
