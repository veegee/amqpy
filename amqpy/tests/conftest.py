import pytest

from .. import Connection


def get_server_props(conn):
    """

    :param conn: connection object
    :type conn: amqpy.Connection
    :return: tuple(server product name, version)
    :rtype: tuple(str, tuple(int...))
    """
    product = conn.server_properties['product']
    version = conn.server_properties['version'].split('.')
    version = tuple([int(i) for i in version])

    return product, version


@pytest.fixture(scope='function')
def conn(request):
    c = Connection(heartbeat=10)

    def fin():
        c.close()

    request.addfinalizer(fin)
    return c


@pytest.fixture(scope='function')
def ch(request, conn):
    chan = conn.channel()

    def fin():
        chan.close()

    request.addfinalizer(fin)
    return chan
