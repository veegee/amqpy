import pytest

from .. import Connection


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
