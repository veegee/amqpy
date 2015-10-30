from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type
from .. import Message


class TestChannel:
    def test_with(self, conn):
        with conn as cxn:
            assert cxn.transport is not None

            with cxn.channel(1) as ch:
                assert 1 in cxn.channels

                # do something with the channel
                ch.exchange_declare('unittest.fanout', 'fanout', auto_delete=True)

                msg = Message('unittest message',
                              content_type='text/plain',
                              application_headers={'foo': 7, 'bar': 'baz'})

                ch.basic_publish(msg, 'unittest.fanout')

            # check that the channel was closed
            assert 1 not in cxn.channels
            assert ch.is_open is False

        # check that the connection was closed
        assert cxn.transport is None
