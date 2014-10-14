import gc

from .. import Channel


class TestConnection:
    def test_create_channel(self, conn):
        ch = conn.channel(1)
        assert isinstance(ch, Channel)
        assert ch.channel_id == 1

        ch2 = conn.channel()
        assert ch2.channel_id != 1

        ch.close()
        ch2.close()

    def test_close(self, conn):
        """Make sure we've broken various references when closing channels and connections, to help
        with GC
        """
        # create a channel and make sure it's linked as we'd expect
        ch = conn.channel()
        assert 1 in conn.channels
        assert ch.connection == conn
        assert ch.is_open is True

        # close the channel and make sure the references are broken that we expect
        ch.close()
        assert ch.connection is None
        assert 1 not in conn.channels
        assert ch.callbacks == {}
        assert ch.is_open is False

        # close the connection and make sure the references we expect are gone
        conn.close()
        assert conn.connection is None
        assert conn.channels is None

    def test_gc_closed(self, conn):
        """Make sure we've broken various references when closing channels and connections, to help
        with GC
        """
        unreachable_before = len(gc.garbage)
        # create a channel and make sure it's linked as we'd expect
        conn.channel()
        assert 1 in conn.channels

        # close the connection and make sure the references we expect are gone.
        conn.close()

        gc.collect()
        gc.collect()
        gc.collect()
        assert unreachable_before == len(gc.garbage)

    def test_gc_forget(self, conn):
        """Make sure the connection gets gc'ed when there is no more references to it
        """
        unreachable_before = len(gc.garbage)

        ch = conn.channel()
        assert 1 in conn.channels

        del ch

        gc.collect()
        gc.collect()
        gc.collect()
        assert unreachable_before == len(gc.garbage)
