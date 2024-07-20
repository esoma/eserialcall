# eevent
from eserialcall import NoOpTransport
from eserialcall import Schema
from eserialcall import connect


def test_noop(event_loop):
    noop = NoOpTransport()
    schema = Schema()
    with connect(noop, schema):
        t = event_loop.create_task(noop.add_peer(None))
        event_loop.run_until_complete(t)
        assert not schema._peer_connections

        t = event_loop.create_task(noop.remove_peer(None))
        event_loop.run_until_complete(t)
        assert not schema._peer_connections

        t = event_loop.create_task(noop.receive(None, None))
        event_loop.run_until_complete(t)

        t = event_loop.create_task(noop.send(None, None, None))
        event_loop.run_until_complete(t)
