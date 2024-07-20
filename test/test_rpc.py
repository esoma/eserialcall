# eevent
from eserialcall import Channel
from eserialcall import Schema
from eserialcall import Transport
from eserialcall import connect
from eserialcall._rpc import _Rpc

# pytest
import pytest

# python
import asyncio
import logging
from unittest.mock import MagicMock

ordered_guaranteed_channel = Channel(ordered=True, guaranteed=True, name="test")


@pytest.fixture
def transports_and_schemas(event_loop):
    class TestTransport(Transport):
        other = None
        other_peer = {}
        payload_override = None

        async def send(self, channel, payload, peers):
            if self.payload_override is not None:
                payload = self.payload_override
            for peer in peers:
                event_loop.create_task(self.other.receive(self.other_peer[peer], payload))

    transport1 = TestTransport()
    transport2 = TestTransport()
    transport1.other = transport2
    transport2.other = transport1

    schema1 = Schema()
    schema2 = Schema()

    with connect(transport1, schema1), connect(transport2, schema2):
        yield transport1, schema1, transport2, schema2


@pytest.fixture
def transport1(transports_and_schemas):
    return transports_and_schemas[0]


@pytest.fixture
def schema1(transports_and_schemas):
    return transports_and_schemas[1]


@pytest.fixture
def transport2(transports_and_schemas):
    return transports_and_schemas[2]


@pytest.fixture
def schema2(transports_and_schemas):
    return transports_and_schemas[3]


@pytest.mark.parametrize("rpc_ids", [0, 10])
def test_add_remove_peer(event_loop, transport1, transport2, schema1, schema2, rpc_ids):
    peer1 = MagicMock()
    peer2 = MagicMock()

    schema1._rpc_ids = {str(i): None for i in range(rpc_ids)}
    schema2._rpc_ids = {str(i): None for i in reversed(range(rpc_ids))}

    transport1.other_peer[peer1] = peer2
    transport2.other_peer[peer2] = peer1

    t1 = event_loop.create_task(asyncio.wait_for(transport1.add_peer(peer1), timeout=1))
    t2 = event_loop.create_task(asyncio.wait_for(transport2.add_peer(peer2), timeout=1))
    event_loop.run_until_complete(asyncio.wait((t1, t2), return_when=asyncio.ALL_COMPLETED))

    assert schema1._peer_connections[peer1].ingress_ready.is_set()
    assert schema1._peer_connections[peer1].ingress_rpcs == dict(
        enumerate(schema2._rpc_ids.keys())
    )
    assert schema1._peer_connections[peer1].egress_rpcs == dict(enumerate(schema1._rpc_ids.keys()))
    assert schema1._peer_connections[peer1].egress_ready.is_set()

    assert schema2._peer_connections[peer2].ingress_ready.is_set()
    assert schema2._peer_connections[peer2].ingress_rpcs == dict(
        enumerate(schema1._rpc_ids.keys())
    )
    assert schema2._peer_connections[peer2].egress_rpcs == dict(enumerate(schema2._rpc_ids.keys()))
    assert schema2._peer_connections[peer2].egress_ready.is_set()

    t1 = event_loop.create_task(asyncio.wait_for(transport1.remove_peer(peer1), timeout=1))
    t2 = event_loop.create_task(asyncio.wait_for(transport2.remove_peer(peer2), timeout=1))
    event_loop.run_until_complete(asyncio.wait((t1, t2), return_when=asyncio.ALL_COMPLETED))

    assert peer1 not in schema1._peer_connections
    assert peer2 not in schema2._peer_connections


@pytest.mark.parametrize(
    "bad_preamble, expected_warning",
    [
        (b"", "ignoring corrupt payload: not json"),
        (b"1", "ignoring corrupt payload: is not a dictionary"),
        (b"false", "ignoring corrupt payload: is not a dictionary"),
        (b"1.1", "ignoring corrupt payload: is not a dictionary"),
        (b"[]", "ignoring corrupt payload: is not a dictionary"),
        (b"null", "ignoring corrupt payload: is not a dictionary"),
        (b'"hello"', "ignoring corrupt payload: is not a dictionary"),
        (b"{}", "ignoring corrupt payload: rpcs not found"),
        (b'{"rpcs": 1}', "ignoring corrupt payload: rpcs is not a dictionary"),
        (b'{"rpcs": 1.1}', "ignoring corrupt payload: rpcs is not a dictionary"),
        (b'{"rpcs": []}', "ignoring corrupt payload: rpcs is not a dictionary"),
        (b'{"rpcs": null}', "ignoring corrupt payload: rpcs is not a dictionary"),
        (b'{"rpcs": "hello"}', "ignoring corrupt payload: rpcs is not a dictionary"),
        (
            b'{"rpcs": {"hello": ""}}',
            "ignoring corrupt payload: rpcs value is not an integer",
        ),
        (
            b'{"rpcs": {"hello": null}}',
            "ignoring corrupt payload: rpcs value is not an integer",
        ),
        (
            b'{"rpcs": {"hello": 1.1}}',
            "ignoring corrupt payload: rpcs value is not an integer",
        ),
        (
            b'{"rpcs": {"hello": false}}',
            "ignoring corrupt payload: rpcs value is not an integer",
        ),
        (
            b'{"rpcs": {"hello": []}}',
            "ignoring corrupt payload: rpcs value is not an integer",
        ),
        (
            b'{"rpcs": {"hello": {}}}',
            "ignoring corrupt payload: rpcs value is not an integer",
        ),
    ],
)
def test_add_peer_bad_preamble(
    caplog, event_loop, transport1, transport2, schema1, schema2, bad_preamble, expected_warning
):
    peer1 = MagicMock()
    peer2 = MagicMock()

    transport1.other_peer[peer1] = peer2
    transport1.other_peer[peer2] = peer1

    transport1.payload_override = bad_preamble

    t1 = event_loop.create_task(asyncio.wait_for(transport1.add_peer(peer1), timeout=1))
    t2 = event_loop.create_task(asyncio.wait_for(transport2.add_peer(peer2), timeout=1))
    event_loop.run_until_complete(asyncio.wait((t1, t2), return_when=asyncio.ALL_COMPLETED))

    assert schema1._peer_connections[peer1].ingress_ready.is_set()
    assert schema1._peer_connections[peer1].ingress_rpcs == {}
    assert schema1._peer_connections[peer1].egress_rpcs == {}
    assert not schema1._peer_connections[peer1].egress_ready.is_set()

    assert not schema2._peer_connections[peer2].ingress_ready.is_set()
    assert schema2._peer_connections[peer2].ingress_rpcs == {}
    assert schema2._peer_connections[peer2].egress_rpcs == {}
    assert not schema2._peer_connections[peer2].egress_ready.is_set()

    assert caplog.record_tuples == [
        ("eserialcall", logging.WARNING, expected_warning),
        ("eserialcall", logging.WARNING, expected_warning),
    ]

    t1 = event_loop.create_task(asyncio.wait_for(transport1.remove_peer(peer1), timeout=1))
    t2 = event_loop.create_task(asyncio.wait_for(transport2.remove_peer(peer2), timeout=1))
    event_loop.run_until_complete(asyncio.wait((t1, t2), return_when=asyncio.ALL_COMPLETED))

    assert peer1 not in schema1._peer_connections
    assert peer2 not in schema2._peer_connections


def test_register(schema1, schema2):
    @schema1.register()
    def func():
        pass

    assert schema1._rpc_ids[func._id] is func
    assert not schema2._rpc_ids


@pytest.mark.parametrize(
    "rpc_kwargs, channel, local",
    [
        ({}, None, False),
        ({"channel": None}, None, False),
        ({"channel": ordered_guaranteed_channel}, ordered_guaranteed_channel, False),
        ({"local": False}, None, False),
        ({"local": True}, None, True),
    ],
)
def test_register_func(schema1, rpc_kwargs, channel, local):
    def _():
        pass

    registered = schema1.register(**rpc_kwargs)(_)
    assert isinstance(registered, _Rpc)
    assert registered._id == _.__qualname__
    assert registered._callback is _
    assert registered._channel is channel
    assert registered._local == local

    with pytest.raises(RuntimeError) as excinfo:
        schema1.register()(_)
    assert str(excinfo.value) == f"{_.__qualname__} already registered"
