# eevent
from eserialcall import Schema
from eserialcall import Transport
from eserialcall import connect

# pytest
import pytest

# python
import asyncio
import logging
from unittest.mock import MagicMock


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


def test_add_remove_peer(event_loop, transport1, transport2, schema1, schema2):
    peer1 = MagicMock()
    peer2 = MagicMock()

    transport1.other_peer[peer1] = peer2
    transport2.other_peer[peer2] = peer1

    t1 = event_loop.create_task(asyncio.wait_for(transport1.add_peer(peer1), timeout=1))
    t2 = event_loop.create_task(asyncio.wait_for(transport2.add_peer(peer2), timeout=1))
    event_loop.run_until_complete(asyncio.wait((t1, t2), return_when=asyncio.ALL_COMPLETED))

    assert schema1._peer_connections[peer1].ingress_ready.is_set()
    assert schema1._peer_connections[peer1].ingress_map == {}
    assert schema1._peer_connections[peer1].egress_ready.is_set()

    assert schema2._peer_connections[peer2].ingress_ready.is_set()
    assert schema2._peer_connections[peer2].ingress_map == {}
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
        (b"{}", "ignoring corrupt payload: egress_map not found"),
        (b'{"egress_map": 1}', "ignoring corrupt payload: egress_map is not a dictionary"),
        (b'{"egress_map": 1.1}', "ignoring corrupt payload: egress_map is not a dictionary"),
        (b'{"egress_map": []}', "ignoring corrupt payload: egress_map is not a dictionary"),
        (b'{"egress_map": null}', "ignoring corrupt payload: egress_map is not a dictionary"),
        (b'{"egress_map": "hello"}', "ignoring corrupt payload: egress_map is not a dictionary"),
        (
            b'{"egress_map": {"hello": ""}}',
            "ignoring corrupt payload: egress_map value is not an integer",
        ),
        (
            b'{"egress_map": {"hello": null}}',
            "ignoring corrupt payload: egress_map value is not an integer",
        ),
        (
            b'{"egress_map": {"hello": 1.1}}',
            "ignoring corrupt payload: egress_map value is not an integer",
        ),
        (
            b'{"egress_map": {"hello": false}}',
            "ignoring corrupt payload: egress_map value is not an integer",
        ),
        (
            b'{"egress_map": {"hello": []}}',
            "ignoring corrupt payload: egress_map value is not an integer",
        ),
        (
            b'{"egress_map": {"hello": {}}}',
            "ignoring corrupt payload: egress_map value is not an integer",
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
    assert schema1._peer_connections[peer1].ingress_map == {}
    assert not schema1._peer_connections[peer1].egress_ready.is_set()

    assert not schema2._peer_connections[peer2].ingress_ready.is_set()
    assert schema2._peer_connections[peer2].ingress_map == {}
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
