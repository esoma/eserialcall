from __future__ import annotations

__all__ = ["connect", "NoOpTransport", "Peer", "Schema", "Transport"]

# eevent
from ._channel import Channel
from ._channel import ESERIALCALL_CHANNEL

# python
from abc import ABC
from abc import abstractmethod
from asyncio import Event
from collections.abc import Hashable
from contextlib import contextmanager
import json
from logging import getLogger
from typing import Awaitable
from typing import Callable
from typing import Final
from typing import Generator
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import ParamSpec
from typing import Sequence
from typing import TypeAlias

Peer: TypeAlias = Hashable

log = getLogger("eserialcall")

_P = ParamSpec("_P")


class Transport(ABC):
    _schema: Schema | None = None

    @abstractmethod
    async def send(
        self, channel: Channel | None, payload: bytes, peers: Sequence[Peer] | None
    ) -> None:
        ...

    async def receive(self, peer: Peer, payload: bytes) -> None:
        assert self._schema is not None
        await self._schema._receive(peer, payload)

    async def add_peer(self, peer: Peer) -> None:
        assert self._schema is not None
        await self._schema._add_peer(peer)

    async def remove_peer(self, peer: Peer) -> None:
        assert self._schema is not None
        await self._schema._remove_peer(peer)


class Schema:
    _PREAMBLE_ACK: Final = b""
    _transport: Transport | None = None

    def __init__(self) -> None:
        self._peer_connections: dict[Peer, _PeerConnection] = {}
        self._rpc_ids: dict[str, _Rpc] = {}

    async def _add_peer(self, peer: Peer) -> None:
        assert self._transport is not None
        connection = _PeerConnection(self, self._rpc_ids.keys())
        self._peer_connections[peer] = connection
        await self._transport.send(ESERIALCALL_CHANNEL, connection.emit_preamble(), [peer])
        await connection.ingress_ready.wait()
        await connection.egress_ready.wait()

    async def _remove_peer(self, peer: Peer) -> None:
        del self._peer_connections[peer]

    async def _receive(self, peer: Peer, payload: bytes) -> None:
        assert self._transport is not None
        connection = self._peer_connections[peer]
        if connection.ingress_ready.is_set():
            if not connection.egress_ready.is_set() and payload == self._PREAMBLE_ACK:
                return connection.egress_ready.set()
            await self._receive_rpc(peer, payload)
        else:
            connection.load_preamble(payload)
            if connection.ingress_ready.is_set():
                await self._transport.send(ESERIALCALL_CHANNEL, self._PREAMBLE_ACK, [peer])

    async def _receive_rpc(self, peer: Peer, payload: bytes) -> None:
        connection = self._peer_connections[peer]
        assert connection.ingress_ready.is_set()

    def register(self, *, channel: Channel | None = None, local: bool = False) -> _SchemaRegister:
        return _SchemaRegister(self, channel, local)

    def _register_rpc(self, id: str, rpc: _Rpc) -> None:
        if id in self._rpc_ids:
            raise RuntimeError(f"{id} already registered")
        self._rpc_ids[id] = rpc

    """
    async def _send(
        self,
        id: _RpcRegistrationId,
        args: list[RpcSerial],
        kwargs: dict[str, RpcSerial],
        channel: RpcChannel | None,
        peers: Sequence[Peer] | None,
    ) -> None:
        for peer in peers:
            connection = self._peer_connections[peer]
            assert connection.egress_ready.is_set()
    """


class _PeerConnection:
    ingress_rpcs: Mapping[int, str]
    egress_rpcs: Mapping[int, str]

    def __init__(self, schema: Schema, rpc_ids: Iterable[str]):
        self.ingress_ready = Event()
        self.egress_ready = Event()
        self.ingress_rpcs = {}
        self.egress_rpcs = dict(enumerate(rpc_ids))

    def emit_preamble(self) -> bytes:
        return json.dumps(
            {"rpcs": {v: k for k, v in self.egress_rpcs.items()}}, separators=(",", ":")
        ).encode("utf-8")

    def load_preamble(self, payload: bytes) -> None:
        assert not self.ingress_ready.is_set()

        try:
            info = json.loads(payload)
        except json.JSONDecodeError:
            log.warning(f"ignoring corrupt payload: not json")
            return
        if not isinstance(info, dict):
            log.warning(f"ignoring corrupt payload: is not a dictionary")
            return

        try:
            rpcs = info["rpcs"]
        except KeyError:
            log.warning(f"ignoring corrupt payload: rpcs not found")
            return
        if not isinstance(rpcs, dict):
            log.warning(f"ignoring corrupt payload: rpcs is not a dictionary")
            return
        for k, v in rpcs.items():
            if type(v) is not int:
                log.warning(f"ignoring corrupt payload: rpcs value is not an integer")
                return

        self.ingress_rpcs = {v: k for k, v in rpcs.items()}
        self.ingress_ready.set()


@contextmanager
def connect(transport: Transport, schema: Schema) -> Generator[None, None, None]:
    schema._transport = transport
    transport._schema = schema
    yield
    schema._transport = None
    transport._schema = None


class _SchemaRegister:
    def __init__(self, schema: Schema, channel: Channel | None, local: bool) -> None:
        self._schema = schema
        self._channel = channel
        self._local = local

    def __call__(self, callback: Callable[_P, Awaitable[None]]) -> Callable[_P, Awaitable[None]]:
        return _Rpc(self._schema, callback, self._channel, self._local)  # type: ignore


class _Rpc(Generic[_P]):
    def __init__(
        self,
        schema: Schema,
        callback: Callable[_P, Awaitable[None]],
        channel: Channel | None,
        local: bool,
    ):
        self._id = callback.__qualname__
        self._callback = callback
        self._channel = channel
        self._local = local

        self._schema = schema
        schema._register_rpc(self._id, self)


"""
class Rpc:
    def __init__(self, transport: RpcTransport) -> None:
        self._transport = transport
        self._registrations: dict[_RpcRegistrationId, _RpcRegistration] = {}

    def _register(self, registration: _RpcRegistration, id: _RpcRegistrationId) -> None:
        if id in self._registrations:
            raise ValueError(f"{id} already registered")
        self._registrations[id] = registration

    async def _send(
        self,
        id: _RpcRegistrationId,
        args: list[RpcSerial],
        kwargs: dict[str, RpcSerial],
        channel: RpcChannel | None,
        peers: Sequence[Peer] | None,
    ) -> None:
        payload = json.dumps([id, args, kwargs], separators=(",", ":")).encode("utf-8")
        await self._transport.send_rpc(channel, payload, peers)

    async def _receive(self, peer: Peer, payload: bytes) -> None:
        try:
            id, args, kwargs = json.loads(payload)
        except json.JSONDecodeError:
            log.warning(f"ignoring corrupt payload: not json")
            return
        except (ValueError, TypeError):
            log.warning(f"ignoring corrupt payload: invalid schema")
            return
        if not isinstance(args, list):
            log.warning(f"ignoring corrupt payload: args is not list")
            return
        if not isinstance(kwargs, dict):
            log.warning(f"ignoring corrupt payload: kwargs is not dict")
            return
        try:
            registration = self._registrations[id]
        except KeyError:
            log.warning(f"ignoring rpc with unexpected id: {id}")
            return
        _rpc_peer.set(peer)
        await registration._call(args, kwargs)

    def __call__(self, *, channel: RpcChannel | None = None, local: bool = False) -> _Register:
        return _Register(channel, local)

    @property
    def peer(self) -> Peer:
        return _rpc_peer.get()

    @property
    def transport(self) -> RpcTransport:
        return self._transport

    @transport.setter
    def transport(self, transport: RpcTransport) -> None:
        self._transport = transport
"""
