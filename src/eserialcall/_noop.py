from __future__ import annotations

__all__ = ["NoOpTransport"]

# eevent
from ._channel import Channel
from ._rpc import Peer
from ._rpc import Transport

# python
from typing import Sequence


class NoOpTransport(Transport):
    async def send(
        self, channel: Channel | None, payload: bytes, peers: Sequence[Peer] | None
    ) -> None:
        pass

    async def receive(self, peer: Peer, payload: bytes) -> None:
        pass

    async def add_peer(self, peer: Peer) -> None:
        pass

    async def remove_peer(self, peer: Peer) -> None:
        pass
