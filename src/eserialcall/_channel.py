from __future__ import annotations

__all__ = ["Channel", "ESERIALCALL_CHANNEL"]

# python
from typing import Final


class Channel:
    __slots__ = ("_ordered", "_guaranteed", "_name")

    def __init__(self, *, ordered: bool, guaranteed: bool, name: str):
        self._ordered = ordered
        self._guaranteed = guaranteed
        self._name = name

    @property
    def ordered(self) -> bool:
        return self._ordered

    @property
    def guaranteed(self) -> bool:
        return self._guaranteed

    @property
    def name(self) -> str:
        return self._name


ESERIALCALL_CHANNEL: Final = Channel(ordered=True, guaranteed=True, name="eserialcall")
