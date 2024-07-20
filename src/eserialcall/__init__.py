from __future__ import annotations

__all__ = [
    "Peer",
    "rpc",
    "RpcChannel",
    "RpcConverter",
    "RpcSerial",
    "RpcTransport",
    "NoOpRpcTransport",
]


# python
from abc import ABC
from abc import abstractmethod
from contextvars import ContextVar
from inspect import Parameter
from inspect import signature
import json
from logging import getLogger
from types import NoneType
from types import UnionType
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Final
from typing import Generic
from typing import Hashable
from typing import Mapping
from typing import NewType
from typing import NoReturn
from typing import ParamSpec
from typing import Self
from typing import Sequence
from typing import TypeAlias
from typing import get_args as get_type_args
from typing import get_type_hints

Peer: TypeAlias = Hashable

RpcSerial: TypeAlias = bool | int | float | str | dict | list | None

_RpcRegistrationId = NewType("_RpcRegistrationId", str)
_P = ParamSpec("_P")


log = getLogger("doorlord.rpc")


class RpcTransport(ABC):
    @abstractmethod
    async def send_rpc(
        self, channel: RpcChannel | None, payload: bytes, peers: Sequence[Peer] | None
    ) -> None:
        ...

    async def receive_rpc(self, peer: Peer, payload: bytes) -> None:
        await rpc._receive(peer, payload)


class NoOpRpcTransport(RpcTransport):
    async def send_rpc(
        self, channel: RpcChannel | None, payload: bytes, peers: Sequence[Peer] | None
    ) -> None:
        pass

    async def receive_rpc(self, peer: Peer, payload: bytes) -> None:
        pass


_rpc_peer: ContextVar[Peer] = ContextVar("_rpc_peer")


class _Rpc:
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


rpc = _Rpc(NoOpRpcTransport())


class RpcChannel:
    __slots__ = ("_ordered", "_guaranteed")

    def __init__(self, *, ordered: bool, guaranteed: bool):
        self._ordered = ordered
        self._guaranteed = guaranteed

    @property
    def ordered(self) -> bool:
        return self._ordered

    @property
    def guaranteed(self) -> bool:
        return self._guaranteed


class _Register:
    def __init__(self, channel: RpcChannel | None, local: bool) -> None:
        self._channel = channel
        self._local = local

    def __call__(self, callback: Callable[_P, Awaitable[None]]) -> Callable[_P, Awaitable[None]]:
        return _RpcRegistration(callback, self._channel, self._local)


class _BoundRpcRegistration(Generic[_P]):
    def __init__(self, registration: _RpcRegistration[_P], bound: Any):
        self._registration = registration
        self.__self__ = bound

    async def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        await self._registration(self.__self__, *args, **kwargs)

    @property
    def __func__(self) -> Callable[_P, Awaitable[None]]:
        return self._registration._callback


class _RpcRegistration(Generic[_P]):
    def __init__(
        self, callback: Callable[_P, Awaitable[None]], channel: RpcChannel | None, local: bool
    ):
        self._id = _RpcRegistrationId(callback.__qualname__)
        self._callback = callback
        self._class: Any = None
        self._channel = channel
        self._local = local

        self._converters_generated = False
        self._param_converters: list[tuple[Parameter, type[RpcConverter]]] = []

        rpc._register(self, self._id)

    def __get__(self, instance: Any, cls: Any) -> _BoundRpcRegistration[_P] | Self:
        callback = self._callback.__get__(instance, cls)
        try:
            self_ = callback.__self__
        except AttributeError:
            return self
        return _BoundRpcRegistration(self, self_)

    def __set_name__(self, owner: Any, name: str) -> None:
        self._class = owner

    async def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        await self.call(args, kwargs)

    async def _call(
        self, serialized_args: list[RpcSerial], serialized_kwargs: dict[str, RpcSerial]
    ) -> None:
        try:
            args, kwargs = self._deserialize_call(serialized_args, serialized_kwargs)
        except TypeError as ex:
            log.warning(f"ignoring rpc for {self._callback} with invalid arguments: {ex}")
            return
        await self._callback(*args, **kwargs)

    async def call(
        self, args: _P.args, kwargs: _P.kwargs, *, peers: Sequence[Peer] | None = None
    ) -> None:
        serialized_args, serialized_kwargs = self._serialize_call(args, kwargs)
        await rpc._send(self._id, serialized_args, serialized_kwargs, self._channel, peers)
        if self._local:
            await self._callback(*args, **kwargs)

    def _deserialize_call(
        self, serialized_args: list[RpcSerial], serialized_kwargs: dict[str, RpcSerial]
    ) -> tuple[_P.args, _P.kwargs]:
        self._generate_converters()

        used_names: set[str] = set()
        remaining_args = list(serialized_args)
        remaining_kwargs = dict(serialized_kwargs)
        args: list[Any] = []
        kwargs: dict[str, Any] = {}

        for i, (param, converter) in enumerate(self._param_converters):
            if param.kind == Parameter.POSITIONAL_ONLY:
                try:
                    _deserialize_arg(converter, param, remaining_args, args)
                except _MissingArgument:
                    raise TypeError(f"expected argument for {param.name!r} in position {i}")
                used_names.add(param.name)
            elif param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                try:
                    if remaining_args:
                        _deserialize_arg(converter, param, remaining_args, args)
                    else:
                        _deserialize_kwarg(converter, param, remaining_kwargs, kwargs)
                except _MissingArgument:
                    raise TypeError(f"expected argument in position {i} or named {param.name!r}")
                used_names.add(param.name)
            elif param.kind == Parameter.VAR_POSITIONAL:
                try:
                    args.extend([converter.rpc_deserialize(value) for value in remaining_args])
                except TypeError as ex:
                    raise TypeError(f"expected {ex} for {param.name!r}") from None
                remaining_args.clear()
            elif param.kind == Parameter.KEYWORD_ONLY:
                try:
                    _deserialize_kwarg(converter, param, remaining_kwargs, kwargs)
                except _MissingArgument:
                    raise TypeError(f"expected keyword argument {param.name!r}")
                used_names.add(param.name)
            else:
                assert param.kind == Parameter.VAR_KEYWORD
                for key in remaining_kwargs.keys():
                    if key in used_names:
                        raise TypeError(f"got multiple values for {key!r}")
                try:
                    kwargs.update(
                        {
                            key: converter.rpc_deserialize(value)
                            for key, value in remaining_kwargs.items()
                        }
                    )
                except TypeError as ex:
                    raise TypeError(f"expected {ex} for {param.name!r}") from None
                remaining_kwargs.clear()

        if remaining_args:
            raise TypeError(f"too many positional arguments")
        if remaining_kwargs:
            raise TypeError(
                f"unexpected keyword arguments: "
                f"{', '.join(repr(k) for k in remaining_kwargs.keys())}"
            )

        return tuple(args), kwargs

    def _serialize_call(
        self, args: _P.args, kwargs: _P.kwargs
    ) -> tuple[list[RpcSerial], dict[str, RpcSerial]]:
        self._generate_converters()

        used_names: set[str] = set()
        remaining_args = list(args)
        remaining_kwargs = dict(kwargs)
        serialized_args: list[RpcSerial] = []
        serialized_kwargs: dict[str, RpcSerial] = {}

        for i, (param, converter) in enumerate(self._param_converters):
            if param.kind == Parameter.POSITIONAL_ONLY:
                try:
                    _serialize_arg(converter, param, remaining_args, serialized_args)
                except _MissingArgument:
                    raise TypeError(f"expected argument for {param.name!r} in position {i}")
                used_names.add(param.name)
            elif param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                try:
                    if remaining_args:
                        _serialize_arg(converter, param, remaining_args, serialized_args)
                    else:
                        _serialize_kwarg(converter, param, remaining_kwargs, serialized_kwargs)
                except _MissingArgument:
                    raise TypeError(f"expected argument in position {i} or named {param.name!r}")
                used_names.add(param.name)
            elif param.kind == Parameter.VAR_POSITIONAL:
                try:
                    serialized_args.extend(
                        [converter.rpc_serialize(value) for value in remaining_args]
                    )
                except TypeError as ex:
                    raise TypeError(f"expected {ex} for {param.name!r}") from None
                remaining_args.clear()
            elif param.kind == Parameter.KEYWORD_ONLY:
                try:
                    _serialize_kwarg(converter, param, remaining_kwargs, serialized_kwargs)
                except _MissingArgument:
                    raise TypeError(f"expected keyword argument {param.name!r}")
                used_names.add(param.name)
            else:
                assert param.kind == Parameter.VAR_KEYWORD
                for key in remaining_kwargs.keys():
                    if key in used_names:
                        raise TypeError(f"got multiple values for {key!r}")
                try:
                    serialized_kwargs.update(
                        {
                            key: converter.rpc_serialize(value)
                            for key, value in remaining_kwargs.items()
                        }
                    )
                except TypeError as ex:
                    raise TypeError(f"expected {ex} for {param.name!r}") from None
                remaining_kwargs.clear()

        if remaining_args:
            raise TypeError(f"too many positional arguments")
        if remaining_kwargs:
            raise TypeError(
                f"unexpected keyword arguments: "
                f"{', '.join(repr(k) for k in remaining_kwargs.keys())}"
            )

        return serialized_args, serialized_kwargs

    def _generate_converters(self) -> None:
        if self._converters_generated:
            return

        assert not self._param_converters

        sig = signature(self._callback)
        type_hints = get_type_hints(self._callback)

        for i, param in enumerate(sig.parameters.values()):
            try:
                type = type_hints[param.name]
            except KeyError:
                if i == 0 and self._class is not None:
                    type = self._class
                else:
                    raise TypeError(f"cannot determine type for {param}")
            converter = _get_converter(type)
            self._param_converters.append((param, converter))

        self._converters_generated = True


class _MissingArgument(RuntimeError):
    pass


def _deserialize_arg(
    converter: type[RpcConverter],
    param: Parameter,
    remaining_args: list[Any],
    args: list[Any],
) -> None:
    try:
        value = remaining_args.pop(0)
    except IndexError:
        raise _MissingArgument()
    try:
        arg = converter.rpc_deserialize(value)
    except TypeError as ex:
        raise TypeError(f"expected {ex} for {param.name!r}") from None
    args.append(arg)


def _deserialize_kwarg(
    converter: type[RpcConverter],
    param: Parameter,
    remaining_kwargs: dict[str, Any],
    kwargs: dict[str, Any],
) -> None:
    try:
        value = remaining_kwargs.pop(param.name)
    except KeyError:
        if param.default is Parameter.empty:
            raise _MissingArgument()
        return
    try:
        kwarg = converter.rpc_deserialize(value)
    except TypeError as ex:
        raise TypeError(f"expected {ex} for {param.name!r}") from None
    kwargs[param.name] = kwarg


def _serialize_arg(
    converter: type[RpcConverter],
    param: Parameter,
    remaining_args: list[Any],
    serialized_args: list[Any],
) -> None:
    try:
        value = remaining_args.pop(0)
    except IndexError:
        raise _MissingArgument()
    try:
        arg = converter.rpc_serialize(value)
    except TypeError as ex:
        raise TypeError(f"expected {ex} for {param.name!r}") from None
    serialized_args.append(arg)


def _serialize_kwarg(
    converter: type[RpcConverter],
    param: Parameter,
    remaining_kwargs: dict[str, Any],
    serialized_kwargs: dict[str, Any],
) -> None:
    try:
        value = remaining_kwargs.pop(param.name)
    except KeyError:
        if param.default is Parameter.empty:
            raise _MissingArgument()
        return
    try:
        kwarg = converter.rpc_serialize(value)
    except TypeError as ex:
        raise TypeError(f"expected {ex} for {param.name!r}") from None
    serialized_kwargs[param.name] = kwarg


class RpcConverter(ABC):
    @classmethod
    @abstractmethod
    def rpc_serialize(cls, value: Any) -> RpcSerial:
        ...

    @classmethod
    @abstractmethod
    def rpc_deserialize(cls, value: RpcSerial) -> Any:
        ...


class _SimpleConverter(RpcConverter):
    _type: type[RpcSerial]

    @classmethod
    def rpc_serialize(cls, value: Any) -> RpcSerial:
        if not isinstance(value, cls._type):
            raise TypeError(str(cls._type))
        return value  # type: ignore

    @classmethod
    def rpc_deserialize(cls, value: RpcSerial) -> Any:
        if not isinstance(value, cls._type):
            raise TypeError(str(cls._type))
        return value


class _IntConverter(_SimpleConverter):
    _type = int


class _FloatConverter(_SimpleConverter):
    _type = float


class _BoolConverter(_SimpleConverter):
    _type = bool


class _StrConverter(_SimpleConverter):
    _type = str


class _NoneConverter(_SimpleConverter):
    _type = NoneType


_SIMPLE_CONVERTER_MAP: Final[Mapping[Any, type[RpcConverter]]] = {
    NoneType: _NoneConverter,
    int: _IntConverter,
    float: _FloatConverter,
    bool: _BoolConverter,
    str: _StrConverter,
}


class _BaseListConverter(RpcConverter):
    _type_converter: type[RpcConverter]

    @classmethod
    def rpc_serialize(cls, value: Any) -> RpcSerial:
        if not isinstance(value, list):
            raise TypeError(str(list))
        return [cls._type_converter.rpc_serialize(i) for i in value]

    @classmethod
    def rpc_deserialize(cls, value: RpcSerial) -> Any:
        if not isinstance(value, list):
            raise TypeError(str(list))
        return [cls._type_converter.rpc_deserialize(i) for i in value]


def _get_list_converter(alias: Any) -> type[_BaseListConverter]:
    type_converter = _get_converter(alias)

    class _ListConverter(_BaseListConverter):
        _type_converter = type_converter

    return _ListConverter


class _BaseDictConverter(RpcConverter):
    _key_converter: type[RpcConverter]
    _value_converter: type[RpcConverter]

    @classmethod
    def rpc_serialize(cls, value: Any) -> RpcSerial:
        if not isinstance(value, dict):
            raise TypeError(str(dict))
        return {
            cls._key_converter.rpc_serialize(k): cls._value_converter.rpc_serialize(v)
            for k, v in value.items()
        }

    @classmethod
    def rpc_deserialize(cls, value: RpcSerial) -> Any:
        if not isinstance(value, dict):
            raise TypeError(str(dict))
        return {
            cls._key_converter.rpc_deserialize(k): cls._value_converter.rpc_deserialize(v)
            for k, v in value.items()
        }


def _get_dict_converter(key_alias: Any, value_alias: Any) -> type[_BaseDictConverter]:
    key_converter = _get_converter(key_alias)
    value_converter = _get_converter(value_alias)

    class _DictConverter(_BaseDictConverter):
        _key_converter = key_converter
        _value_converter = value_converter

    return _DictConverter


class _BaseUnionConverter(RpcConverter):
    _types: list[Any]
    _converters: dict[str, type[RpcConverter]]

    @classmethod
    def rpc_serialize(cls, value: Any) -> RpcSerial:
        for type_ in type(value).mro():
            type_id = type_.__qualname__
            try:
                converter = cls._converters[type_id]
            except KeyError:
                continue
            return [type_id, converter.rpc_serialize(value)]
        cls._raise_invalid_type_error()

    @classmethod
    def rpc_deserialize(cls, value: RpcSerial) -> Any:
        if not isinstance(value, list):
            cls._raise_invalid_type_error()
        try:
            type_id, value = value
        except ValueError:
            cls._raise_invalid_type_error()
        try:
            converter = cls._converters[type_id]
        except KeyError:
            cls._raise_invalid_type_error()
        return converter.rpc_deserialize(value)

    @classmethod
    def _raise_invalid_type_error(cls) -> NoReturn:
        raise TypeError(", ".join(str(t) for t in cls._types))


def _get_union_converter(union: UnionType) -> type[_BaseUnionConverter]:
    converters: dict[str, type[RpcConverter]] = {}
    types: list[Any] = []
    for alias in get_type_args(union):
        try:
            type = alias.__origin__
        except AttributeError:
            type = alias
        if type.__qualname__ in converters:
            raise TypeError(
                f"unable to get rpc converter for Union of ({union}): "
                f"type identifer {type.__qualname__} is shared"
            )
        converters[type.__qualname__] = _get_converter(alias)
        types.append(type)

    class _UnionConverter(_BaseUnionConverter):
        _converters = converters
        _types = types

    return _UnionConverter


def _get_converter(alias: Any) -> type[RpcConverter]:
    if isinstance(alias, UnionType):
        return _get_union_converter(alias)

    try:
        type = alias.__origin__
    except AttributeError:
        type = alias
    if type is None:
        type = NoneType

    try:
        return _SIMPLE_CONVERTER_MAP[type]
    except KeyError:
        pass

    try:
        if issubclass(type, RpcConverter):
            return type  # type: ignore
    except TypeError:
        pass

    if type is list:
        return _get_list_converter(*get_type_args(alias))
    if type is dict:
        return _get_dict_converter(*get_type_args(alias))

    raise TypeError(f"unable to get rpc converter for {type}")
