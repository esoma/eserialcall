from __future__ import annotations

__all__ = ["RpcConverter", "get_rpc_converter"]

# python
from abc import ABC
from abc import abstractmethod
from types import NoneType
from types import UnionType
from typing import Any
from typing import NoReturn
from typing import TypeAlias
from typing import get_args as get_type_args

RpcSerial: TypeAlias = bool | int | float | str | dict | list | None


class RpcConverter(ABC):
    @classmethod
    @abstractmethod
    def rpc_serialize(cls, value: Any) -> RpcSerial:
        ...

    @classmethod
    @abstractmethod
    def rpc_deserialize(cls, value: RpcSerial) -> Any:
        ...


class _PassthroughConverter(RpcConverter):
    _map: dict[Any, type[_PassthroughConverter]] = {}
    _type: Any

    def __init_subclass__(cls, *, type: Any) -> None:
        cls._map[type] = cls
        cls._type = type

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


class _BoolConverter(_PassthroughConverter, type=bool):
    pass


class _IntConverter(_PassthroughConverter, type=int):
    pass


class _FloatConverter(_PassthroughConverter, type=float):
    pass


class _StrConverter(_PassthroughConverter, type=str):
    pass


class _NoneConverter(_PassthroughConverter, type=NoneType):
    pass


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
    type_converter = get_rpc_converter(alias)

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
    key_converter = get_rpc_converter(key_alias)
    value_converter = get_rpc_converter(value_alias)

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
        converters[type.__qualname__] = get_rpc_converter(alias)
        types.append(type)

    class _UnionConverter(_BaseUnionConverter):
        _converters = converters
        _types = types

    return _UnionConverter


def get_rpc_converter(alias: Any) -> type[RpcConverter]:
    if isinstance(alias, UnionType):
        return _get_union_converter(alias)

    try:
        type = alias.__origin__
    except AttributeError:
        type = alias
    if type is None:
        type = NoneType

    try:
        if issubclass(type, RpcConverter):
            return type  # type: ignore
    except TypeError:
        pass

    if type is list:
        return _get_list_converter(*get_type_args(alias))
    if type is dict:
        return _get_dict_converter(*get_type_args(alias))

    try:
        return _PassthroughConverter._map[type]
    except KeyError:
        pass

    raise TypeError(f"unable to get rpc converter for {type}")
