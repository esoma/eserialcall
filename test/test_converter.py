# eevent
from eserialcall import RpcConverter
from eserialcall._converter import get_rpc_converter

# pytest
import pytest

# python
import json
from types import NoneType
from typing import get_args as get_type_args


@pytest.mark.parametrize(
    "type, value",
    [
        (int, 1),
        (str, "asdfsf"),
        (float, 1.0),
        (bool, False),
        (NoneType, None),
    ],
)
def test_simple_type_converter(type, value):
    converter = get_rpc_converter(type)
    assert issubclass(converter, RpcConverter)

    serialized = converter.rpc_serialize(value)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) == value

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_serialize(object())
    assert str(excinfo.value) == str(type)

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_deserialize(object())
    assert str(excinfo.value) == str(type)


@pytest.mark.parametrize(
    "value_type, value",
    [
        (int, [1, 2, 3]),
        (str, ["a", "b", "c"]),
        (float, [1.0, 2.0, 3.0]),
        (bool, [False, True]),
        (NoneType, [None]),
    ],
)
def test_list_type_converter(value_type, value):
    type = list[value_type]

    converter = get_rpc_converter(type)
    assert issubclass(converter, RpcConverter)

    serialized = converter.rpc_serialize(value)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) == value

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_serialize(object())
    assert str(excinfo.value) == str(list)

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_deserialize(object())
    assert str(excinfo.value) == str(list)


@pytest.mark.parametrize(
    "key_type, key_values",
    [
        (int, [1, 2, 3]),
        (str, ["a", "b", "c"]),
        (float, [1.0, 2.0, 3.0]),
        (bool, [False, True]),
        (NoneType, [None]),
    ],
)
@pytest.mark.parametrize(
    "value_type, value_values",
    [
        (int, [1, 2, 3]),
        (str, ["a", "b", "c"]),
        (float, [1.0, 2.0, 3.0]),
        (bool, [False, True]),
        (NoneType, [None]),
    ],
)
def test_dict_type_converter(key_type, key_values, value_type, value_values):
    type = dict[key_type, value_type]
    value = {k: v for k, v in zip(key_values, value_values)}

    converter = get_rpc_converter(type)
    assert issubclass(converter, RpcConverter)

    serialized = converter.rpc_serialize(value)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) == value

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_serialize(object())
    assert str(excinfo.value) == str(dict)

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_deserialize(object())
    assert str(excinfo.value) == str(dict)


def test_custom_converter():
    class CustomConverter(RpcConverter):
        pass

    converter = get_rpc_converter(CustomConverter)
    assert converter is CustomConverter

    class SubclassConverter(CustomConverter):
        pass

    converter = get_rpc_converter(SubclassConverter)
    assert converter is SubclassConverter


def test_no_converter():
    with pytest.raises(TypeError) as excinfo:
        get_rpc_converter(object)
    assert str(excinfo.value) == f"unable to get rpc converter for {object}"


@pytest.mark.parametrize(
    "type, value",
    [
        (int | None, 1),
        (int | None, None),
        (int | str | float, 100),
        (int | str | float, "100"),
        (int | str | float, 100.0),
    ],
)
def test_union_converter(type, value):
    types = get_type_args(type)

    converter = get_rpc_converter(type)
    assert issubclass(converter, RpcConverter)

    serialized = converter.rpc_serialize(value)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) == value

    expected_error = ", ".join(str(t) for t in types)

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_serialize(object())
    assert str(excinfo.value) == expected_error

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_deserialize(object())
    assert str(excinfo.value) == expected_error

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_serialize(object())
    assert str(excinfo.value) == expected_error

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_deserialize(["", object()])
    assert str(excinfo.value) == expected_error

    with pytest.raises(TypeError) as excinfo:
        converter.rpc_deserialize(["", object(), ""])
    assert str(excinfo.value) == expected_error


def test_union_converter_custom():
    class X(RpcConverter):
        @classmethod
        def rpc_serialize(cls, value):
            if value is x:
                return 1
            return None

        @classmethod
        def rpc_deserialize(cls, value):
            if value == 1:
                return x
            return None

    x = X()

    class Y(RpcConverter):
        @classmethod
        def rpc_serialize(cls, value):
            if value is y:
                return 1
            return None

        @classmethod
        def rpc_deserialize(cls, value):
            if value == 1:
                return y
            return None

    y = Y()

    converter = get_rpc_converter(X | Y)

    serialized = converter.rpc_serialize(x)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is x

    serialized = converter.rpc_serialize(y)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is y


def test_union_converter_subclass():
    class X(RpcConverter):
        map = {}

        def __init__(self):
            self.id = len(X.map)
            X.map[self.id] = self

        @classmethod
        def rpc_serialize(cls, value):
            return value.id

        @classmethod
        def rpc_deserialize(cls, value):
            return X.map[value]

    class Y(RpcConverter):
        map = {}

        def __init__(self):
            self.id = len(Y.map)
            Y.map[self.id] = self

        @classmethod
        def rpc_serialize(cls, value):
            return value.id

        @classmethod
        def rpc_deserialize(cls, value):
            return Y.map[value]

    class XX(X):
        pass

    xx = XX()

    class YY(Y):
        pass

    yy = YY()

    converter = get_rpc_converter(XX | YY)

    serialized = converter.rpc_serialize(xx)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is xx

    serialized = converter.rpc_serialize(yy)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is yy

    converter = get_rpc_converter(X | Y)

    serialized = converter.rpc_serialize(xx)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is xx

    serialized = converter.rpc_serialize(yy)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is yy


def test_union_converter_repeated_ids():
    class X(RpcConverter):
        ...

    class Y(RpcConverter):
        ...

    Y.__qualname__ = X.__qualname__

    with pytest.raises(TypeError) as excinfo:
        converter = get_rpc_converter(X | Y)
    assert str(excinfo.value) == (
        f"unable to get rpc converter for Union of ({X | Y}): "
        f"type identifer {X.__qualname__} is shared"
    )
