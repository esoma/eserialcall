# doorlord
# eevent
from eserialcall import NoOpRpcTransport
from eserialcall import RpcChannel
from eserialcall import RpcConverter
from eserialcall import RpcTransport
from eserialcall import _BoundRpcRegistration
from eserialcall import _Rpc
from eserialcall import _RpcRegistration
from eserialcall import _get_converter
from eserialcall import rpc

# pytest
import pytest

# python
from asyncio import sleep
import json
import logging
from textwrap import dedent
from types import NoneType
from typing import get_args as get_type_args
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

ordered_guaranteed_channel = RpcChannel(ordered=True, guaranteed=True)
mock_peer = MagicMock()


async def no_params() -> None:
    ...


async def positional_or_keyword_int(a: int) -> None:
    ...


async def positional_or_keyword_int_with_default_1(a: int = 1) -> None:
    ...


async def positional_only_int(a: int, /) -> None:
    ...


async def keyword_only_int_with_default_1(*, a: int = 1) -> None:
    ...


async def keyword_only_int_with_no_default(*, a: int) -> None:
    ...


async def var_position_int(*args: int) -> None:
    ...


async def var_keyword_int(**kwargs: int) -> None:
    ...


async def var_args_and_kwargs_int(*args: int, **kwargs: int) -> None:
    ...


async def complex_int_params(
    a: int, /, b: int, c: int = 3, *args: int, d: int, e: int = 10, **kwargs: int
) -> None:
    ...


def test_rpc():
    assert isinstance(rpc, _Rpc)
    assert isinstance(rpc._transport, NoOpRpcTransport)


def test_transport_receive(rpc, event_loop):
    class Transport(RpcTransport):
        async def send_rpc(*args, **kwargs):
            ...

    transport = Transport()
    peer = object()
    payload = object()
    with patch.object(rpc, "_receive", new_callable=AsyncMock) as rpc_receive_mock:
        event_loop.run_until_complete(transport.receive_rpc(peer, object))
    rpc_receive_mock.assert_called_once_with(peer, object)


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
def test_register_func(rpc, rpc_kwargs, channel, local):
    def _():
        pass

    registered = rpc(**rpc_kwargs)(_)
    assert isinstance(registered, _RpcRegistration)
    assert registered._id == _.__qualname__
    assert registered._callback is _
    assert registered._class is None
    assert registered._channel is channel
    assert registered._local == local

    with pytest.raises(ValueError) as excinfo:
        rpc()(_)
    assert str(excinfo.value) == f"{_.__qualname__} already registered"


def test_register_missing_type_hint(rpc, event_loop):
    async def _(x):
        pass

    registered = rpc()(_)

    with pytest.raises(TypeError) as excinfo:
        event_loop.run_until_complete(registered(None))
    assert str(excinfo.value) == "cannot determine type for x"


def test_register_missing_converter(rpc, event_loop):
    async def _(x: object):
        pass

    registered = rpc()(_)

    with pytest.raises(TypeError) as excinfo:
        event_loop.run_until_complete(registered(None))
    assert str(excinfo.value) == f"unable to get rpc converter for {object!r}"


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
def test_register_method(rpc, rpc_kwargs, channel, local):
    class X:
        def _(self):
            pass

        registered = rpc(**rpc_kwargs)(_)

    assert isinstance(X.registered, _RpcRegistration)
    assert X.registered._id == X._.__qualname__
    assert X.registered._callback is X._
    assert X.registered._class is X
    assert X.registered._channel is channel
    assert X.registered._local == local

    x = X()
    assert isinstance(x.registered, _BoundRpcRegistration)
    assert x.registered._registration is X.registered
    assert x.registered.__self__ is x
    assert x.registered.__func__ is X._

    with pytest.raises(ValueError) as excinfo:
        rpc()(X._)
    assert str(excinfo.value) == f"{X._.__qualname__} already registered"


def test_register_class_method(rpc):
    @classmethod
    def func(cls):
        ...

    class X:
        _ = rpc()(func)

    assert isinstance(X._, _BoundRpcRegistration)
    assert X._._registration._callback is func
    assert X._.__self__ is X

    x = X()
    assert isinstance(x._, _BoundRpcRegistration)
    assert x._._registration._callback is func
    assert x._.__self__ is X

    with pytest.raises(ValueError) as excinfo:
        rpc()(func)
    assert str(excinfo.value) == f"{func.__qualname__} already registered"


def test_register_static_method(rpc):
    @staticmethod
    def func(cls):
        ...

    class X:
        registered = rpc()(func)

    assert isinstance(X.registered, _RpcRegistration)
    assert X.registered._id == func.__qualname__
    assert X.registered._callback is func
    assert X.registered._class is X

    x = X()
    assert isinstance(x.registered, _RpcRegistration)
    assert x.registered._id == func.__qualname__
    assert x.registered._callback is func
    assert x.registered._class is X

    with pytest.raises(ValueError) as excinfo:
        rpc()(func)
    assert str(excinfo.value) == f"{func.__qualname__} already registered"


@pytest.mark.parametrize(
    "id, signature, args, kwargs, expected_payload",
    [
        ("empty", "", (), {}, rb'["empty",[],{}]'),
        ("intarg", "x: int", (1,), {}, rb'["intarg",[1],{}]'),
        ("intkwarg", "x: int", (), {"x": 1}, rb'["intkwarg",[],{"x":1}]'),
    ],
)
@pytest.mark.parametrize("channel", [None, ordered_guaranteed_channel])
@pytest.mark.parametrize("peers", [None, [mock_peer]])
def test_send_receive(
    rpc, event_loop, id, signature, args, kwargs, expected_payload, channel, peers
):
    rpc.transport = MagicMock()
    rpc.transport.send_rpc = AsyncMock()

    event_loop.run_until_complete(rpc._send(id, args, kwargs, channel, peers))
    rpc.transport.send_rpc.assert_called_once_with(channel, expected_payload, peers)

    expected_peer = object()

    async def callback_impl(*args, **kwargs):
        assert rpc.peer is expected_peer

    async def _():
        g = {}
        l = {}
        exec(
            dedent(
                f"""
            async def {id}({signature}) -> None:
                pass
            """
            ),
            g,
            l,
        )
        registered = rpc()(l[id])
        registered._generate_converters()

        with patch.object(registered, "_callback", side_effect=callback_impl) as mock_callback:
            await rpc._receive(expected_peer, expected_payload)
        mock_callback.assert_called_once_with(*args, **kwargs)

    event_loop.run_until_complete(_())


def test_receive_not_json(rpc, event_loop, caplog):
    async def _():
        await rpc._receive(object(), b"notjson")
        await sleep(0)

    event_loop.run_until_complete(_())
    assert caplog.record_tuples == [
        ("doorlord.rpc", logging.WARNING, f"ignoring corrupt payload: not json"),
    ]


@pytest.mark.parametrize("payload", [b"{}", b"1", b"[1,[]]", b"[1,[],{},4]", b'"sd"'])
def test_receive_bad_schema(rpc, event_loop, caplog, payload):
    async def _():
        await rpc._receive(object(), payload)
        await sleep(0)

    event_loop.run_until_complete(_())
    assert caplog.record_tuples == [
        ("doorlord.rpc", logging.WARNING, f"ignoring corrupt payload: invalid schema"),
    ]


def test_receive_unexpected_id(rpc, event_loop, caplog):
    async def _():
        await rpc._receive(object(), rb'["empty",[],{}]')
        await sleep(0)

    event_loop.run_until_complete(_())
    assert caplog.record_tuples == [
        ("doorlord.rpc", logging.WARNING, f"ignoring rpc with unexpected id: empty"),
    ]


@pytest.mark.parametrize("payload", [b"[1,1,{}]", b'"abc"', b"[1,{},{}]"])
def test_receive_bad_args_type(rpc, event_loop, caplog, payload):
    async def _():
        await rpc._receive(object(), payload)
        await sleep(0)

    event_loop.run_until_complete(_())
    assert caplog.record_tuples == [
        ("doorlord.rpc", logging.WARNING, f"ignoring corrupt payload: args is not list"),
    ]


@pytest.mark.parametrize("payload", [b"[1,[],1]", b"[1,[],[]]"])
def test_receive_bad_kwargs_type(rpc, event_loop, caplog, payload):
    async def _():
        await rpc._receive(object(), payload)
        await sleep(0)

    event_loop.run_until_complete(_())
    assert caplog.record_tuples == [
        ("doorlord.rpc", logging.WARNING, f"ignoring corrupt payload: kwargs is not dict"),
    ]


def test_call_bound_rpc_registration(event_loop):
    registration = AsyncMock()
    instance = MagicMock()

    bound = _BoundRpcRegistration(registration, instance)
    event_loop.run_until_complete(bound(1, 2, kwarg=3))

    registration.assert_called_once_with(instance, 1, 2, kwarg=3)


@pytest.mark.parametrize("args", [(), (1, 2, 3)])
@pytest.mark.parametrize("kwargs", [{}, {"one": 1, "two": 2, "three": 3}])
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
@pytest.mark.parametrize(
    "call_kwargs, peers",
    [
        ({}, None),
        ({"peers": None}, None),
        ({"peers": [mock_peer]}, [mock_peer]),
    ],
)
def test_call_rpc_registration(
    rpc, event_loop, args, kwargs, rpc_kwargs, channel, local, call_kwargs, peers
):
    @rpc(**rpc_kwargs)
    def registration():
        pass

    serialized_args = object()
    serialized_kwargs = object()

    with (
        patch.object(rpc, "_send") as send_mock,
        patch.object(
            registration, "_serialize_call", return_value=(serialized_args, serialized_kwargs)
        ) as serialize_call_mock,
        patch.object(registration, "_callback", new_callable=AsyncMock) as callback_mock,
    ):
        event_loop.run_until_complete(registration.call(args, kwargs, **call_kwargs))

    serialize_call_mock.assert_called_once_with(args, kwargs)
    send_mock.assert_called_once_with(
        registration._id, serialized_args, serialized_kwargs, channel, peers
    )
    if local:
        callback_mock.assert_called_once_with(*args, **kwargs)
    else:
        assert callback_mock.call_count == 0


@pytest.mark.parametrize("args", [(), (1, 2, 3)])
@pytest.mark.parametrize("kwargs", [{}, {"one": 1, "two": 2, "three": 3}])
def test_dunder_call_rpc_registration(rpc, event_loop, args, kwargs):
    @rpc()
    def registration():
        pass

    serialized_args = object()
    serialized_kwargs = object()

    with (patch.object(registration, "call", new_callable=AsyncMock) as call_mock,):
        event_loop.run_until_complete(registration(*args, **kwargs))

    call_mock.assert_called_once_with(args, kwargs)


@pytest.mark.parametrize("args", [(), (1, 2, 3)])
@pytest.mark.parametrize("kwargs", [{}, {"one": 1, "two": 2, "three": 3}])
def test_private_call_rpc_registration(rpc, event_loop, args, kwargs):
    @rpc()
    def registration():
        pass

    serialized_args = object()
    serialized_kwargs = object()

    with (
        patch.object(registration, "_callback", new_callable=AsyncMock) as callback_mock,
        patch.object(
            registration, "_deserialize_call", return_value=(args, kwargs)
        ) as deserialize_call_mock,
    ):
        event_loop.run_until_complete(registration._call(serialized_args, serialized_kwargs))

    deserialize_call_mock.assert_called_once_with(serialized_args, serialized_kwargs)
    callback_mock.assert_called_once_with(*args, **kwargs)


def test_call_rpc_registration_invalid(rpc, event_loop, caplog):
    @rpc()
    def registration():
        pass

    with (
        patch.object(registration, "_callback", new_callable=AsyncMock) as callback_mock,
        patch.object(
            registration, "_deserialize_call", side_effect=TypeError("oh noo")
        ) as deserialize_call_mock,
    ):
        event_loop.run_until_complete(registration._call((), {}))

    assert caplog.record_tuples == [
        (
            "doorlord.rpc",
            logging.WARNING,
            f"ignoring rpc for {callback_mock} with invalid arguments: oh noo",
        ),
    ]

    assert callback_mock.call_count == 0


@pytest.mark.parametrize(
    "func, serialized_args, serialized_kwargs, args, kwargs",
    [
        [no_params, [], {}, (), {}],
        [positional_or_keyword_int, [1], {}, (1,), {}],
        [positional_or_keyword_int, [], {"a": 1}, (), {"a": 1}],
        [positional_or_keyword_int_with_default_1, [1], {}, (1,), {}],
        [positional_or_keyword_int_with_default_1, [], {"a": 1}, (), {"a": 1}],
        [positional_or_keyword_int_with_default_1, [], {}, (), {}],
        [positional_only_int, [1], {}, (1,), {}],
        [keyword_only_int_with_default_1, [], {"a": 2}, (), {"a": 2}],
        [keyword_only_int_with_default_1, [], {}, (), {}],
        [keyword_only_int_with_no_default, [], {"a": 2}, (), {"a": 2}],
        [var_position_int, [], {}, (), {}],
        [var_position_int, [1, 2], {}, (1, 2), {}],
        [var_keyword_int, [], {}, (), {}],
        [var_keyword_int, [], {"a": 1, "b": 2}, (), {"a": 1, "b": 2}],
        [var_args_and_kwargs_int, [], {"args": 1, "kwargs": 2}, (), {"args": 1, "kwargs": 2}],
        [complex_int_params, [1, 2], {"d": 4}, (1, 2), {"d": 4}],
        [complex_int_params, [1, 2, 3], {"d": 4}, (1, 2, 3), {"d": 4}],
        [complex_int_params, [1, 2, 3, -1, -2], {"d": 4}, (1, 2, 3, -1, -2), {"d": 4}],
        [complex_int_params, [1], {"b": -1, "d": 4}, (1,), {"b": -1, "d": 4}],
        [complex_int_params, [1, 2], {"c": -2, "d": 4}, (1, 2), {"c": -2, "d": 4}],
        [complex_int_params, [1, 2], {"d": 4, "e": -4}, (1, 2), {"d": 4, "e": -4}],
        [complex_int_params, [1, 2], {"d": 4, "g": 100}, (1, 2), {"d": 4, "g": 100}],
    ],
)
def test_roundtrip_call_valid(rpc, func, serialized_args, serialized_kwargs, args, kwargs):
    registration = rpc()(func)

    serialized_args_, serialized_kwargs_ = registration._serialize_call(args, kwargs)
    assert serialized_args_ == serialized_args
    assert serialized_kwargs_ == serialized_kwargs

    args_, kwargs_ = registration._deserialize_call(serialized_args, serialized_kwargs)
    assert args_ == args
    assert kwargs_ == kwargs


@pytest.mark.parametrize(
    "func, args, kwargs, expected_error",
    [
        (no_params, (1,), {}, "too many positional arguments"),
        (no_params, (), {"x": 1}, f"unexpected keyword arguments: {'x'!r}"),
        (no_params, (), {"x": 1, "y": 1}, f"unexpected keyword arguments: {'x'!r}, {'y'!r}"),
        (no_params, (), {"y": 1, "x": 1}, f"unexpected keyword arguments: {'y'!r}, {'x'!r}"),
        (positional_or_keyword_int, (), {}, f"expected argument in position 0 or named {'a'!r}"),
        (positional_or_keyword_int, (1,), {"a": 2}, f"unexpected keyword arguments: {'a'!r}"),
        (positional_only_int, (), {"a": 1}, f"expected argument for {'a'!r} in position 0"),
        (
            var_position_int,
            (),
            {"x": 1, "y": 1},
            f"unexpected keyword arguments: {'x'!r}, {'y'!r}",
        ),
        (var_keyword_int, (1,), {}, "too many positional arguments"),
        (complex_int_params, (1, 2), {"b": 0, "d": 5}, f"got multiple values for {'b'!r}"),
        (complex_int_params, (1, 2), {}, f"expected keyword argument {'d'!r}"),
        (positional_only_int, (None,), {}, f"expected {int!r} for {'a'!r}"),
        (positional_or_keyword_int, (None,), {}, f"expected {int!r} for {'a'!r}"),
        (keyword_only_int_with_no_default, (), {"a": None}, f"expected {int!r} for {'a'!r}"),
        (var_position_int, (None,), {}, f"expected {int!r} for {'args'!r}"),
        (var_keyword_int, (), {"k": None}, f"expected {int!r} for {'kwargs'!r}"),
    ],
)
def test_roundtrip_call_invalid(rpc, func, args, kwargs, expected_error):
    registration = rpc()(func)

    with pytest.raises(TypeError) as excinfo:
        registration._serialize_call(args, kwargs)
    assert str(excinfo.value) == expected_error

    with pytest.raises(TypeError) as excinfo:
        registration._deserialize_call(args, kwargs)
    assert str(excinfo.value) == expected_error


def test_rountrip_call_method(rpc):
    class X(RpcConverter):
        @rpc()
        def func(self):
            pass

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
    registration = x.func._registration

    serialized_args, serialized_kwargs = registration._serialize_call((x,), {})
    assert serialized_args == [1]
    assert serialized_kwargs == {}

    args, kwargs = registration._deserialize_call([1], {})
    assert args == (x,)
    assert kwargs == {}


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
    converter = _get_converter(type)
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

    converter = _get_converter(type)
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

    converter = _get_converter(type)
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

    converter = _get_converter(CustomConverter)
    assert converter is CustomConverter

    class SubclassConverter(CustomConverter):
        pass

    converter = _get_converter(SubclassConverter)
    assert converter is SubclassConverter


def test_no_converter():
    with pytest.raises(TypeError) as excinfo:
        _get_converter(object)
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

    converter = _get_converter(type)
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

    converter = _get_converter(X | Y)

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

    converter = _get_converter(XX | YY)

    serialized = converter.rpc_serialize(xx)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is xx

    serialized = converter.rpc_serialize(yy)
    json.dumps(serialized)
    assert converter.rpc_deserialize(serialized) is yy

    converter = _get_converter(X | Y)

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
        converter = _get_converter(X | Y)
    assert str(excinfo.value) == (
        f"unable to get rpc converter for Union of ({X | Y}): "
        f"type identifer {X.__qualname__} is shared"
    )
