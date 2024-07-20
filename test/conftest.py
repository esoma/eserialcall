# eevent
from eserialcall import rpc as global_rpc

# pytest
import pytest

# python
import asyncio
import gc
from unittest.mock import patch


@pytest.fixture
def event_loop(cleanup_event_loop):
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)

    yield event_loop
    del event_loop

    cleanup_event_loop()


@pytest.fixture
def cleanup_event_loop():
    def _():
        try:
            event_loop = asyncio.get_event_loop()
        except RuntimeError:
            gc.collect()
            return
        pending = asyncio.all_tasks(loop=event_loop)
        event_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        del pending
        asyncio.set_event_loop(None)
        del event_loop
        gc.collect()

    return _


@pytest.fixture
def rpc(event_loop):
    transport = global_rpc.transport
    with patch.object(global_rpc, "_registrations", dict(global_rpc._registrations)):
        yield global_rpc
    global_rpc.transport = transport
    gc.collect()
