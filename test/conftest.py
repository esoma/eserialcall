# pytest
import pytest

# python
import asyncio
import gc


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
