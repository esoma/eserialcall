# eevent
from eserialcall import Channel


def test_channel():
    c = Channel(ordered=True, guaranteed=False, name="test")
    assert c.ordered
    assert not c.guaranteed
    assert c.name == "test"
