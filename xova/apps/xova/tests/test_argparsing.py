import pytest

from xova.apps.xova.arguments import _parse_fields, _parse_channels


@pytest.mark.parametrize("spec, result", [
    ("1, 3C147, 2", [1, "3C147", 2]),
    ("PKS-1934", ["PKS-1934"]),
    ("1", [1])
])
def test_parse_fields(spec, result):
    assert _parse_fields(spec) == result


@pytest.mark.parametrize("spec, result", [
    ("1", [1]),
    ("3~100", [(3, 100)]),
    ("1,2,3~100", [1, 2, (3, 100)])
])
def test_parse_channels(spec, result):
    assert _parse_channels(spec) == result
