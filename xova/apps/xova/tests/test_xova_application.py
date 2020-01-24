import pytest

from xova.apps.xova.app import Application


@pytest.mark.parametrize("ms", ["/home/sperkins/data/WSRT_multiple.MS_p0"])
def test_xova_application(ms):
    Application([ms]).execute()
