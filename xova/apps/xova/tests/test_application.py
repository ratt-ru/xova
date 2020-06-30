# -*- coding: utf-8 -*-

import pytest

from xova.testing_data import test_ms
from xova.apps.xova.app import Application


@pytest.mark.parametrize("mode", ["timechannel", "bda"])
def test_application(mode):
    ms = test_ms()
    Application([mode, ms]).execute()


def test_field_selection():
    ms = test_ms()
    Application(["timechannel", ms, "-f 0"]).execute()
