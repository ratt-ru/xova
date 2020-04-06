# -*- coding: utf-8 -*-

from xova.testing_data import test_ms
from xova.apps.xova.app import Application


def test_application():
    ms = test_ms()
    Application([ms]).execute()
