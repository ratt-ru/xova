# -*- coding: utf-8 -*-

from xova.testing_data import test_ms
from xova.apps.xova.app import Application


def test_application():
    ms = test_ms()
    Application([ms]).execute()


def test_field_selection():
    ms = test_ms()
    Application([ms, "-f 0"]).execute()
