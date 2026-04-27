"""Verify the removed modules raise on import per Brief 1."""
from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize("module_path", [
    "src.adjustments",
    "src.calibration_feed",
    "src.triangulation",
])
def test_deprecated_modules_raise_on_import(module_path):
    with pytest.raises(ImportError, match="deprecated"):
        importlib.import_module(module_path)
