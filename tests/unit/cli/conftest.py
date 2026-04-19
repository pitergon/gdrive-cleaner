# tests/unit/cli/conftest.py
from unittest.mock import Mock

import pytest

from gdrive_cleaner import cli as cli_module
from tests.helpers.helpers_console import patch_console


@pytest.fixture(autouse=True)
def mock_error_console(monkeypatch, request):
    """
    Patch CLI `error_console` for all unit CLI tests.
    Use `mock_error_console.get_output()` to assert stderr-style UI messages.
    In `-s` mode output stays visible and is still captured.
    """
    return patch_console(monkeypatch, request, cli_module, "error_console")


@pytest.fixture(autouse=True)
def mock_console(request, monkeypatch):
    """
    Patch CLI `console` for all unit CLI tests.
    Use `mock_console.get_output()` to assert stdout-style UI messages.
    In `-s` mode output stays visible and is still captured.
    """

    return patch_console(monkeypatch, request, cli_module, "console")


@pytest.fixture(autouse=True)
def silence_cli_plain_prints(request, monkeypatch):
    """
    Silence plain `print()` helpers in CLI tests to avoid noisy runner output.
    This keeps assertions focused on patched Rich console output.
    """
    capture_mode = request.config.getoption("capture")
    if capture_mode != "no":
        monkeypatch.setattr(cli_module, "print_summary", Mock(return_value=None))
        monkeypatch.setattr(cli_module, "smart_print", Mock(return_value=None))
