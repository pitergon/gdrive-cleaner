# tests/unit/cli/conftest.py

import io
import pytest
from rich.console import Console
from gdrive_cleaner import cli as cli_module


@pytest.fixture(autouse=True)
def mock_error_console(request, monkeypatch):
    """
    Redirects UI error messages from stderr to an internal buffer to prevent
    terminal clutter, especially in PyCharm's test runner which often fails
    to capture stderr correctly.

    NOTE: This fixture overrides standard `capsys` for stderr. To verify
    error output in tests, use the `.get_output()` method of the returned object.

    If pytest is run with `-s` (capture=no), the real stderr is preserved
    for live debugging.
    """

    capture_mode = request.config.getoption("capture")

    if capture_mode == "no":
        # Don't replace in -s mode
        real_err = cli_module.error_console
        if not hasattr(real_err, "get_output"):
            real_err.get_output = lambda: ""
        return real_err

    # capture "sys" | "fd" | "tee-sys"
    err_stream = io.StringIO()
    fake_console = Console(file=err_stream, force_terminal=False)
    monkeypatch.setattr(cli_module, "error_console", fake_console)

    # add a method to retrieve captured output for tests
    fake_console.get_output = lambda: err_stream.getvalue()
    return fake_console


@pytest.fixture
def mock_console(request, monkeypatch):
    """
    Mocks the main stdout console to capture command output for verification.

    Using this fixture instead of `capsys` ensures clean text capture without
    ANSI escape codes (colors/styles) that can break string assertions.

    Access the captured content via `mock_console.get_output()`.

    Like the error console mock, this respects the `-s` flag to allow
    real-time output during manual debugging sessions.
    """
    capture_mode = request.config.getoption("capture")

    if capture_mode == "no":
        real_out = cli_module.console
        if not hasattr(real_out, "get_output"):
            real_out.get_output = lambda: ""
        return real_out

    # capture "sys" | "fd" | "tee-sys"
    out_stream = io.StringIO()
    fake_console = Console(file=out_stream, force_terminal=False)
    monkeypatch.setattr(cli_module, "console", fake_console)

    # add a method to retrieve captured output for tests
    fake_console.get_output = lambda: out_stream.getvalue()
    return fake_console


