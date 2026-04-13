# tests/unit/cli/conftest.py

import io
from unittest.mock import Mock
import pytest
from rich.console import Console
from gdrive_cleaner import cli as cli_module


class TeeStream:
    def __init__(self, real_stream, buffer_stream):
        self.real_stream = real_stream
        self.buffer_stream = buffer_stream

    def write(self, data):
        self.real_stream.write(data)
        self.buffer_stream.write(data)
        return len(data)

    def flush(self):
        self.real_stream.flush()
        self.buffer_stream.flush()

    def isatty(self):
        return getattr(self.real_stream, "isatty", lambda: False)()


@pytest.fixture(autouse=True)
def mock_error_console(request, monkeypatch):
    """
    NOTE: This fixture overrides standard `capsys` for stderr. To verify
    error output in tests, use the `.get_output()` method of the returned object.

    In `-s` mode we use tee behavior: output remains visible and is still captured
    for assertions via `.get_output()`.
    If pytest is run with `-s` (capture=no), the real stderr is preserved
    for live debugging.
    """

    capture_mode = request.config.getoption("capture")

    err_stream = io.StringIO()
    if capture_mode == "no":
        stream = TeeStream(cli_module.error_console.file, err_stream)
    else:
        stream = err_stream
    fake_console = Console(file=stream, force_terminal=False)
    monkeypatch.setattr(cli_module, "error_console", fake_console)
    fake_console.get_output = lambda: err_stream.getvalue()

    return fake_console


@pytest.fixture(autouse=True)
def mock_console(request, monkeypatch):
    """
    NOTE: This fixture overrides standard `capsys` for stdout. To verify
     output in tests, use the `.get_output()` method of the returned object.

    In `-s` mode we use tee behavior: output remains visible and is still captured
    for assertions via `.get_output()`.
    If pytest is run with `-s` (capture=no), the real stderr is preserved
    for live debugging.
    """

    capture_mode = request.config.getoption("capture")

    out_stream = io.StringIO()
    if capture_mode == "no":
        stream = TeeStream(cli_module.console.file, out_stream)
    else:
        stream = out_stream
    fake_console = Console(file=stream, force_terminal=False)
    monkeypatch.setattr(cli_module, "console", fake_console)
    fake_console.get_output = lambda: out_stream.getvalue()

    return fake_console


@pytest.fixture(autouse=True)
def silence_cli_plain_prints(request, monkeypatch):
    """
    Suppress functions with plain print() in CLI to prevent cluttering test output,
    especially in PyCharm's test runner which often fails to capture stdout correctly.
    """
    capture_mode = request.config.getoption("capture")
    if capture_mode != "no":
        monkeypatch.setattr(cli_module, "print_summary", Mock(return_value=None))
        monkeypatch.setattr(cli_module, "smart_print", Mock(return_value=None))


# @pytest.fixture(autouse=True)
# def mock_error_console(request, monkeypatch):
#     """
#     Redirects UI error messages from stderr to an internal buffer to prevent
#     terminal clutter, especially in PyCharm's test runner which often fails
#     to capture stderr correctly.
#
#     NOTE: This fixture overrides standard `capsys` for stderr. To verify
#     error output in tests, use the `.get_output()` method of the returned object.
#
#     If pytest is run with `-s` (capture=no), the real stderr is preserved
#     for live debugging.
#     """
#
#     capture_mode = request.config.getoption("capture")
#
#     if capture_mode == "no":
#         # Don't replace in -s mode
#         real_err: Console = cli_module.error_console
#         if not hasattr(real_err, "get_output"):
#             real_err.get_output = lambda: ""
#         return real_err
#
#     # capture "sys" | "fd" | "tee-sys"
#     err_stream = io.StringIO()
#     fake_console = Console(file=err_stream, force_terminal=False)
#     monkeypatch.setattr(cli_module, "error_console", fake_console)
#
#     # add a method to retrieve captured output for tests
#     fake_console.get_output = lambda: err_stream.getvalue()
#     return fake_console
#
#
# @pytest.fixture(autouse=True)
# def mock_console(request, monkeypatch):
#     """
#     Mocks the main stdout console to capture command output for verification.
#
#     Using this fixture instead of `capsys` ensures clean text capture without
#     ANSI escape codes (colors/styles) that can break string assertions.
#
#     Access the captured content via `mock_console.get_output()`.
#
#     Like the error console mock, this respects the `-s` flag to allow
#     real-time output during manual debugging sessions.
#     """
#     capture_mode = request.config.getoption("capture")
#
#     if capture_mode == "no":
#         real_out: Console = cli_module.console
#         if not hasattr(real_out, "get_output"):
#             real_out.get_output = lambda: ""
#         return real_out
#
#     # capture "sys" | "fd" | "tee-sys"
#     out_stream = io.StringIO()
#     fake_console = Console(file=out_stream, force_terminal=False)
#     monkeypatch.setattr(cli_module, "console", fake_console)
#
#     # add a method to retrieve captured output for tests
#     fake_console.get_output = lambda: out_stream.getvalue()
#     return fake_console