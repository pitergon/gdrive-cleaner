# tests/helpers/helpers_console.py
import io

import pytest
from rich.console import Console


class TeeStream:
    """Write to the real stream and to an in-memory buffer at the same time."""

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


def _build_console_capture(
    *,
    real_console: Console,
    capture_mode: str | None = None,
    request: pytest.FixtureRequest,
) -> Console:
    """
    Create a Rich Console used in tests.
    The console always captures output for assertions via `get_output()`.
    In `capture=no` mode, output is also sent to the original stream (tee mode).
    """
    capture_mode = capture_mode or request.config.getoption("capture")
    buffer = io.StringIO()
    if capture_mode == "no":
        stream = TeeStream(real_console.file, buffer)
    else:
        stream = buffer

    fake_console = Console(file=stream, force_terminal=False)
    fake_console.get_output = lambda: buffer.getvalue()
    return fake_console


def patch_console(monkeypatch, request, module, attr_name):
    """
    Replace `module.<attr_name>` with a capture-enabled Rich Console.
    Returns the patched console object with `get_output()`.
    """
    real_console = getattr(module, attr_name)
    fake_console = _build_console_capture(real_console=real_console, request=request)
    monkeypatch.setattr(module, attr_name, fake_console)
    return fake_console
