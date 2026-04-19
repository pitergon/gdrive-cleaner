# tests/unit/cli/test_fetch_flow.py
# Flow tests for `fetch`: verify execution paths and side effects.
from argparse import Namespace
from typing import Callable
from dataclasses import dataclass
from unittest.mock import Mock

import pytest
import gdrive_cleaner.cli as cli_module
from gdrive_cleaner.cli import handle_fetch
from gdrive_cleaner.drive_core import FileItem
from gdrive_cleaner.operations import DriveOperations
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock
from tests.helpers.cli_args_builders import build_fetch_args


@dataclass
class FetchSetup:
    item: FileItem
    args: Namespace


@pytest.fixture
def fetch_setup(mock_ops) -> Callable[..., FetchSetup]:
    def _make(**overrides) -> FetchSetup:
        item = overrides.pop("item", build_item(file_id="item-id", name="Test Item", mime_type="text/plain"))
        args_defaults = {"id": "item-id", "force": True}
        args_defaults.update(overrides)
        args = build_fetch_args(**args_defaults)
        as_mock(mock_ops.get_item).return_value = item
        return FetchSetup(item=item, args=args)

    return _make


def test_fetch_calls_ops_fetch_item_with_expected_args(mock_ops: DriveOperations, fetch_setup):
    setup = fetch_setup()

    handle_fetch(setup.args, mock_ops)

    call_kwargs = as_mock(mock_ops.fetch_item).call_args.kwargs
    as_mock(mock_ops.fetch_item).assert_called_once()
    assert call_kwargs["item_or_id"] == setup.item


def test_fetch_resolves_relative_path_under_download(mock_ops: DriveOperations, fetch_setup):
    item = build_item(file_id="item-id", name="text.txt", mime_type="text/plain")
    setup = fetch_setup(item=item, path="folder/test.txt")

    handle_fetch(setup.args, mock_ops)

    call_kwargs = as_mock(mock_ops.fetch_item).call_args.kwargs
    as_mock(mock_ops.fetch_item).assert_called_once()
    output_path = call_kwargs["output_path"]
    assert output_path is not None
    assert output_path.parts[-3:] == ("download", "folder", "test.txt")


def test_fetch_with_explicit_absolute_path_passes_absolute_path(mock_ops: DriveOperations, tmp_path):
    item = build_item(file_id="item-id", name="text.txt", mime_type="text/plain")
    path = (tmp_path / "text.txt").resolve()
    args = build_fetch_args(id="item-id", path=path, force=True)
    setup = FetchSetup(item=item,args=args)
    as_mock(mock_ops.get_item).return_value = setup.item

    handle_fetch(setup.args, mock_ops)

    as_mock(mock_ops.fetch_item).assert_called_once()
    call_kwargs = as_mock(mock_ops.fetch_item).call_args.kwargs
    output_path = call_kwargs["output_path"]
    assert output_path is not None
    assert output_path == path
    assert output_path.is_absolute()


def test_fetch_dry_run_passes_dry_run_flag_to_ops(mock_ops: DriveOperations, fetch_setup):
    setup = fetch_setup(dry_run=True)

    handle_fetch(setup.args, mock_ops)

    as_mock(mock_ops.fetch_item).assert_called_once()
    call_kwargs = as_mock(mock_ops.fetch_item).call_args.kwargs
    assert call_kwargs["dry_run"] is True


def test_fetch_item_not_found_raises_user_input_error(mock_ops):
    as_mock(mock_ops.get_item).return_value = None
    args = build_fetch_args(id="item-id")

    with pytest.raises(cli_module.UserInputError, match="Item with ID 'item-id' not found."):
        handle_fetch(args, mock_ops)


def test_fetch_handles_finished_progress_status(mock_ops, mock_error_console, fetch_setup):
    setup = fetch_setup()

    handle_fetch(setup.args, mock_ops)

    as_mock(mock_ops.fetch_item).assert_called_once()
    assert "Command 'fetch' completed." in mock_error_console.get_output()


def test_fetch_handles_error_progress_status(mock_ops, mock_error_console, fetch_setup):
    setup = fetch_setup()

    def fake_fetch_item(**kwargs):
        item_or_id: FileItem = kwargs["item_or_id"]
        on_progress: Callable = kwargs["on_progress"]
        on_progress(item_or_id.id, item_or_id.name, 0, item_or_id.size, "error")

    as_mock(mock_ops.fetch_item).side_effect = fake_fetch_item

    handle_fetch(setup.args, mock_ops)

    as_mock(mock_ops.fetch_item).assert_called_once()
    call_kwargs = as_mock(mock_ops.fetch_item).call_args.kwargs
    assert call_kwargs["on_progress"] is not None
    assert "FAIL: Test Item" in mock_error_console.get_output()


def test_fetch_handles_dry_run_progress_status(mock_ops, mock_error_console, fetch_setup):
    setup = fetch_setup()

    def fake_fetch_item(**kwargs):
        item_or_id: FileItem = kwargs["item_or_id"]
        on_progress: Callable = kwargs["on_progress"]
        on_progress(item_or_id.id, item_or_id.name, 0, item_or_id.size, "dry_run")

    as_mock(mock_ops.fetch_item).side_effect = fake_fetch_item

    handle_fetch(setup.args, mock_ops)

    as_mock(mock_ops.fetch_item).assert_called_once()
    call_kwargs = as_mock(mock_ops.fetch_item).call_args.kwargs
    assert call_kwargs["on_progress"] is not None
    assert "DRY: Test Item" in mock_error_console.get_output()


def test_fetch_tty_does_not_crash_on_finished_then_error(monkeypatch, mock_ops, fetch_setup):

    printed = []

    class FakeConsole:
        def print(self, message):
            printed.append(str(message))

    class FakeProgress:
        def __init__(self, *args, **kwargs):
            self.console = FakeConsole()
            self._tasks = {}
            self._next = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def add_task(self, description, total):
            task_id = self._next
            self._next += 1
            self._tasks[task_id] = {"description": description, "total": total, "completed": 0}
            return task_id

        def update(self, task_id, completed):
            if task_id not in self._tasks:
                raise KeyError(task_id)
            self._tasks[task_id]["completed"] = completed

        def remove_task(self, task_id):
            if task_id not in self._tasks:
                raise KeyError(task_id)
            del self._tasks[task_id]

    def fake_fetch_item(**kwargs):
        item_or_id: FileItem = kwargs["item_or_id"]
        on_progress: Callable = kwargs["on_progress"]
        on_progress(item_or_id.id, item_or_id.name, 0, 0, "finished")
        on_progress(item_or_id.id, item_or_id.name, 0, 0, "error")

    setup = fetch_setup(item=build_item(file_id="f-2", name="glitch.docx"))
    as_mock(mock_ops.fetch_item).side_effect = fake_fetch_item

    monkeypatch.setattr(cli_module, "Progress", FakeProgress)
    monkeypatch.setattr(cli_module.sys.stdout, "isatty", Mock(return_value=True))

    handle_fetch(setup.args, mock_ops)

    assert any("Success" in msg for msg in printed)
    assert any("Failed" in msg for msg in printed)
