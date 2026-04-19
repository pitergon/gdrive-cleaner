from argparse import Namespace
from datetime import datetime, timedelta, timezone

import pytest

from gdrive_cleaner import cli as cli_module
from gdrive_cleaner.cli import (
    UserInputError,
    confirm_copying,
    confirm_deleting,
    confirm_saving_report,
    get_date_filters,
    get_name_filters,
    read_ids_file,
    save_operation_report,
)
from gdrive_cleaner.drive_core import OperationEntry, OperationResult
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock


class FakeConsole:
    def __init__(self, inputs=None):
        self.inputs = list(inputs or [])
        self.printed = []

    def print(self, message):
        self.printed.append(str(message))

    def input(self, _prompt):
        if not self.inputs:
            raise AssertionError("Unexpected input() call")
        return self.inputs.pop(0)


def test_confirm_deleting_force_returns_true_without_prompt(monkeypatch):
    fake_console = FakeConsole()
    monkeypatch.setattr(cli_module, "error_console", fake_console)
    args = Namespace(dry_run=False, force=True)

    result = confirm_deleting(args, items_count=2, source_msg="selected criteria")

    assert result is True
    assert any("Force deleting 2 items" in line for line in fake_console.printed)


def test_confirm_deleting_dry_run_returns_false_without_prompt(monkeypatch):
    fake_console = FakeConsole()
    monkeypatch.setattr(cli_module, "error_console", fake_console)
    args = Namespace(dry_run=True, force=False)

    result = confirm_deleting(args, items_count=2, source_msg="selected criteria")

    assert result is False
    assert fake_console.printed == []


def test_confirm_deleting_prompt_yes_returns_true(monkeypatch):
    fake_console = FakeConsole(inputs=["yes"])
    monkeypatch.setattr(cli_module, "error_console", fake_console)
    args = Namespace(dry_run=False, force=False)

    result = confirm_deleting(args, items_count=1, source_msg="folder X", has_folders=True)

    assert result is True
    assert any("READY TO DELETE" in line for line in fake_console.printed)
    assert any("DANGER: Deleting a folder" in line for line in fake_console.printed)


def test_confirm_copying_force_returns_true_without_prompt(monkeypatch):
    fake_console = FakeConsole()
    monkeypatch.setattr(cli_module, "error_console", fake_console)
    args = Namespace(dry_run=False, force=True)

    result = confirm_copying(args, source_msg="src", target_msg="dst")

    assert result is True
    assert any("Force copying src to dst" in line for line in fake_console.printed)


def test_confirm_copying_prompt_no_returns_false(monkeypatch):
    fake_console = FakeConsole(inputs=["no"])
    monkeypatch.setattr(cli_module, "error_console", fake_console)
    args = Namespace(dry_run=False, force=False)

    result = confirm_copying(args, source_msg="src", target_msg="dst")

    assert result is False
    assert any("READY TO COPY" in line for line in fake_console.printed)


def test_confirm_saving_report_yes_returns_true(monkeypatch):
    fake_console = FakeConsole(inputs=["yes"])
    monkeypatch.setattr(cli_module, "error_console", fake_console)

    result = confirm_saving_report()

    assert result is True


def test_save_operation_report_writes_disclaimer_csv_and_prints_path(monkeypatch, tmp_path):
    fake_console = FakeConsole()
    monkeypatch.setattr(cli_module, "error_console", fake_console)

    result = OperationResult(
        total=1,
        success=1,
        failed=0,
        entries=[
            OperationEntry(
                id="id-1",
                name="file.txt",
                type="file",
                size="10 B",
                status="success",
                error=None,
            )
        ],
    )
    output_path = tmp_path / "reports" / "report.csv"

    save_operation_report(result, output_path=output_path)

    assert output_path.exists()
    text = output_path.read_text(encoding="utf-8")
    assert text.startswith("Note: Folders are deleted with all their content.")
    assert "ID,Name,Type,Size,Status,Error" in text
    assert "id-1,file.txt,file,10 B,success," in text
    assert any(str(output_path) in line for line in fake_console.printed)


def test_read_ids_file_csv_reads_unique_ids(tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("id\na\nb\na\n", encoding="utf-8")

    ids = read_ids_file(ids_file)

    assert ids == ["a", "b"]


def test_read_ids_file_without_id_column_returns_empty(tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("name\nx\n", encoding="utf-8")

    ids = read_ids_file(ids_file)

    assert ids == []


def test_get_date_filters_before_after_boundary_logic():
    before = datetime(2025, 1, 10, tzinfo=timezone.utc)
    after = datetime(2025, 1, 1, tzinfo=timezone.utc)
    args = Namespace(older=None, before=before, newer=None, after=after)

    date_before, date_after = get_date_filters(args)

    assert date_before == before
    assert date_after == after + timedelta(days=1)  # Used next day 00:00 for API date filter


def test_get_date_filters_rejects_invalid_range():
    before = datetime(2025, 1, 1, tzinfo=timezone.utc)
    after = datetime(2025, 1, 1, tzinfo=timezone.utc)
    args = Namespace(older=None, before=before, newer=None, after=after)

    with pytest.raises(UserInputError):
        get_date_filters(args)


def test_get_name_filters_returns_name_exact():
    args = Namespace(name="Report Q1", contains=None)
    name_exact, name_contains = get_name_filters(args)
    assert name_exact == "Report Q1"
    assert name_contains is None


def test_get_name_filters_returns_name_contains():
    args = Namespace(name=None, contains="Report")
    name_exact, name_contains = get_name_filters(args)
    assert name_exact is None
    assert name_contains == "Report"


def test_ensure_folder_returns_folder_if_valid(mock_ops):
    folder_id  = "folder-123"
    folder = build_item(folder_id, mime_type="application/vnd.google-apps.folder")
    as_mock(mock_ops.get_item).return_value = folder
    args = Namespace(id=folder_id)
    result = cli_module.ensure_folder(args.id, mock_ops)

    assert result.id == folder_id


def test_ensure_folder_rises_user_input_error_if_id_not_found(mock_ops):
    as_mock(mock_ops.get_item).return_value = None
    args = Namespace(id="missing-folder")

    with pytest.raises(UserInputError, match="Folder 'missing-folder' not found."):
        cli_module.ensure_folder(args.id, mock_ops)


def test_ensure_folder_rises_user_input_error_if_id_not_a_folder(mock_ops):
    as_mock(mock_ops.get_item).return_value = build_item("not-folder", mime_type="text/plain")
    args = Namespace(id="not-folder")

    with pytest.raises(UserInputError, match="ID 'not-folder' is not a folder."):
        cli_module.ensure_folder(args.id, mock_ops)
