# tests/unit/cli/test_delete_flow.py
# Flow tests for `delete`: verify execution paths and side effects
# (which ops methods are called, dry-run behavior, confirmation branch,
# no-items branch, and report-saving branch).

from pathlib import Path
from unittest.mock import Mock

from gdrive_cleaner import cli as cli_module
from gdrive_cleaner.cli import handle_delete
from gdrive_cleaner.drive_core import FileFilter, OperationResult
from tests.helpers.cli_args_builders import build_delete_args
from tests.helpers.helpers_drive import as_mock, build_item


def test_delete_dry_run_does_not_delete(mock_ops):
    item_id = "abc"
    item = build_item(file_id=item_id)
    mock_ops.get_items_batch.return_value = {item_id: item}
    args = build_delete_args(id=item_id, dry_run=True)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.get_items_batch).assert_called_once_with([item_id])
    as_mock(mock_ops.delete_items).assert_not_called()


def test_delete_by_ids_file_calls_get_batch_and_delete(mock_ops, tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("id\nid-1\nid-2\n", encoding="utf-8")
    item_1 = build_item("id-1")
    item_2 = build_item("id-2")
    mock_ops.get_items_batch.return_value = {"id-1": item_1, "id-2": item_2}
    result = OperationResult(total=2, success=2, failed=0, entries=[])
    mock_ops.delete_items.return_value = result
    args = build_delete_args(ids_file=str(ids_file), force=True)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.get_items_batch).assert_called_once_with(["id-1", "id-2"])
    as_mock(mock_ops.delete_items).assert_called_once()


def test_delete_by_api_name_filter_uses_list_files(mock_ops):
    item = build_item("id-1", name="report.txt")
    mock_ops.list_files.return_value = [item]
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    mock_ops.delete_items.return_value = result
    args = build_delete_args(name="report.txt", force=True)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    assert isinstance(call_kwargs["file_filter"], FileFilter)
    assert call_kwargs["file_filter"].name_exact == "report.txt"
    as_mock(mock_ops.delete_items).assert_called_once()


def test_delete_when_no_items_found_does_not_delete(mock_ops):
    mock_ops.get_items_batch.return_value = {"missing": None}
    args = build_delete_args(id="missing")

    handle_delete(args, mock_ops)

    as_mock(mock_ops.delete_items).assert_not_called()


def test_delete_cancelled_by_confirmation_does_not_delete(mock_ops, monkeypatch):
    item = build_item("id-1")
    mock_ops.get_items_batch.return_value = {"id-1": item}
    args = build_delete_args(id="id-1", force=False)
    monkeypatch.setattr(cli_module, "confirm_deleting", lambda *a, **k: False)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.delete_items).assert_not_called()


def test_delete_with_csv_flag_saves_report(mock_ops, monkeypatch):
    item = build_item("id-1")
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    mock_ops.get_items_batch.return_value = {"id-1": item}
    mock_ops.delete_items.return_value = result
    save_report_mock = Mock()
    monkeypatch.setattr(cli_module, "save_operation_report", save_report_mock)
    args = build_delete_args(id="id-1", force=True, csv="AUTO")

    handle_delete(args, mock_ops)

    save_report_mock.assert_called_once()
    assert save_report_mock.call_args.args[0] == result
    assert save_report_mock.call_args.kwargs["output_path"].suffix == ".csv"


def test_delete_with_relative_csv_path_resolves_under_reports(mock_ops, monkeypatch):
    item = build_item("id-1")
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    mock_ops.get_items_batch.return_value = {"id-1": item}
    mock_ops.delete_items.return_value = result
    save_report_mock = Mock()
    monkeypatch.setattr(cli_module, "save_operation_report", save_report_mock)
    args = build_delete_args(id="id-1", force=True, csv="ops/report.csv")

    handle_delete(args, mock_ops)

    save_report_mock.assert_called_once()
    output_path = save_report_mock.call_args.kwargs["output_path"]
    assert str(output_path).endswith(str(Path("reports") / "ops" / "report.csv"))
