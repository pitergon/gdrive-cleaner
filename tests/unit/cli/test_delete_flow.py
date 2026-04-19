# tests/unit/cli/test_delete_flow.py
# Flow tests for `delete`: verify execution paths and side effects
# (which ops methods are called, dry-run behavior, confirmation branch,
# no-items branch, and report-saving branch).

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from gdrive_cleaner import cli as cli_module
from gdrive_cleaner.cli import handle_delete
from gdrive_cleaner.drive_core import FileFilter, OperationResult
from tests.helpers.cli_args_builders import build_delete_args
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock, get_bound_args


# =================================================================
# Happy Path scenarios for delete flow
# =================================================================
def test_delete_by_ids_file_calls_get_batch_and_delete(mock_ops, tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("id\nid-1\nid-2\n", encoding="utf-8")
    item_1 = build_item("id-1")
    item_2 = build_item("id-2")
    as_mock(mock_ops.get_items_batch).return_value = {"id-1": item_1, "id-2": item_2}
    result = OperationResult(total=2, success=2, failed=0, entries=[])
    as_mock(mock_ops.delete_items).return_value = result
    args = build_delete_args(ids_file=str(ids_file), force=True)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.get_items_batch).assert_called_once_with(["id-1", "id-2"])
    as_mock(mock_ops.delete_items).assert_called_once()


def test_delete_by_id_calls_delete(mock_ops, mock_error_console):
    item_id = "abc"
    item = build_item(file_id=item_id)
    as_mock(mock_ops.get_items_batch).return_value = {item_id: item}
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    as_mock(mock_ops.delete_items).return_value = result
    args = build_delete_args(id=item_id, force=True)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.get_items_batch).assert_called_once()
    as_mock(mock_ops.delete_items).assert_called_once()

    assert "Command 'delete' completed." in mock_error_console.get_output()


@pytest.mark.parametrize(
    ("overrides", "expected_name_exact", "expected_name_contains"),
    [
        ({"name": "report.txt"}, "report.txt", None),
        ({"contains": "report"}, None, "report"),
        (
            {"name": "report.txt", "before": datetime(2025, 1, 10, tzinfo=timezone.utc)},
            "report.txt",
            None,
        ),
    ],
    ids=["name-exact", "name-contains", "name-plus-before"],
)
def test_delete_api_name_filters_build_expected_file_filter(
    mock_ops, overrides, expected_name_exact, expected_name_contains
):
    item = build_item("id-1", name="report.txt")
    as_mock(mock_ops.list_files).return_value = [item]
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    as_mock(mock_ops.delete_items).return_value = result
    args = build_delete_args(force=True, **overrides)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    file_filter = call_kwargs["file_filter"]
    assert isinstance(file_filter, FileFilter)
    assert file_filter.name_exact == expected_name_exact
    assert file_filter.name_contains == expected_name_contains
    as_mock(mock_ops.delete_items).assert_called_once()


@pytest.mark.parametrize(
    ("overrides", "expected_before", "expected_after"),
    [
        (
            {"before": datetime(2025, 1, 10, tzinfo=timezone.utc)},
            datetime(2025, 1, 10, tzinfo=timezone.utc),
            None,
        ),
        (
            {"after": datetime(2025, 1, 10, tzinfo=timezone.utc)},
            None,
            datetime(2025, 1, 11, tzinfo=timezone.utc),
        ),
        ({"older": 7}, "dynamic", None),
        ({"newer": 3}, None, "dynamic"),
    ],
    ids=["before-date", "after-date-shifted-plus-one-day", "older-days", "newer-days"],
)
def test_delete_api_date_filters_build_expected_file_filter(
    mock_ops, overrides, expected_before, expected_after
):
    item = build_item("id-1", name="report.txt")
    as_mock(mock_ops.list_files).return_value = [item]
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    as_mock(mock_ops.delete_items).return_value = result
    args = build_delete_args(force=True, **overrides)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    file_filter = call_kwargs["file_filter"]
    assert isinstance(file_filter, FileFilter)

    if expected_before == "dynamic":
        older = args.older
        if older:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            date_before = today - timedelta(days=int(older))
            assert file_filter.created_before is not None
            assert file_filter.created_before == date_before
            assert file_filter.created_before.tzinfo == timezone.utc
    else:
        assert file_filter.created_before == expected_before

    if expected_after == "dynamic":
        newer = args.newer
        if newer:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            date_after = today - timedelta(days=int(newer) - 1)
            assert file_filter.created_after is not None
            assert file_filter.created_after == date_after
            assert file_filter.created_after.tzinfo == timezone.utc
    else:
        assert file_filter.created_after == expected_after

    as_mock(mock_ops.delete_items).assert_called_once()


# =================================================================
# Branching scenarios for delete flow
# =================================================================
def test_delete_dry_run_does_not_delete(mock_ops):
    item_id = "abc"
    item = build_item(file_id=item_id)
    as_mock(mock_ops.get_items_batch).return_value = {item_id: item}
    args = build_delete_args(id=item_id, dry_run=True)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.get_items_batch).assert_called_once_with([item_id])
    as_mock(mock_ops.delete_items).assert_not_called()


def test_delete_when_no_items_found_does_not_delete(mock_ops):
    as_mock(mock_ops.get_items_batch).return_value = {"missing": None}
    args = build_delete_args(id="missing")

    handle_delete(args, mock_ops)

    as_mock(mock_ops.delete_items).assert_not_called()


def test_delete_cancelled_by_confirmation_does_not_delete(mock_ops, monkeypatch):
    item = build_item("id-1")
    as_mock(mock_ops.get_items_batch).return_value = {"id-1": item}
    args = build_delete_args(id="id-1", force=False)
    monkeypatch.setattr(cli_module, "confirm_deleting", lambda *a, **k: False)

    handle_delete(args, mock_ops)

    as_mock(mock_ops.delete_items).assert_not_called()


def test_delete_with_csv_flag_saves_report(mock_ops, monkeypatch):
    item = build_item("id-1")
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    as_mock(mock_ops.get_items_batch).return_value = {"id-1": item}
    as_mock(mock_ops.delete_items).return_value = result
    orig_func = cli_module.save_operation_report
    save_report_mock = Mock(spec=orig_func)
    monkeypatch.setattr(cli_module, "save_operation_report", save_report_mock)
    args = build_delete_args(id="id-1", force=True, csv="AUTO")

    handle_delete(args, mock_ops)

    save_report_mock.assert_called_once()

    bound_args = get_bound_args(save_report_mock, orig_func)

    assert bound_args['result'] == result
    assert bound_args['output_path'].suffix == ".csv"


def test_delete_with_relative_csv_path_resolves_under_reports(mock_ops, monkeypatch):
    item = build_item("id-1")
    result = OperationResult(total=1, success=1, failed=0, entries=[])
    as_mock(mock_ops.get_items_batch).return_value = {"id-1": item}
    as_mock(mock_ops.delete_items).return_value = result

    save_report_mock = Mock()
    monkeypatch.setattr(cli_module, "save_operation_report", save_report_mock)
    args = build_delete_args(id="id-1", force=True, csv="ops/report.csv")

    handle_delete(args, mock_ops)

    save_report_mock.assert_called_once()

    output_path = save_report_mock.call_args.kwargs["output_path"]
    assert output_path.parts[-3:] == ("reports", "ops", "report.csv")
