# tests/unit/cli/test_clear_folder_flow.py
from argparse import Namespace
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from unittest.mock import Mock

import pytest

from gdrive_cleaner import cli as cli_module
from gdrive_cleaner.cli import UserInputError, handle_clear_folder
from gdrive_cleaner.drive_core import FileFilter, FileItem, OperationResult
from tests.helpers.cli_args_builders import build_clear_folder_args
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock, get_bound_args


@dataclass
class ClearFolderSetup:
    folder_id: str
    folder_item: FileItem
    items: list[FileItem]
    result: OperationResult
    args: Namespace


@pytest.fixture
def clear_folder_setup(mock_ops) -> Callable[..., ClearFolderSetup]:
    def _make(**overrides) -> ClearFolderSetup:
        folder_id = overrides.pop("folder_id", "folder-123")
        folder_item = build_item(folder_id, mime_type="application/vnd.google-apps.folder")
        item_1 = build_item("id-1", parents=[folder_id])
        item_2 = build_item("id-2", parents=[folder_id])

        as_mock(mock_ops.get_item).return_value = folder_item
        as_mock(mock_ops.list_files).return_value = [item_1, item_2]

        result = OperationResult(total=2, success=2, failed=0, entries=[])
        as_mock(mock_ops.delete_items).return_value = result

        args = build_clear_folder_args(folder_id=folder_id, **overrides)
        return ClearFolderSetup(
            folder_id=folder_id,
            folder_item=folder_item,
            items=[item_1, item_2],
            result=result,
            args=args,
        )

    return _make


# =================================================================
# Happy Path scenarios for clear folder flow
# =================================================================
def test_clear_folder_by_id_calls_list_and_delete(mock_ops, clear_folder_setup):
    folder_id = "folder-123"
    setup = clear_folder_setup(folder_id=folder_id, force=True)

    handle_clear_folder(setup.args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    assert isinstance(call_kwargs["file_filter"], FileFilter)
    assert call_kwargs["file_filter"].folder_id == folder_id
    as_mock(mock_ops.delete_items).assert_called_once()


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
def test_clear_folder_api_name_filters_build_expected_file_filter(
        mock_ops,
        clear_folder_setup,
        overrides,
        expected_name_exact,
        expected_name_contains,
):
    folder_id = "folder-123"
    setup = clear_folder_setup(**overrides, folder_id=folder_id, force=True)

    handle_clear_folder(setup.args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    file_filter = call_kwargs["file_filter"]
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
def test_clear_folder_api_date_filters_build_expected_file_filter(
        mock_ops,
        clear_folder_setup,
        overrides,
        expected_before,
        expected_after,
):
    folder_id = "folder-123"
    setup = clear_folder_setup(**overrides, folder_id=folder_id, force=True)

    handle_clear_folder(setup.args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    file_filter = call_kwargs["file_filter"]

    if expected_before == "dynamic":
        older = setup.args.older
        if older:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            date_before = today - timedelta(days=int(older))
            assert file_filter.created_before is not None
            assert file_filter.created_before == date_before
            assert file_filter.created_before.tzinfo == timezone.utc
    else:
        assert file_filter.created_before == expected_before

    if expected_after == "dynamic":
        newer = setup.args.newer
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
# Branching scenarios for clear-folder flow
# =================================================================
def test_clear_folder_dry_run_does_not_delete(mock_ops, clear_folder_setup):
    setup = clear_folder_setup(dry_run=True)
    handle_clear_folder(setup.args, mock_ops)
    as_mock(mock_ops.delete_items).assert_not_called()


def test_clear_folder_no_items_found_does_not_delete(mock_ops, clear_folder_setup):
    setup = clear_folder_setup(force=True)
    as_mock(mock_ops.list_files).return_value = []
    handle_clear_folder(setup.args, mock_ops)
    as_mock(mock_ops.delete_items).assert_not_called()


def test_clear_folder_id_not_found_raises(mock_ops, clear_folder_setup):
    setup = clear_folder_setup(folder_id="missing-id")
    as_mock(mock_ops.get_item).return_value = None

    with pytest.raises(UserInputError, match="Folder missing-id not found."):
        handle_clear_folder(setup.args, mock_ops)


def test_clear_folder_id_not_folder_raises(mock_ops, clear_folder_setup):
    setup = clear_folder_setup(folder_id="not-a-folder")
    as_mock(mock_ops.get_item).return_value = build_item("not-a-folder", mime_type="application/vnd.google-apps.file")

    with pytest.raises(UserInputError, match="Folder not-a-folder not found."):
        handle_clear_folder(setup.args, mock_ops)


def test_clear_folder_cancelled_by_confirmation_does_not_delete(mock_ops, clear_folder_setup, monkeypatch):
    setup = clear_folder_setup(force=False)
    monkeypatch.setattr(cli_module, "confirm_deleting", Mock(return_value=False))
    handle_clear_folder(setup.args, mock_ops)
    as_mock(mock_ops.delete_items).assert_not_called()


def test_clear_folder_with_csv_flag_saves_report(mock_ops, clear_folder_setup, monkeypatch):
    setup = clear_folder_setup(force=True, csv="AUTO")
    orig_func = cli_module.save_operation_report
    save_report_mock = Mock(spec=orig_func)
    monkeypatch.setattr(cli_module, "save_operation_report", save_report_mock)

    handle_clear_folder(setup.args, mock_ops)

    save_report_mock.assert_called_once()

    bound_args = get_bound_args(save_report_mock, orig_func)

    assert bound_args['result'] == setup.result
    assert bound_args['output_path'].suffix == ".csv"


def test_clear_folder_with_relative_csv_path_resolves_under_reports(mock_ops, clear_folder_setup, monkeypatch):
    setup = clear_folder_setup(force=True, csv="ops/report.csv")
    orig_func = cli_module.save_operation_report
    save_report_mock = Mock(spec=orig_func)
    monkeypatch.setattr(cli_module, "save_operation_report", save_report_mock)

    handle_clear_folder(setup.args, mock_ops)

    save_report_mock.assert_called_once()

    bound_args = get_bound_args(save_report_mock, orig_func)

    assert bound_args['result'] == setup.result

    output_path = save_report_mock.call_args.kwargs["output_path"]
    assert str(output_path).endswith(str(Path("reports") / "ops" / "report.csv"))
