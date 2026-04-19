# tests/unit/cli/test_list_flow.py
# Flow tests for `list`: verify filter construction and execution branches
# (list API, export API, folder-id validation, and UI list rendering call).

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from gdrive_cleaner import cli as cli_module
from gdrive_cleaner.cli import UserInputError, handle_list
from gdrive_cleaner.drive_core import FileFilter
from tests.helpers.cli_args_builders import build_list_args
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock


def test_list_calls_list_without_filters(mock_ops, monkeypatch):
    item = build_item("id-1", name="a.txt")
    mock_ops.list_files.return_value = [item]
    smart_print_mock = Mock()
    monkeypatch.setattr(cli_module, "smart_print", smart_print_mock)
    args = build_list_args()

    handle_list(args, mock_ops)

    as_mock(mock_ops.list_files).assert_called_once()
    call_kwargs = as_mock(mock_ops.list_files).call_args.kwargs
    file_filter = call_kwargs["file_filter"]
    assert isinstance(file_filter, FileFilter)
    assert file_filter.folder_id is None
    assert file_filter.created_before is None
    assert file_filter.created_after is None
    assert file_filter.name_exact is None
    assert file_filter.name_contains is None
    assert call_kwargs["limit"] is None
    smart_print_mock.assert_called_once_with([item])


def test_list_api_id_filter_builds_expected_file_filter(mock_ops, monkeypatch):
    folder_id = "folder-123"
    folder = build_item(folder_id, mime_type="application/vnd.google-apps.folder")
    mock_ops.get_item.return_value = folder
    mock_ops.list_files.return_value = []
    args = build_list_args(id=folder_id)

    handle_list(args, mock_ops)

    as_mock(mock_ops.get_item).assert_called_once_with(file_id=folder_id)
    as_mock(mock_ops.list_files).assert_called_once()
    file_filter = as_mock(mock_ops.list_files).call_args.kwargs["file_filter"]
    assert file_filter.folder_id == folder_id


def test_list_id_not_found_raises_user_input_error(mock_ops):
    mock_ops.get_item.return_value = None
    args = build_list_args(id="missing-folder")

    with pytest.raises(UserInputError, match="Folder 'missing-folder' not found."):
        handle_list(args, mock_ops)


def test_list_id_not_folder_raises_user_input_error(mock_ops):
    mock_ops.get_item.return_value = build_item("not-folder", mime_type="text/plain")
    args = build_list_args(id="not-folder")

    with pytest.raises(UserInputError, match="ID 'not-folder' is not a folder."):
        handle_list(args, mock_ops)


@pytest.mark.parametrize(
    ("overrides", "expected_name_exact", "expected_name_contains"),
    [
        ({"name": "report.txt"}, "report.txt", None),
        ({"contains": "report"}, None, "report"),
    ],
    ids=["name-exact", "name-contains"],
)
def test_list_api_name_filters_build_expected_file_filter(
    mock_ops, overrides, expected_name_exact, expected_name_contains
):
    mock_ops.list_files.return_value = []
    args = build_list_args(**overrides)

    handle_list(args, mock_ops)

    file_filter = as_mock(mock_ops.list_files).call_args.kwargs["file_filter"]
    assert file_filter.name_exact == expected_name_exact
    assert file_filter.name_contains == expected_name_contains


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
    ids=["before-date", "after-plus-one-day", "older-days", "newer-days"],
)
def test_list_api_date_filters_build_expected_file_filter(mock_ops, overrides, expected_before, expected_after):
    mock_ops.list_files.return_value = []
    args = build_list_args(**overrides)

    handle_list(args, mock_ops)

    file_filter = as_mock(mock_ops.list_files).call_args.kwargs["file_filter"]

    if expected_before == "dynamic":
        older = args.older
        assert older is not None
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        assert file_filter.created_before == today - timedelta(days=int(older))
    else:
        assert file_filter.created_before == expected_before

    if expected_after == "dynamic":
        newer = args.newer
        assert newer is not None
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        assert file_filter.created_after == today - timedelta(days=int(newer) - 1)
    else:
        assert file_filter.created_after == expected_after


def test_list_folders_only_sets_folder_mime_filter(mock_ops):
    mock_ops.list_files.return_value = []
    args = build_list_args(folders_only=True)

    handle_list(args, mock_ops)

    file_filter = as_mock(mock_ops.list_files).call_args.kwargs["file_filter"]
    assert file_filter.mime_type == "application/vnd.google-apps.folder"


def test_list_export_csv_uses_export_api_and_not_list_files(mock_ops):
    args = build_list_args(csv="AUTO")

    handle_list(args, mock_ops)

    as_mock(mock_ops.export_to_csv).assert_called_once()
    as_mock(mock_ops.list_files).assert_not_called()


def test_list_export_relative_csv_path_resolves_under_export(mock_ops):
    args = build_list_args(csv="nested/report.csv")

    handle_list(args, mock_ops)

    output_path = as_mock(mock_ops.export_to_csv).call_args.kwargs["output_path"]
    assert output_path.parts[-3:] == ("export", "nested", "report.csv")


def test_list_export_xlsx_uses_export_api_and_not_list_files(mock_ops):
    args = build_list_args(xlsx="AUTO")

    handle_list(args, mock_ops)

    as_mock(mock_ops.export_to_xlsx).assert_called_once()
    as_mock(mock_ops.list_files).assert_not_called()


def test_list_export_relative_xlsx_path_resolves_under_export(mock_ops):
    args = build_list_args(xlsx="nested/report.xlsx")

    handle_list(args, mock_ops)

    output_path = as_mock(mock_ops.export_to_xlsx).call_args.kwargs["output_path"]
    assert output_path.parts[-3:] == ("export", "nested", "report.xlsx")

