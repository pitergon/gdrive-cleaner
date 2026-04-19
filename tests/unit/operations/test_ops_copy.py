from unittest.mock import Mock

from helpers.helpers_drive import build_item
from helpers.helpers_mock import as_mock

from gdrive_cleaner.drive_core import DriveCore
from gdrive_cleaner.operations import DriveOperations


def test_operations_copy_file_returns_copied_item(mock_drive: DriveCore):
    copied = build_item(file_id="new-id", name="new-name")

    ops = DriveOperations(mock_drive)
    as_mock(mock_drive.get_file_metadata).return_value = copied
    as_mock(mock_drive.copy_file).return_value = {"id": "new-id"}

    result = ops.copy_file(file_id="src-id", new_name="new-name", target_id="folder-id")

    assert result == copied
    as_mock(mock_drive.copy_file).assert_called_once_with(
        file_id="src-id", new_name="new-name", target_id="folder-id"
    )
    as_mock(mock_drive.get_file_metadata).assert_called_once_with("new-id")


def test_operations_copy_file_returns_none_when_copied_metadata_is_missing(mock_drive: DriveCore):
    ops = DriveOperations(mock_drive)
    as_mock(mock_drive.copy_file).return_value = {"id": "missing-id"}
    as_mock(mock_drive.get_file_metadata).return_value = None

    result = ops.copy_file(file_id="src-id")
    assert result is None
