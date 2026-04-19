"""
operations.list_files 
если core кидает HttpError 404 при folder_id, 
поднимается OperationInputError."""
import pytest
from googleapiclient.errors import HttpError

from gdrive_cleaner.drive_core import DriveCore, FileFilter
from gdrive_cleaner.operations import DriveOperations, OperationInputError
from tests.helpers.helpers_mock import as_mock


def test_list_raises_input_error_on_404(mock_drive: DriveCore):

    resp = type("Resp", (), {"status": 404, "reason": "Not Found"})()
    as_mock(mock_drive.list_files).side_effect = HttpError(resp=resp, content=b"Not Found")

    ops = DriveOperations(mock_drive)
    file_filter = FileFilter(folder_id="nonexistent_folder_id")

    with pytest.raises(OperationInputError, match="Folder with ID 'nonexistent_folder_id' not found"):
        ops.list_files(file_filter=file_filter)
