# tests/unit/operations/test_ops_delete.py

from unittest.mock import Mock

from gdrive_cleaner.drive_core import DriveCore
from gdrive_cleaner.operations import DriveOperations
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock


def test_delete_items_returns_zero_result_for_empty_input(mock_drive: DriveCore):
    ops = DriveOperations(mock_drive)

    result = ops.delete_items([])

    assert result.total == 0
    assert result.success == 0
    assert result.failed == 0
    assert result.entries == []


def test_delete_items_passes_ids_and_metadata_to_core(mock_drive: DriveCore):
    ops = DriveOperations(mock_drive)
    on_progress = Mock()

    i1 = build_item(file_id="f1", name="a.txt", size=1024, mime_type="text/plain")
    i2 = build_item(
        file_id="d1",
        name="folder",
        size=0,
        mime_type="application/vnd.google-apps.folder",
    )

    ops.delete_items([i1, i2], batch_size=50, on_progress=on_progress)

    as_mock(mock_drive.delete_ids).assert_called_once()
    kwargs = as_mock(mock_drive.delete_ids).call_args.kwargs

    assert kwargs["ids"] == ["f1", "d1"]
    assert kwargs["batch_size"] == 50
    assert kwargs["on_progress"] is on_progress
    assert kwargs["metadata"] == {
        "f1": {"name": "a.txt", "type": "file", "size": "1.00 KB"},
        "d1": {"name": "folder", "type": "dir", "size": "0.00 B"},
    }
