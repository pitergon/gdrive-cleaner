from argparse import Namespace
from datetime import datetime, timezone

import pytest

from gdrive_cleaner import cli
from gdrive_cleaner.drive_core import FileItem
from gdrive_cleaner.operations import DriveOperations


def make_item(file_id: str, name: str, mime_type: str = "text/plain") -> FileItem:
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return FileItem(
        id=file_id,
        name=name,
        size=10,
        mime_type=mime_type,
        created_at=now,
        modified_at=now,
        parents=[],
        owner=None,
    )


class DriveMock:
    def __init__(self, copy_response: dict, item_map: dict[str, FileItem | None]):
        self.copy_response = copy_response
        self.item_map = item_map
        self.copy_calls = []

    def copy_file(self, file_id: str, new_name: str | None = None, target_id: str | None = None):
        self.copy_calls.append((file_id, new_name, target_id))
        return self.copy_response

    def get_file_metadata(self, file_id: str):
        return self.item_map.get(file_id)


def test_operations_copy_file_returns_copied_item():
    copied = make_item("new-id", "new-name")
    drive = DriveMock(copy_response={"id": "new-id"}, item_map={"new-id": copied})
    ops = DriveOperations(drive)

    result = ops.copy_file(file_id="src-id", new_name="new-name", target_id="folder-id")

    assert result.id == "new-id"
    assert drive.copy_calls == [("src-id", "new-name", "folder-id")]


def test_operations_copy_file_raises_when_response_has_no_id():
    drive = DriveMock(copy_response={"id": "missing-id"}, item_map={})
    ops = DriveOperations(drive)

    result = ops.copy_file(file_id="src-id")
    assert result is None


class OpsCopyMock:
    def __init__(self, source: FileItem, target: FileItem | None = None, copied: FileItem | None = None):
        self.source = source
        self.target = target
        self.copied = copied or make_item("copied-id", "copied.txt")
        self.copy_calls = []

    def get_item(self, file_id: str):
        if file_id == self.source.id:
            return self.source
        if self.target and file_id == self.target.id:
            return self.target
        return None

    def copy_file(self, file_id: str, new_name: str | None = None, target_id: str | None = None):
        self.copy_calls.append((file_id, new_name, target_id))
        return self.copied


def test_handle_copy_cancelled_on_negative_confirm(monkeypatch, capsys):
    source = make_item("src", "source.txt")
    ops = OpsCopyMock(source=source)
    args = Namespace(id="src", name=None, target_id=None, force=False, dry_run=False)
    monkeypatch.setattr(cli, "confirm_copying", lambda *a, **k: False)

    cli.handle_copy(args, ops)
    captured = capsys.readouterr()

    assert ops.copy_calls == []
    assert "Copy operation cancelled." in captured.err
    assert "Command 'copy' cancelled." in captured.err


def test_handle_copy_dry_run_does_not_call_copy(capsys):
    source = make_item("src", "source.txt")
    ops = OpsCopyMock(source=source)
    args = Namespace(id="src", name="copy.txt", target_id=None, force=False, dry_run=True)

    cli.handle_copy(args, ops)
    captured = capsys.readouterr()

    assert ops.copy_calls == []
    assert "Dry run enabled. Would copy" in captured.err
    assert "Command 'copy' completed: nothing copied." in captured.err


def test_handle_copy_success_calls_ops_and_prints_completion(monkeypatch, capsys):
    source = make_item("src", "source.txt")
    target = make_item("dst-folder", "Dest", mime_type="application/vnd.google-apps.folder")
    copied = make_item("copied-id", "new-name.txt")
    ops = OpsCopyMock(source=source, target=target, copied=copied)
    args = Namespace(id="src", name="new-name.txt", target_id="dst-folder", force=False, dry_run=False)
    monkeypatch.setattr(cli, "confirm_copying", lambda *a, **k: True)

    cli.handle_copy(args, ops)
    captured = capsys.readouterr()

    assert ops.copy_calls == [("src", "new-name.txt", "dst-folder")]
    assert "Copied to 'new-name.txt' in folder Dest (ID: dst-folder) with new ID:" in captured.err
    assert "copied-id" in captured.err
    assert "Command 'copy' completed." in captured.err


def test_handle_copy_raises_when_new_item_is_none(monkeypatch):
    source = make_item("src", "source.txt")
    ops = OpsCopyMock(source=source, copied=None)
    ops.copied = None
    args = Namespace(id="src", name=None, target_id=None, force=False, dry_run=False)
    monkeypatch.setattr(cli, "confirm_copying", lambda *a, **k: True)

    with pytest.raises(cli.UserInputError, match="metadata is unavailable"):
        cli.handle_copy(args, ops)
