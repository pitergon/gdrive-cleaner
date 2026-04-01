from argparse import Namespace
from datetime import datetime, timezone

import pytest

from gdrive_cleaner.cli import UserInputError, handle_delete
from gdrive_cleaner.drive_core import FileItem


def make_item(file_id: str, name: str = "name", mime_type: str = "text/plain", size: int = 10) -> FileItem:
    return FileItem(
        id=file_id,
        name=name,
        size=size,
        mime_type=mime_type,
        created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        modified_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        parents=[],
        owner=None,
    )


class DummyOps:
    def __init__(self, items_map=None):
        self.items_map = items_map or {}
        self.get_items_batch_called = 0
        self.list_files_called = 0
        self.delete_items_called = 0

    def get_items_batch(self, ids):
        self.get_items_batch_called += 1
        return {fid: self.items_map.get(fid) for fid in ids}

    def list_files(self, file_filter=None, on_progress=None):
        self.list_files_called += 1
        return []

    def delete_items(self, items, on_progress=None):
        self.delete_items_called += 1
        raise AssertionError("delete_items should not be called in this test")


def test_handle_delete_rejects_ids_with_date_filters():
    ops = DummyOps()
    args = Namespace(
        id="123",
        ids_file=None,
        older=10,
        before=None,
        newer=None,
        after=None,
        dry_run=False,
        force=False,
        csv=False,
    )

    with pytest.raises(UserInputError):
        handle_delete(args, ops)


def test_handle_delete_requires_any_filter():
    ops = DummyOps()
    args = Namespace(
        id=None,
        ids_file=None,
        older=None,
        before=None,
        newer=None,
        after=None,
        dry_run=False,
        force=False,
        csv=False,
    )

    with pytest.raises(UserInputError):
        handle_delete(args, ops)


def test_handle_delete_dry_run_does_not_delete():
    item = make_item("abc")
    ops = DummyOps(items_map={"abc": item})
    args = Namespace(
        id="abc",
        ids_file=None,
        older=None,
        before=None,
        newer=None,
        after=None,
        dry_run=True,
        force=False,
        csv=False,
    )

    handle_delete(args, ops)

    assert ops.get_items_batch_called == 1
    assert ops.delete_items_called == 0

