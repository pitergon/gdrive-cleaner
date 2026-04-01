from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

from gdrive_cleaner.cli import handle_delete, handle_fetch, handle_list
from gdrive_cleaner.drive_core import FileItem, OperationResult


def make_item(
    file_id: str,
    name: str = "file.txt",
    mime_type: str = "text/plain",
    size: int = 10,
) -> FileItem:
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return FileItem(
        id=file_id,
        name=name,
        size=size,
        mime_type=mime_type,
        created_at=now,
        modified_at=now,
        parents=[],
        owner=None,
    )


class ListOpsMock:
    def __init__(self):
        self.export_csv_calls = []

    def export_to_csv(self, output_path, file_filter, limit, resolve_ext_parents, on_progress):
        self.export_csv_calls.append(
            {
                "output_path": output_path,
                "file_filter": file_filter,
                "limit": limit,
                "resolve_ext_parents": resolve_ext_parents,
            }
        )
        on_progress("Saving CSV...", 1)

    def export_to_xlsx(self, output_path, file_filter, limit, resolve_ext_parents, on_progress):
        raise AssertionError("export_to_xlsx should not be called in this test")

    def list_files(self, file_filter, limit, on_progress):
        raise AssertionError("list_files should not be called in this test")


class DeleteOpsMock:
    def __init__(self, item: FileItem):
        self.item = item
        self.delete_called = 0

    def get_items_batch(self, ids):
        return {ids[0]: self.item}

    def list_files(self, file_filter=None, on_progress=None):
        raise AssertionError("list_files should not be called in this test")

    def delete_items(self, items, on_progress=None):
        self.delete_called += 1
        if on_progress:
            on_progress(len(items))
        return OperationResult(total=len(items), success=len(items), failed=0, entries=[])


class FetchOpsMock:
    def __init__(self, item: FileItem):
        self.item = item
        self.last_call = None

    def get_item(self, file_id):
        return self.item if file_id == self.item.id else None

    def fetch_item(self, item_or_id, output_path, recursive, force, export, dry_run, on_progress):
        self.last_call = {
            "item_or_id": item_or_id,
            "output_path": output_path,
            "recursive": recursive,
            "force": force,
            "export": export,
            "dry_run": dry_run,
        }
        on_progress(item_or_id.id, item_or_id.name, item_or_id.size, item_or_id.size, "finished")


def test_handle_list_export_csv_reports_to_stderr(capsys):
    ops = ListOpsMock()
    args = Namespace(
        id=None,
        older=None,
        before=None,
        newer=None,
        after=None,
        csv="AUTO",
        xlsx=None,
        folders_only=False,
        resolve_parents=False,
        limit=None,
    )

    handle_list(args, ops)
    captured = capsys.readouterr()

    assert len(ops.export_csv_calls) == 1
    assert "Command 'list' completed. Exported:" in captured.err
    assert str(Path("export")) in captured.err


def test_handle_delete_writes_machine_summary_to_stdout(capsys):
    ops = DeleteOpsMock(make_item("id-1"))
    args = Namespace(
        id="id-1",
        ids_file=None,
        older=None,
        before=None,
        newer=None,
        after=None,
        dry_run=False,
        force=True,
        csv=False,
    )

    handle_delete(args, ops)
    captured = capsys.readouterr()

    assert ops.delete_called == 1
    assert "total=1 success=1 failed=0 critical=0" in captured.out
    assert "Command 'delete' completed." in captured.err


def test_handle_fetch_reports_done_and_completion(capsys, tmp_path):
    item = make_item("f-1", name="report.txt")
    ops = FetchOpsMock(item)
    args = Namespace(
        id="f-1",
        path=str(tmp_path),
        recursive=True,
        force=False,
        export=False,
        dry_run=False,
    )

    handle_fetch(args, ops)
    captured = capsys.readouterr()

    assert ops.last_call is not None
    assert ops.last_call["output_path"] == tmp_path
    assert "DONE: report.txt" in captured.err
    assert "Command 'fetch' completed." in captured.err

