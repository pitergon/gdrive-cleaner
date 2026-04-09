from argparse import Namespace
from datetime import datetime, timezone
import io
from pathlib import Path

from rich.console import Console

from gdrive_cleaner import cli as cli_module
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


def build_error_console():
    output = io.StringIO()
    return Console(file=output, force_terminal=False), output


def test_handle_list_export_csv_reports_to_stderr(monkeypatch):
    ops = ListOpsMock()
    console, output = build_error_console()
    monkeypatch.setattr(cli_module, "error_console", console)
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
    text = output.getvalue()

    assert len(ops.export_csv_calls) == 1
    assert "Command 'list' completed. Exported:" in text
    assert str(Path("export")) in text


def test_handle_delete_writes_machine_summary_to_stdout(monkeypatch):
    ops = DeleteOpsMock(make_item("id-1"))
    console, output = build_error_console()
    summary_calls = []
    monkeypatch.setattr(cli_module, "error_console", console)
    monkeypatch.setattr(cli_module, "print_summary", lambda result: summary_calls.append(result))
    args = Namespace(
        id="id-1",
        ids_file=None,
        older=None,
        before=None,
        newer=None,
        after=None,
        dry_run=False,
        force=True,
        csv=None,
    )

    handle_delete(args, ops)
    text = output.getvalue()

    assert ops.delete_called == 1
    assert len(summary_calls) == 1
    assert summary_calls[0].total == 1
    assert "Command 'delete' completed." in text


def test_handle_fetch_reports_done_and_completion(monkeypatch, tmp_path):
    item = make_item("f-1", name="report.txt")
    ops = FetchOpsMock(item)
    console, output = build_error_console()
    monkeypatch.setattr(cli_module, "error_console", console)
    args = Namespace(
        id="f-1",
        path=str(tmp_path),
        recursive=True,
        force=False,
        export=False,
        dry_run=False,
    )

    handle_fetch(args, ops)
    text = output.getvalue()

    assert ops.last_call is not None
    assert ops.last_call["output_path"] == tmp_path
    assert "DONE: report.txt" in text
    assert "Command 'fetch' completed." in text


def test_handle_list_relative_csv_path_resolved_under_export(monkeypatch):
    ops = ListOpsMock()
    console, _ = build_error_console()
    monkeypatch.setattr(cli_module, "error_console", console)
    args = Namespace(
        id=None,
        older=None,
        before=None,
        newer=None,
        after=None,
        csv="nested/report.csv",
        xlsx=None,
        folders_only=False,
        resolve_parents=False,
        limit=None,
    )

    handle_list(args, ops)

    output_path = ops.export_csv_calls[0]["output_path"]
    assert str(output_path).endswith(str(Path("export") / "nested" / "report.csv"))


def test_handle_fetch_relative_path_resolved_under_download(monkeypatch):
    item = make_item("f-10", name="file.txt")
    ops = FetchOpsMock(item)
    console, _ = build_error_console()
    monkeypatch.setattr(cli_module, "error_console", console)
    args = Namespace(
        id="f-10",
        path="backup",
        recursive=False,
        force=False,
        export=False,
        dry_run=False,
    )

    handle_fetch(args, ops)

    assert ops.last_call["output_path"] == (Path("download") / "backup").resolve()


def test_handle_fetch_uses_default_download_dir_when_path_omitted(monkeypatch):
    item = make_item("f-11", name="file.txt")
    ops = FetchOpsMock(item)
    console, _ = build_error_console()
    monkeypatch.setattr(cli_module, "error_console", console)
    args = Namespace(
        id="f-11",
        path=None,
        recursive=False,
        force=False,
        export=False,
        dry_run=False,
    )

    handle_fetch(args, ops)

    assert ops.last_call["output_path"] == Path("download").resolve()


def test_handle_fetch_reports_dry_run_status(monkeypatch, tmp_path):
    class FetchOpsDryRunMock(FetchOpsMock):
        def fetch_item(self, item_or_id, output_path, recursive, force, export, dry_run, on_progress):
            self.last_call = {
                "item_or_id": item_or_id,
                "output_path": output_path,
                "recursive": recursive,
                "force": force,
                "export": export,
                "dry_run": dry_run,
            }
            on_progress(item_or_id.id, item_or_id.name, 0, item_or_id.size, "dry_run")

    item = make_item("f-3", name="dryrun.txt")
    ops = FetchOpsDryRunMock(item)
    console, output = build_error_console()
    monkeypatch.setattr(cli_module, "error_console", console)
    args = Namespace(
        id="f-3",
        path=str(tmp_path),
        recursive=False,
        force=False,
        export=False,
        dry_run=True,
    )

    handle_fetch(args, ops)
    text = output.getvalue()

    assert "DRY: dryrun.txt" in text
    assert "Command 'fetch' completed." in text


def test_handle_fetch_tty_does_not_crash_on_finished_then_error(monkeypatch, tmp_path):
    class DummyStdout:
        def isatty(self):
            return True

    printed = []

    class FakeConsole:
        def print(self, message):
            printed.append(str(message))

    class FakeProgress:
        def __init__(self, *args, **kwargs):
            self.console = FakeConsole()
            self._tasks = {}
            self._next = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def add_task(self, description, total):
            task_id = self._next
            self._next += 1
            self._tasks[task_id] = {"description": description, "total": total, "completed": 0}
            return task_id

        def update(self, task_id, completed):
            if task_id not in self._tasks:
                raise KeyError(task_id)
            self._tasks[task_id]["completed"] = completed

        def remove_task(self, task_id):
            if task_id not in self._tasks:
                raise KeyError(task_id)
            del self._tasks[task_id]

    class FetchOpsGlitchMock(FetchOpsMock):
        def fetch_item(self, item_or_id, output_path, recursive, force, export, dry_run, on_progress):
            on_progress(item_or_id.id, item_or_id.name, 0, 0, "finished")
            on_progress(item_or_id.id, item_or_id.name, 0, 0, "error")

    item = make_item("f-2", name="glitch.docx")
    ops = FetchOpsGlitchMock(item)
    args = Namespace(
        id="f-2",
        path=str(tmp_path),
        recursive=False,
        force=False,
        export=True,
        dry_run=False,
    )

    monkeypatch.setattr(cli_module, "Progress", FakeProgress)
    monkeypatch.setattr(cli_module.sys, "stdout", DummyStdout())

    handle_fetch(args, ops)

    assert any("Success" in msg for msg in printed)
    assert any("Failed" in msg for msg in printed)
