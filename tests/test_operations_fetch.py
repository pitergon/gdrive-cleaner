from pathlib import Path

from gdrive_cleaner.drive_core import FileItem
from gdrive_cleaner.operations import DriveOperations

GOOGLE_DOC_MAPPING = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".docx",
    ),
}


class DummyDrive:
    def __init__(self):
        self.download_calls: list[Path] = []
        self.export_calls: list[tuple[str, Path]] = []

    def download_media(self, file_id, destination_path, on_progress=None):
        path = Path(destination_path)
        self.download_calls.append(path)
        path.write_bytes(b"new-content")
        if on_progress:
            on_progress(len(b"new-content"), "progress")
            on_progress(0, "finished")

    def export_media(self, file_id, mime_type, destination_path, on_progress=None):
        path = Path(destination_path)
        self.export_calls.append((mime_type, path))
        path.write_bytes(b"docx-content")
        if on_progress:
            on_progress(len(b"docx-content"), "progress")
            on_progress(0, "finished")


class FinishOnlyExportDrive(DummyDrive):
    def export_media(self, file_id, mime_type, destination_path, on_progress=None):
        path = Path(destination_path)
        self.export_calls.append((mime_type, path))
        path.write_bytes(b"docx-content")
        if on_progress:
            on_progress(0, "finished")


def make_file_item(file_id: str, name: str, size: int, mime_type: str) -> FileItem:
    return FileItem(
        id=file_id,
        name=name,
        size=size,
        mime_type=mime_type,
        created_at=None,
        modified_at=None,
        parents=[],
        owner=None,
    )


def test_download_item_skips_existing_same_size(tmp_path):
    drive = DummyDrive()
    ops = DriveOperations(drive)
    item = make_file_item("f1", "file.txt", 4, "text/plain")
    existing = tmp_path / "file.txt"
    existing.write_bytes(b"1234")
    events = []

    def on_progress(file_id, name, completed, total, status):
        events.append((file_id, status, completed, total))

    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
        on_progress=on_progress,
    )

    assert drive.download_calls == []
    assert events == [("f1", "finished", 4, 4)]


def test_download_item_renames_on_conflict(tmp_path):
    drive = DummyDrive()
    ops = DriveOperations(drive)
    item = make_file_item("abc123", "report.txt", 20, "text/plain")
    (tmp_path / "report.txt").write_bytes(b"old")

    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    assert (tmp_path / "report.txt").read_bytes() == b"old"
    assert (tmp_path / "report_abc123.txt").exists()
    assert (tmp_path / "report_abc123.txt").read_bytes() == b"new-content"


def test_download_item_skips_google_doc_without_export(tmp_path):
    drive = DummyDrive()
    ops = DriveOperations(drive)
    item = make_file_item(
        "doc1",
        "DocName",
        0,
        "application/vnd.google-apps.document",
    )

    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    assert drive.download_calls == []
    assert drive.export_calls == []
    assert not (tmp_path / "DocName.docx").exists()


def test_download_item_exports_google_doc_with_extension(tmp_path):
    drive = DummyDrive()
    ops = DriveOperations(drive)
    item = make_file_item(
        "doc2",
        "DocName",
        0,
        "application/vnd.google-apps.document",
    )

    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=True,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    assert len(drive.export_calls) == 1
    assert (tmp_path / "DocName.docx").exists()


def test_download_item_emits_initial_progress_for_export_when_backend_only_finishes(tmp_path):
    drive = FinishOnlyExportDrive()
    ops = DriveOperations(drive)
    item = make_file_item(
        "doc3",
        "DocName",
        0,
        "application/vnd.google-apps.document",
    )
    events = []

    def on_progress(file_id, name, completed, total, status):
        events.append((status, completed, total))

    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=True,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
        on_progress=on_progress,
    )

    assert events[0] == ("progress", 0, 1)
    assert events[-1][0] == "finished"
