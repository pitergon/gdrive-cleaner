from pathlib import Path

from helpers.helpers_drive import build_item
from helpers.helpers_mock import as_mock

from gdrive_cleaner.drive_core import DriveCore
from gdrive_cleaner.operations import DriveOperations

GOOGLE_DOC_MAPPING = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".docx",
    ),
}


def test_download_item_skips_existing_same_size(tmp_path, mock_drive: DriveCore):
    item = build_item(file_id="f1", name="file.txt", size=4, mime_type="text/plain")
    existing_file = tmp_path / "file.txt"
    existing_file.write_bytes(b"1234")

    ops = DriveOperations(mock_drive)
    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    as_mock(mock_drive.download_media).assert_not_called()


def test_download_reports_finished_when_skipped(tmp_path, mock_drive: DriveCore):
    item = build_item(file_id="f1", name="file.txt", size=4, mime_type="text/plain")
    existing_file = tmp_path / "file.txt"
    existing_file.write_bytes(b"1234")

    def on_progress(file_id, name, completed, total, status):
        assert file_id == "f1"
        assert name == "file.txt"
        assert completed == 4
        assert total == 4
        assert status == "finished"

    ops = DriveOperations(mock_drive)
    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
        on_progress=on_progress,
    )


def test_download_item_renames_on_conflict(tmp_path, mock_drive):

    item = build_item(file_id="id-123", name="file.txt", size=4, mime_type="text/plain")
    existing_file = tmp_path / "file.txt"
    existing_file.write_bytes(b"existing_content")

    def download_media(file_id, destination_path, on_progress=None):
        path = Path(destination_path)
        path.write_bytes(b"new-content")
        if on_progress:
            on_progress(len(b"new-content"), "progress")
            on_progress(0, "finished")

    as_mock(mock_drive.download_media).side_effect = download_media

    ops = DriveOperations(mock_drive)
    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    assert (tmp_path / "file.txt").read_bytes() == b"existing_content"
    assert (tmp_path / "file_id-123.txt").exists()
    assert (tmp_path / "file_id-123.txt").read_bytes() == b"new-content"


def test_download_item_skips_google_doc_without_export(tmp_path, mock_drive: DriveCore):

    item = build_item(file_id="doc1", name="DocName", size=0, mime_type="application/vnd.google-apps.document")

    ops = DriveOperations(mock_drive)
    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=False,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    assert not (tmp_path / "DocName.docx").exists()
    as_mock(mock_drive.download_media).assert_not_called()
    as_mock(mock_drive.export_media).assert_not_called()


def test_download_item_exports_google_doc_with_extension(tmp_path, mock_drive: DriveCore):

    item = build_item(file_id="doc1", name="DocName", size=0, mime_type="application/vnd.google-apps.document")

    def export_media(file_id, mime_type, destination_path, on_progress=None):
        path = Path(destination_path)
        path.write_bytes(b"docx-content")
        if on_progress:
            on_progress(0, "finished")

    as_mock(mock_drive.export_media).side_effect = export_media

    ops = DriveOperations(mock_drive)
    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=True,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
    )

    as_mock(mock_drive.export_media).assert_called_once()
    assert (tmp_path / "DocName.docx").exists()


def test_download_item_emits_initial_progress_for_export_when_backend_only_finishes(tmp_path, mock_drive):

    item = build_item(file_id="doc1", name="DocName", size=0, mime_type="application/vnd.google-apps.document")

    def export_media(file_id, mime_type, destination_path, on_progress=None):
        path = Path(destination_path)
        path.write_bytes(b"docx-content")
        if on_progress:
            on_progress(0, "finished")
    as_mock(mock_drive.export_media).side_effect = export_media

    ui_events = []
    def on_progress(file_id, name, completed, total, status):
        ui_events.append((status, completed, total))

    ops = DriveOperations(mock_drive)
    ops._download_item(
        item=item,
        target_dir=tmp_path,
        force=False,
        export=True,
        mapping=GOOGLE_DOC_MAPPING,
        dry_run=False,
        on_progress=on_progress,
    )

    assert ui_events[0] == ("progress", 0, 1)
    assert ui_events[-1][0] == "finished"


def test_fetch_item_dry_run_folder_emits_dry_run_status_and_does_not_create_dir(tmp_path, mock_drive: DriveCore):

    folder = build_item(file_id="folder-id", name="my-folder", size=0, mime_type="application/vnd.google-apps.folder")

    ui_events = []
    def on_progress(file_id, name, completed, total, status):
        ui_events.append((file_id, name, status))

    as_mock(mock_drive.list_files).return_value=[]

    ops = DriveOperations(mock_drive)
    ops.fetch_item(
        item_or_id=folder,
        output_path=tmp_path,
        recursive=False,
        force=False,
        export=False,
        dry_run=True,
        on_progress=on_progress,
    )

    assert ui_events == [("folder-id", "my-folder", "dry_run")]
    assert not (tmp_path / "my-folder").exists()

