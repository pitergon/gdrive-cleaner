from datetime import datetime, timezone

from gdrive_cleaner.drive_core import DriveCore, FileFilter


def make_core_for_query(service_account_email: str = "sa@example.com") -> DriveCore:
    core = object.__new__(DriveCore)
    core.service_account_email = service_account_email
    return core


def test_build_query_default_filter():
    core = make_core_for_query()
    query = core._build_query(FileFilter())

    assert "'sa@example.com' in owners" in query
    assert "trashed = false" in query


def test_build_query_full_filter_set():
    core = make_core_for_query("svc@example.com")
    query = core._build_query(
        FileFilter(
            folder_id="folder123",
            name_contains="invoice",
            created_after=datetime(2025, 3, 2, 17, 45, tzinfo=timezone.utc),
            created_before=datetime(2025, 3, 20, 9, 0, tzinfo=timezone.utc),
            mime_type="text/plain",
            exclude_mime_type="application/vnd.google-apps.folder",
        )
    )

    assert "'svc@example.com' in owners" in query
    assert "'folder123' in parents" in query
    assert "name contains 'invoice'" in query
    assert "createdTime > '2025-03-02T00:00:00+00:00'" in query
    assert "createdTime < '2025-03-20T00:00:00+00:00'" in query
    assert "mimeType = 'text/plain'" in query
    assert "mimeType != 'application/vnd.google-apps.folder'" in query
    assert "trashed = false" in query


def test_build_query_can_disable_owner_only_and_trashed_filter():
    core = make_core_for_query()
    query = core._build_query(FileFilter(owner_only=False, trashed=True))

    assert "owners" not in query
    assert "trashed = false" not in query

