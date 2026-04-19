from datetime import datetime, timezone

import pytest

from gdrive_cleaner.drive_core import DriveCore, FileFilter


@pytest.fixture
def core() -> DriveCore:
    core = object.__new__(DriveCore)  #  without calling __init__ method
    core.service_account_email = "sa@example.com"
    return core


def test_build_query_default_filter(core):
    query = core._build_query(FileFilter())
    assert "'sa@example.com' in owners" in query
    assert "trashed = false" in query


def test_build_query_full_filter_set(core):
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

    assert "'sa@example.com' in owners" in query
    assert "'folder123' in parents" in query
    assert "name contains 'invoice'" in query
    assert "createdTime > '2025-03-02T00:00:00+00:00'" in query
    assert "createdTime < '2025-03-20T00:00:00+00:00'" in query
    assert "mimeType = 'text/plain'" in query
    assert "mimeType != 'application/vnd.google-apps.folder'" in query
    assert "trashed = false" in query


def test_build_query_name_and_contains_filters(core):
    query = core._build_query(
        FileFilter(
            name_exact="report.csv",
            name_contains="report_",
        )
    )
    assert "name = 'report.csv'" in query
    assert "name contains 'report_'" in query


def test_build_query_escapes_single_quote_and_backslash(core):
    query = core._build_query(FileFilter(name_exact=r"O'Reilly\Docs"))
    assert r"name = 'O\'Reilly\\Docs'" in query


def test_build_query_can_disable_owner_only_and_trashed_filter(core):
    query = core._build_query(FileFilter(owner_only=False, trashed=True))
    assert "owners" not in query
    assert "trashed = false" not in query
