# tests/unit/drive_core/test_drive_core_batch_metadata.py
from unittest.mock import Mock

import pytest

from gdrive_cleaner.drive_core import DriveCore, FileItem


class FakeBatch:
    def __init__(self, callback):
        self.callback = callback
        self.request_ids = []

    def add(self, request, request_id):
        self.request_ids.append(request_id)


class FakeFiles:
    def __init__(self):
        self.get_calls = []

    def get(self, fileId, fields):
        self.get_calls.append(fileId)
        return {"fileId": fileId, "fields": fields}


class FakeService:
    def __init__(self):
        self.files_api = FakeFiles()

    def files(self):
        return self.files_api

    def new_batch_http_request(self, callback):
        return FakeBatch(callback)


@pytest.fixture
def fake_drive() -> DriveCore:
    drive = object.__new__(DriveCore)
    drive.service = FakeService()
    drive.logger = Mock()
    return drive


def _ok_response(file_id: str) -> dict:
    return {
        "id": file_id,
        "name": f"name-{file_id}",
        "size": "10",
        "mimeType": "text/plain",
        "parents": [],
        "createdTime": "2025-01-01T00:00:00Z",
        "modifiedTime": "2025-01-01T00:00:00Z",
        "owners": [{"emailAddress": "owner@example.com"}],
    }


def test_get_files_metadata_batch_respects_limit_and_batch_size(fake_drive: DriveCore):
    seen_chunks = []

    def fake_execute_batch(batch):
        seen_chunks.append(list(batch.request_ids))
        for rid in batch.request_ids:
            batch.callback(rid, _ok_response(rid), None)

    fake_drive._execute_batch = fake_execute_batch

    result = fake_drive.get_files_metadata_batch(
        ids=["a", "b", "c", "d"],
        limit=3,
        batch_size=2,
    )

    assert seen_chunks == [["a", "b"], ["c"]]
    assert fake_drive.service.files_api.get_calls == ["a", "b", "c"]
    assert list(result.keys()) == ["a", "b", "c"]
    assert isinstance(result["a"], FileItem)
    assert isinstance(result["b"], FileItem)
    assert isinstance(result["c"], FileItem)


def test_get_files_metadata_batch_keeps_none_for_failed_id_callback(fake_drive: DriveCore):

    def fake_execute_batch(batch):
        for rid in batch.request_ids:
            if rid == "bad":
                batch.callback(rid, None, Exception("boom"))
            else:
                batch.callback(rid, _ok_response(rid), None)

    fake_drive._execute_batch = fake_execute_batch

    result = fake_drive.get_files_metadata_batch(
        ids=["ok1", "bad", "ok2"],
        batch_size=10,
    )

    assert isinstance(result["ok1"], FileItem)
    assert result["bad"] is None
    assert isinstance(result["ok2"], FileItem)
