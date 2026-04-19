# tests/unit/drive_core/test_drive_core_delete.py

from unittest.mock import Mock

from googleapiclient.errors import HttpError
import pytest

from gdrive_cleaner.drive_core import DriveCore


class FakeBatch:
    def __init__(self, callback):
        self.callback = callback
        self.request_ids = []

    def add(self, request, request_id):
        self.request_ids.append(request_id)


class FakeFiles:
    def delete(self, fileId):
        return ("delete", fileId)


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


def test_delete_ids_dedup_batches_and_progress(fake_drive: DriveCore):

    progress_calls = []
    seen_chunks = []

    def fake_execute_batch(batch):
        seen_chunks.append(list(batch.request_ids))
        for rid in batch.request_ids:
            batch.callback(rid, {}, None)

    fake_drive._execute_batch = fake_execute_batch

    result = fake_drive.delete_ids(
        ids=["a", "b", "a", "c", "d"],
        batch_size=2,
        on_progress=lambda n: progress_calls.append(n),
    )

    assert seen_chunks == [["a", "b"], ["c", "d"]]
    assert progress_calls == [2, 2]
    assert result.total == 4
    assert result.success == 4
    assert result.failed == 0


def test_delete_ids_404_is_success_and_other_error_is_failed(fake_drive):

    def fake_execute_batch(batch):
        # ok -> success
        batch.callback("ok", {}, None)

        # 404 -> already deleted, success
        resp_404 = type("Resp", (), {"status": 404, "reason": "Not Found"})()
        batch.callback("gone", None, HttpError(resp=resp_404, content=b"Not Found"))

        # 500 -> failed
        resp_500 = type("Resp", (), {"status": 500, "reason": "Internal Error"})()
        batch.callback("bad", None, HttpError(resp=resp_500, content=b"Internal Error"))

    fake_drive._execute_batch = fake_execute_batch

    result = fake_drive.delete_ids(ids=["ok", "gone", "bad"], batch_size=100)

    assert result.total == 3
    assert result.success == 2
    assert result.failed == 1

    by_id = {e.id: e for e in result.entries}
    assert by_id["ok"].status == "success"
    assert by_id["gone"].status == "success"   # 404 treated as success
    assert by_id["bad"].status == "error"
    assert by_id["bad"].error is not None
