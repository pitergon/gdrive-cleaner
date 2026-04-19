import os
import uuid
from pathlib import Path

import pytest
from googleapiclient.errors import HttpError

from gdrive_cleaner.drive_core import DriveCore, FileFilter
from gdrive_cleaner.operations import DriveOperations
from tests.helpers.helpers_drive import setup_test_structure

pytestmark = pytest.mark.integration


def _skip_on_blocked_network(exc: Exception) -> None:
    if isinstance(exc, PermissionError) and getattr(exc, "winerror", None) == 10013:
        pytest.skip("Network/socket access is blocked in this environment (WinError 10013)")
    raise exc


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


@pytest.fixture
def drive() -> DriveCore:
    if os.environ.get("RUN_GDRIVE_INTEGRATION") != "1":
        pytest.skip("RUN_GDRIVE_INTEGRATION=1 is required to run integration tests")
    sa_path = Path(_require_env("GOOGLE_APPLICATION_CREDENTIALS")).expanduser().resolve()
    if not sa_path.exists():
        pytest.skip(f"Credentials file not found: {sa_path}")
    return DriveCore(str(sa_path))


@pytest.fixture
def ops(drive: DriveCore) -> DriveOperations:
    return DriveOperations(drive)


def test_drive_core_real_create_list_get_delete_cycle(drive: DriveCore):
    parent_id = _require_env("TEST_DRIVE_FOLDER_ID")
    run_id = uuid.uuid4().hex[:8]

    test_folder_name = f"gdc_it_{run_id}"
    test_file_name = f"file_{run_id}.txt"

    try:
        folder_id = drive.create_item(
            name=test_folder_name,
            mime_type="application/vnd.google-apps.folder",
            parent_id=parent_id,
        )
    except Exception as exc:
        _skip_on_blocked_network(exc)
        raise  # for static analyzer
    try:
        file_id = drive.create_item(
            name=test_file_name,
            mime_type="text/plain",
            parent_id=folder_id,
            content="integration test content",
        )

        items = drive.list_files(FileFilter(folder_id=folder_id))
        names = {item.name for item in items}
        ids = {item.id for item in items}

        assert test_file_name in names
        assert file_id in ids

        meta = drive.get_file_metadata(file_id)
        assert meta is not None
        assert meta.id == file_id
        assert meta.name == test_file_name

        delete_result = drive.delete_ids([file_id])
        assert delete_result.total == 1
        assert delete_result.failed == 0
        assert delete_result.success == 1

        deleted_meta = drive.get_file_metadata(file_id)
        assert deleted_meta is None

    finally:
        # Folder delete is recursive at Drive level; this cleans leftovers on partial failures.
        try:
            drive.delete_ids([folder_id])
        except Exception:
            pass


def test_fetch_item_recursive_uses_nested_structure(
    drive: DriveCore, ops: DriveOperations, tmp_path: Path
):
    parent_id = _require_env("TEST_DRIVE_FOLDER_ID")
    run_id = uuid.uuid4().hex[:8]
    try:
        created = setup_test_structure(drive, parent_id, name_suffix=run_id)
    except Exception as exc:
        _skip_on_blocked_network(exc)
        raise  # for static analyzer

    root_folder_id = created["root_folder_id"]
    root_folder_name = created["root_folder_name"]
    subfolder_l1_name = created["subfolder_l1_name"]
    subfolder_l2_name = created["subfolder_l2_name"]

    root_item = drive.get_file_metadata(root_folder_id)
    assert root_item is not None

    non_recursive_output = tmp_path / "non_recursive"
    recursive_output = tmp_path / "recursive"

    try:
        ops.fetch_item(root_item, non_recursive_output, recursive=False)
        ops.fetch_item(root_item, recursive_output, recursive=True)

        non_recursive_root = non_recursive_output / root_folder_name
        recursive_root = recursive_output / root_folder_name

        assert (non_recursive_root / "test_file.txt").exists()
        assert not (non_recursive_root / subfolder_l1_name / "sub_file_l1.txt").exists()

        assert (recursive_root / "test_file.txt").exists()
        assert (recursive_root / subfolder_l1_name / "sub_file_l1.txt").exists()
        assert (recursive_root / subfolder_l1_name / "test_file.txt").exists()
        assert (recursive_root / subfolder_l1_name / subfolder_l2_name / "sub_file_l2.txt").exists()
    finally:
        drive.delete_ids([root_folder_id])


def test_drive_core_create_copy_cycle(drive: DriveCore):
    parent_id = _require_env("TEST_DRIVE_FOLDER_ID")
    run_id = uuid.uuid4().hex[:8]

    test_folder_name = f"gdc_it_copy_{run_id}"
    test_file_name = f"file_{run_id}.txt"
    new_file_name = f"copy_of_{test_file_name}"
    try:
        folder_id = drive.create_item(
            name=test_folder_name,
            mime_type="application/vnd.google-apps.folder",
            parent_id=parent_id,
        )
    except Exception as exc:
        _skip_on_blocked_network(exc)
        raise  # for static analyzer
    try:
        file_id = drive.create_item(
            name=test_file_name,
            mime_type="text/plain",
            parent_id=folder_id,
            content="integration test content",
        )

        result = drive.copy_file(file_id=file_id, new_name=new_file_name, target_id=folder_id)
        print(f"Copy result: {result}")
        items = drive.list_files(FileFilter(folder_id=folder_id))
        names = {item.name for item in items}
        ids = {item.id for item in items}

        assert result.keys() >= {"id", "name", "parents"}

        assert test_file_name in names
        assert new_file_name in names
        assert file_id in ids
        assert result["id"] in ids
        assert result["name"] == new_file_name

    finally:
        # Folder delete is recursive at Drive level; this cleans leftovers on partial failures.
        try:
            drive.delete_ids([folder_id])
        except Exception:
            pass


def test_404_on_folder_id_raises_exception(drive: DriveCore):
    file_filter = FileFilter(folder_id="nonexistent_folder_id")

    with pytest.raises(HttpError) as exc_info:
        drive.list_files(file_filter=file_filter)

    assert exc_info.value.resp.status == 404
