from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import Mock

from gdrive_cleaner.drive_core import DriveCore, FileItem


def as_mock(obj: Any) -> Mock:
    return cast(Mock, obj)  # type: ignore


def build_item(
    file_id: str, name: str = "name", mime_type: str = "text/plain", size: int = 10
) -> FileItem:
    return FileItem(
        id=file_id,
        name=name,
        size=size,
        mime_type=mime_type,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        modified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        parents=[],
        owner=None,
    )


def setup_test_structure(
    drive: DriveCore, root_folder_id: str, name_suffix: str | None = None
) -> dict[str, str]:
    suffix = f"_{name_suffix}" if name_suffix else ""
    test_folder_name = f"Test_Folder{suffix}"
    subfolder_l1_name = f"Subfolder_l1{suffix}"
    subfolder_l2_name = f"Subfolder_l2{suffix}"

    test_folder_id = drive.create_item(
        test_folder_name,
        "application/vnd.google-apps.folder",
        parent_id=root_folder_id,
    )
    subfolder_l1_id = drive.create_item(
        subfolder_l1_name,
        "application/vnd.google-apps.folder",
        parent_id=test_folder_id,
    )
    subfolder_l2_id = drive.create_item(
        subfolder_l2_name,
        "application/vnd.google-apps.folder",
        parent_id=subfolder_l1_id,
    )

    files_to_create = [
        {"name": "test_file.txt", "parent": test_folder_id, "content": "Hello Root"},
        {"name": "sub_file_l1.txt", "parent": subfolder_l1_id, "content": "Content 1"},
        {"name": "sub_file_l2.txt", "parent": subfolder_l2_id, "content": "Content 2"},
        {"name": "test_file.txt", "parent": subfolder_l1_id, "content": "I am a shadow"},
    ]

    for file_payload in files_to_create:
        drive.create_item(
            file_payload["name"],
            "text/plain",
            file_payload["parent"],
            file_payload["content"],
        )

    return {
        "root_folder_id": test_folder_id,
        "root_folder_name": test_folder_name,
        "subfolder_l1_id": subfolder_l1_id,
        "subfolder_l1_name": subfolder_l1_name,
        "subfolder_l2_id": subfolder_l2_id,
        "subfolder_l2_name": subfolder_l2_name,
    }
