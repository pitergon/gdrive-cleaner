# tests/unit/cli/test_clear_folder_arguments.py
# Argument-rule tests for `clear_folder`: verify handler-level input contracts
# (allowed/forbidden argument combinations and required filters).
# Focused on validation logic before execution side effects.

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from gdrive_cleaner.cli import UserInputError, handle_clear_folder
from gdrive_cleaner.operations import DriveOperations
from tests.helpers.cli_args_builders import build_clear_folder_args
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock


@dataclass(frozen=True)
class ClearArgsCase:
    name: str
    overrides: dict
    expect_user_input_error: bool
    note: str


CASES: list[ClearArgsCase] = [
    ClearArgsCase(
        name="valid-folder-id",
        overrides={"folder_id": "folder-123", "dry_run": True},
        expect_user_input_error=False,
        note="Valid folder_id argument combination should pass argument checks",
    ),
    ClearArgsCase(
        name="valid-name-filter",
        overrides={"folder_id": "folder-123", "name": "documents.txt", "dry_run": True},
        expect_user_input_error=False,
        note="Valid name-filter argument combination should pass argument checks",
    ),
    ClearArgsCase(
        name="valid-contains-filter",
        overrides={"folder_id": "folder-123", "contains": "documents", "dry_run": True},
        expect_user_input_error=False,
        note="Valid name-filter argument combination should pass argument checks",
    ),
    ClearArgsCase(
        name="valid-date-range",
        overrides={
            "folder_id": "folder-123",
            "before": datetime(2025, 2, 1, tzinfo=timezone.utc),
            "after": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "dry_run": True
        },
        expect_user_input_error=False,
        note="Date range must be logically valid",
    ),
    ClearArgsCase(
        name="invalid-date-range",
        overrides={
            "folder_id": "folder-123",
            "before": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "after": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "dry_run": True
        },
        expect_user_input_error=True,
        note="Date range must be logically valid",
    ),
    ClearArgsCase(
        name="valid-older-newer-range",
        overrides={
            "folder_id": "folder-123",
            "older": 10,
            "newer": 15,
            "dry_run": True
        },
        expect_user_input_error=False,
        note="Date range must be logically valid",
    ),
    ClearArgsCase(
        name="invalid-older-newer-range",
        overrides={
            "folder_id": "folder-123",
            "older": 15,
            "newer": 10,
            "dry_run": True
        },
        expect_user_input_error=True,
        note="Date range must be logically valid",
    )
]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_clear_folder_argument_cases(case: ClearArgsCase, mock_ops: DriveOperations):
    args = build_clear_folder_args(**case.overrides)
    # Keep successful argument cases from failing in unrelated downstream logic.
    folder_item = build_item(args.folder_id, name="Test Folder", mime_type="application/vnd.google-apps.folder")
    as_mock(mock_ops.get_item).return_value = folder_item
    if case.expect_user_input_error:
        with pytest.raises(UserInputError):
            handle_clear_folder(args, mock_ops)
    else:
        handle_clear_folder(args, mock_ops)
