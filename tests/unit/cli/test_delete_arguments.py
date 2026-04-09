# tests/unit/cli/test_delete_arguments.py
# Argument-rule tests for `delete`: verify handler-level input contracts
# (allowed/forbidden argument combinations and required filters).
# Focused on validation logic before execution side effects.

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from gdrive_cleaner.cli import UserInputError, handle_delete
from tests.helpers.cli_args_builders import build_delete_args


@dataclass(frozen=True)
class DeleteArgsCase:
    name: str
    args_overrides: dict
    expect_user_input_error: bool
    note: str


CASES: list[DeleteArgsCase] = [
    DeleteArgsCase(
        name="conflict-id-with-older",
        args_overrides={"id": "123", "older": 10},
        expect_user_input_error=True,
        note="ID filter cannot be combined with API date filters",
    ),
    DeleteArgsCase(
        name="conflict-id-with-before",
        args_overrides={"id": "123", "before": datetime(2025, 1, 10, tzinfo=timezone.utc)},
        expect_user_input_error=True,
        note="ID filter cannot be combined with API date filters",
    ),
    DeleteArgsCase(
        name="conflict-id-with-name",
        args_overrides={"id": "123", "name": "report.txt"},
        expect_user_input_error=True,
        note="ID filter cannot be combined with API name filters",
    ),
    DeleteArgsCase(
        name="conflict-id-with-contains",
        args_overrides={"id": "123", "contains": "report"},
        expect_user_input_error=True,
        note="ID filter cannot be combined with API name filters",
    ),
    DeleteArgsCase(
        name="missing-all-filters",
        args_overrides={},
        expect_user_input_error=True,
        note="At least one filter is required",
    ),
    DeleteArgsCase(
        name="invalid-date-range",
        args_overrides={
            "before": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "after": datetime(2025, 1, 1, tzinfo=timezone.utc),
        },
        expect_user_input_error=True,
        note="Date range must be logically valid",
    ),
    DeleteArgsCase(
        name="valid-id-path",
        args_overrides={"id": "ok-id", "dry_run": True},
        expect_user_input_error=False,
        note="Valid ID-driven delete path should pass argument checks",
    ),
    DeleteArgsCase(
        name="valid-name-path",
        args_overrides={"name": "report.txt", "dry_run": True},
        expect_user_input_error=False,
        note="Valid API-filter path should pass argument checks",
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_delete_argument_matrix(case: DeleteArgsCase, mock_ops):
    args = build_delete_args(**case.args_overrides)

    # Keep successful argument cases from failing in unrelated downstream logic.
    mock_ops.get_items_batch.return_value = {}
    mock_ops.list_files.return_value = []

    if case.expect_user_input_error:
        with pytest.raises(UserInputError):
            handle_delete(args, mock_ops)
    else:
        handle_delete(args, mock_ops)
