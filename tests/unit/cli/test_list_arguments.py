from argparse import Namespace
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from gdrive_cleaner.cli import UserInputError, handle_list
from tests.helpers.cli_args_builders import build_list_args
from tests.helpers.helpers_drive import build_item


@dataclass(frozen=True)
class ListArgsCase:
    name: str
    overrides: dict
    expect_user_input_error: bool
    note: str


CASES: list[ListArgsCase] = [
    ListArgsCase(
        name="invalid-date-range-before-equals-after",
        overrides={
            "before": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "after": datetime(2025, 1, 1, tzinfo=timezone.utc),
        },
        expect_user_input_error=True,
        note="before and after must form a valid range",
    ),
    ListArgsCase(
        name="invalid-date-range-older-with-newer",
        overrides={"older": 1, "newer": 1},
        expect_user_input_error=True,
        note="older/newer computed boundaries must form a valid range",
    ),
    ListArgsCase(
        name="valid-empty-args",
        overrides={},
        expect_user_input_error=False,
        note="list supports no filters",
    ),
    ListArgsCase(
        name="valid-name-filter",
        overrides={"name": "report.txt"},
        expect_user_input_error=False,
        note="name filter is valid for list",
    ),
    ListArgsCase(
        name="valid-date-range-before-after",
        overrides={
            "before": datetime(2025, 1, 10, tzinfo=timezone.utc),
            "after": datetime(2025, 1, 1, tzinfo=timezone.utc),
        },
        expect_user_input_error=False,
        note="before later than after should be accepted",
    ),
    ListArgsCase(
        name="valid-id-folder-path",
        overrides={"id": "folder-123"},
        expect_user_input_error=False,
        note="existing folder id should be accepted",
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_list_argument_matrix(case: ListArgsCase, mock_ops, monkeypatch):
    args = build_list_args(**case.overrides)

    # Keep successful argument cases from failing in downstream branches.
    if args.id:
        mock_ops.get_item.return_value = build_item(args.id, mime_type="application/vnd.google-apps.folder")
    mock_ops.list_files.return_value = []

    if case.expect_user_input_error:
        with pytest.raises(UserInputError):
            handle_list(args, mock_ops)
    else:
        handle_list(args, mock_ops)
