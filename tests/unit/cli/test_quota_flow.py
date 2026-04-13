# tests/unit/cli/test_quota_flow.py
import sys

import pytest
from helpers.helpers_mock import as_mock

from gdrive_cleaner.cli import handle_quota
from tests.helpers.cli_args_builders import build_quota_args


@pytest.mark.parametrize("is_tty", [True, False])
def test_get_quota_flow_calls_ops(mock_ops, mock_console, monkeypatch, is_tty):

    monkeypatch.setattr(sys.stdout, "isatty", lambda: is_tty)
    quota_info = {"usage": "100.00 MB", "free": "14.90 GB", "limit": "15.00 GB"}
    as_mock(mock_ops.get_quota_info).return_value = quota_info
    args = build_quota_args()
    handle_quota(args, mock_ops)
    as_mock(mock_ops.get_quota_info).assert_called_once()

    output = mock_console.get_output()
    assert quota_info["usage"] in output
    assert quota_info["free"] in output
    assert quota_info["limit"] in output
