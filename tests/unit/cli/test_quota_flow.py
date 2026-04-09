# tests/unit/cli/test_quota_flow.py
import sys

from tests.helpers.cli_args_builders import build_quota_args
from tests.helpers.helpers_drive import as_mock

from gdrive_cleaner.cli import handle_quota


def test_get_quota_flow_calls_ops(mock_ops, mock_console, monkeypatch):

    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    quota_info = {"usage": "100.00 MB", "free": "14.90 GB", "limit": "15.00 GB"}
    mock_ops.get_quota_info.return_value = quota_info
    args = build_quota_args()
    handle_quota(args, mock_ops)
    as_mock(mock_ops.get_quota_info).assert_called_once()

    output = mock_console.get_output()
    assert quota_info["usage"] in output
    assert quota_info["free"] in output
    assert quota_info["limit"] in output
