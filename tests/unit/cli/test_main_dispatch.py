from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from unittest.mock import Mock

import pytest

import gdrive_cleaner.cli as cli_module
from gdrive_cleaner.cli import UserInputError
from gdrive_cleaner.drive_core import DriveCore
from gdrive_cleaner.operations import DriveOperations


@dataclass
class MainFlowSetup:
    sa_path: Path
    drive: DriveCore
    ops: DriveOperations
    drive_builder: Mock
    ops_builder: Mock
    handlers: dict[str, Mock]


@pytest.fixture
def main_flow_setup(monkeypatch, tmp_path, mock_drive, mock_ops) -> Callable[..., MainFlowSetup]:
    def _make(argv: list[str]) -> MainFlowSetup:
        sa_path = tmp_path / "sa.json"
        sa_path.write_text('{"type":"service_account"}', encoding="utf-8")

        drive_builder = Mock(return_value=mock_drive)
        ops_builder = Mock(return_value=mock_ops)
        monkeypatch.setattr(cli_module, "DriveCore", drive_builder)
        monkeypatch.setattr(cli_module, "DriveOperations", ops_builder)
        monkeypatch.setattr(cli_module.sys, "argv", ["gdrive-cleaner", *argv, "--sa", str(sa_path)])

        handlers = {
            "handle_list": Mock(),
            "handle_delete": Mock(),
            "handle_clear_folder": Mock(),
            "handle_fetch": Mock(),
            "handle_copy": Mock(),
            "handle_quota": Mock(),
        }
        for name, handler_mock in handlers.items():
            monkeypatch.setattr(cli_module, name, handler_mock)

        return MainFlowSetup(
            sa_path=sa_path,
            drive=mock_drive,
            ops=mock_ops,
            drive_builder=drive_builder,
            ops_builder=ops_builder,
            handlers=handlers,
        )

    return _make


@pytest.mark.parametrize(
    ("command_argv", "handler_name"),
    [
        (["list"], "handle_list"),
        (["delete", "id-1"], "handle_delete"),
        (["clear-folder", "folder-1"], "handle_clear_folder"),
        (["fetch", "id-1"], "handle_fetch"),
        (["copy", "id-1"], "handle_copy"),
        (["quota"], "handle_quota"),
    ],
    ids=["list", "delete", "clear-folder", "fetch", "copy", "quota"],
)
def test_main_dispatches_to_expected_handler(
    command_argv, handler_name, main_flow_setup
):
    setup = main_flow_setup(command_argv)

    cli_module.main()

    handler = setup.handlers[handler_name]
    handler.assert_called_once()

    for name, handler_mock in setup.handlers.items():
        if name != handler_name:
            handler_mock.assert_not_called()

    parsed_args, ops_instance = handler.call_args.args
    assert parsed_args.command == command_argv[0]
    assert ops_instance is setup.ops
    setup.drive_builder.assert_called_once_with(str(setup.sa_path.resolve()))
    setup.ops_builder.assert_called_once_with(setup.drive)


def test_main_handles_user_input_error_and_exits(main_flow_setup, mock_error_console):
    setup = main_flow_setup(["list"])
    setup.handlers["handle_list"].side_effect = UserInputError("bad input")

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 1
    assert "Error during command 'list': bad input" in mock_error_console.get_output()


def test_main_handles_unexpected_error_and_exits(main_flow_setup, mock_error_console):
    setup = main_flow_setup(["list"])
    setup.handlers["handle_list"].side_effect = RuntimeError("boom")

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 1
    assert "Command 'list' failed. boom Use -vv for details." in mock_error_console.get_output()


def test_main_exits_when_service_account_is_missing(monkeypatch, tmp_path, mock_error_console):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.chdir(tmp_path)

    drive_builder = Mock()
    ops_builder = Mock()
    monkeypatch.setattr(cli_module, "DriveCore", drive_builder)
    monkeypatch.setattr(cli_module, "DriveOperations", ops_builder)
    monkeypatch.setattr(cli_module.sys, "argv", ["gdrive-cleaner", "list"])

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 1
    assert "Error: Service account file not found:" in mock_error_console.get_output()
    drive_builder.assert_not_called()
    ops_builder.assert_not_called()
