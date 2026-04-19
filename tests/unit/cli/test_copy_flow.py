# tests/unit/cli/test_copy_flow.py
# Flow tests for `copy`: verify execution paths and side effects.

from argparse import Namespace
from dataclasses import dataclass
from unittest.mock import Mock

import pytest

import gdrive_cleaner.cli as cli_module
from gdrive_cleaner.cli import handle_copy
from gdrive_cleaner.drive_core import FileItem
from gdrive_cleaner.operations import DriveOperations
from tests.helpers.cli_args_builders import build_copy_args
from tests.helpers.helpers_drive import build_item
from tests.helpers.helpers_mock import as_mock


@dataclass
class CopySetup:
    args: Namespace
    item: FileItem
    target: FileItem | None
    copied: FileItem | None


@pytest.fixture
def copy_setup(mock_ops):
    def _make(**overrides) -> CopySetup:
        item = overrides.pop(
            "item",
            build_item(file_id="src-id", name="file.txt", mime_type="text/plain"),
        )
        target = overrides.pop(
            "target",
            build_item(file_id="dst-id", name="destination", mime_type="application/vnd.google-apps.folder"),
        )
        copied = overrides.pop(
            "copied",
            build_item(file_id="copied-id", name="copied_file.txt", mime_type="text/plain"),
        )
        args_defaults = {
            "id": "src-id",
            "target_id": "dst-id",
            "name": "copied_file.txt",
            "force": True,
        }
        args_defaults.update(overrides)
        args = build_copy_args(**args_defaults)

        # Keep list-based side_effect as requested.
        get_item_side_effect = [item]
        if args.target_id:
            get_item_side_effect.append(target)

        as_mock(mock_ops.get_item).side_effect = get_item_side_effect
        as_mock(mock_ops.copy_file).return_value = copied

        return CopySetup(args=args, item=item, target=target, copied=copied)

    return _make


def test_copy_success_calls_copy_file_and_prints_completion(mock_ops: DriveOperations, mock_error_console, copy_setup):
    setup = copy_setup()

    handle_copy(setup.args, mock_ops)

    as_mock(mock_ops.copy_file).assert_called_once()
    call_kwargs = as_mock(mock_ops.copy_file).call_args.kwargs
    assert call_kwargs["file_id"] == "src-id"
    assert call_kwargs["new_name"] == "copied_file.txt"
    assert call_kwargs["target_id"] == "dst-id"
    assert "Command 'copy' completed." in mock_error_console.get_output()


def test_copy_without_name_uses_default_copy_prefix(mock_ops: DriveOperations, copy_setup):
    setup = copy_setup(name=None, target_id=None)

    handle_copy(setup.args, mock_ops)

    as_mock(mock_ops.copy_file).assert_called_once()
    call_kwargs = as_mock(mock_ops.copy_file).call_args.kwargs
    assert call_kwargs["file_id"] == "src-id"
    assert setup.args.name is None
    assert call_kwargs["new_name"] == f"Copy of {setup.item.name}"


def test_copy_cancelled_by_confirmation_does_not_copy(
    mock_ops: DriveOperations, mock_error_console, monkeypatch, copy_setup
):
    setup = copy_setup(force=False, target_id=None)
    monkeypatch.setattr(cli_module, "confirm_copying", Mock(return_value=False))

    handle_copy(setup.args, mock_ops)

    as_mock(mock_ops.copy_file).assert_not_called()
    assert "Command 'copy' cancelled." in mock_error_console.get_output()


def test_copy_dry_run_does_not_copy(mock_ops: DriveOperations, mock_error_console, copy_setup):
    setup = copy_setup(dry_run=True, target_id=None, force=False)

    handle_copy(setup.args, mock_ops)

    as_mock(mock_ops.copy_file).assert_not_called()
    assert "Dry run" in mock_error_console.get_output()


def test_copy_source_not_found_raises_user_input_error(mock_ops):
    args = build_copy_args(id="src-id", force=True)
    as_mock(mock_ops.get_item).return_value = None

    with pytest.raises(cli_module.UserInputError, match="Item with ID 'src-id' not found."):
        handle_copy(args, mock_ops)


def test_copy_source_is_folder_raises_user_input_error(mock_ops):
    item = build_item(file_id="src-id", name="folder", mime_type="application/vnd.google-apps.folder")
    args = build_copy_args(id="src-id", force=True)
    as_mock(mock_ops.get_item).return_value = item

    with pytest.raises(cli_module.UserInputError, match="Copying folders is not supported."):
        handle_copy(args, mock_ops)


def test_copy_target_id_not_found_raises_user_input_error(mock_ops, copy_setup):
    setup = copy_setup(target=None)

    with pytest.raises(cli_module.UserInputError, match="Folder 'dst-id' not found."):
        handle_copy(setup.args, mock_ops)


def test_copy_target_id_not_folder_raises_user_input_error(mock_ops, copy_setup):
    not_folder = build_item(file_id="dst-id", name="destination", mime_type="text/plain")
    setup = copy_setup(target=not_folder)

    with pytest.raises(cli_module.UserInputError, match="ID 'dst-id' is not a folder."):
        handle_copy(setup.args, mock_ops)


def test_copy_copied_item_missing_metadata_raises_user_input_error(mock_ops, copy_setup):
    setup = copy_setup(target_id=None)
    as_mock(mock_ops.copy_file).return_value = None

    with pytest.raises(cli_module.UserInputError, match="Copy finished but copied item metadata is unavailable."):
        handle_copy(setup.args, mock_ops)
