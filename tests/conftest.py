import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from dotenv import load_dotenv

from gdrive_cleaner.drive_core import DriveCore, FileItem
from gdrive_cleaner.operations import DriveOperations

ROOT = Path(__file__).resolve().parents[1]  # Root of the project
SRC = ROOT / "src"
TEST_FOLDER = ROOT / "tests"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def pytest_configure(config):
    load_dotenv(ROOT / ".env", override=False)
    load_dotenv(ROOT / ".env.test", override=True)


@pytest.fixture
def cli_args_factory():
    def _build(defaults: dict, **overrides) -> Namespace:
        data = dict(defaults)
        data.update(overrides)
        return Namespace(**data)

    return _build


@pytest.fixture
def mock_ops() -> DriveOperations:
    return create_autospec(DriveOperations, instance=True)

@pytest.fixture
def mock_drive() -> DriveCore:
    return create_autospec(DriveCore, instance=True)

# @pytest.fixture
# def mock_item() -> FileItem:
#     return create_autospec(FileItem, instance=True)
