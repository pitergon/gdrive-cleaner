from gdrive_cleaner.drive_core import DriveCore
from gdrive_cleaner.operations import DriveOperations
from tests.helpers.helpers_mock import as_mock


def test_quota_returns_correct_info(mock_drive: DriveCore):

    as_mock(mock_drive.get_quota).return_value = {
        "limit": 15 * 1024**3,   # 15 GB
        "usage": 5 * 1024**3,    # 5 GB
        "free": 10 * 1024**3,    # 10 GB
    }

    ops = DriveOperations(mock_drive)
    result = ops.get_quota_info()

    assert result == {
        "limit": "15.00 GB",
        "usage": "5.00 GB",
        "free": "10.00 GB",
    }

def test_quota_returns_na_if_error(mock_drive: DriveCore):
    as_mock(mock_drive.get_quota).side_effect = Exception("Invalid service account")

    ops = DriveOperations(mock_drive)
    result = ops.get_quota_info()

    assert result == {"limit": "N/A", "usage": "N/A", "free": "N/A"}
