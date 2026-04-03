from argparse import Namespace
from datetime import datetime, timedelta, timezone

import pytest

from gdrive_cleaner.cli import UserInputError, build_parser, get_date_filters, read_ids_file


def test_read_ids_file_csv_reads_unique_ids(tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("id\na\nb\na\n", encoding="utf-8")

    ids = read_ids_file(ids_file)

    assert ids == ["a", "b"]


def test_read_ids_file_without_id_column_returns_empty(tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("name\nx\n", encoding="utf-8")

    ids = read_ids_file(ids_file)

    assert ids == []


def test_get_date_filters_before_after_boundary_logic():
    before = datetime(2025, 1, 10, tzinfo=timezone.utc)
    after = datetime(2025, 1, 1, tzinfo=timezone.utc)
    args = Namespace(older=None, before=before, newer=None, after=after)

    date_before, date_after = get_date_filters(args)

    assert date_before == before
    assert date_after == after + timedelta(days=1)


def test_get_date_filters_rejects_invalid_range():
    before = datetime(2025, 1, 1, tzinfo=timezone.utc)
    after = datetime(2025, 1, 1, tzinfo=timezone.utc)
    args = Namespace(older=None, before=before, newer=None, after=after)

    with pytest.raises(UserInputError):
        get_date_filters(args)


def test_parser_accepts_name_and_contains_filters():
    parser = build_parser()

    args_name = parser.parse_args(["list", "--name", "Report Q1"])
    assert args_name.name == "Report Q1"
    assert args_name.contains is None

    args_contains = parser.parse_args(["delete", "--contains", "Report"])
    assert args_contains.contains == "Report"
    assert args_contains.name is None


def test_parser_accepts_copy_command_with_required_id():
    parser = build_parser()
    args = parser.parse_args(["copy", "file123"])
    assert args.command == "copy"
    assert args.id == "file123"
