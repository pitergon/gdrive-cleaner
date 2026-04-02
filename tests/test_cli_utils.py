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


def test_parser_accepts_name_and_prefix_filters():
    parser = build_parser()

    args_name = parser.parse_args(["list", "--name", "Report Q1"])
    assert args_name.name == "Report Q1"
    assert args_name.prefix is None

    args_prefix = parser.parse_args(["delete", "--prefix", "Report_"])
    assert args_prefix.prefix == "Report_"
    assert args_prefix.name is None
