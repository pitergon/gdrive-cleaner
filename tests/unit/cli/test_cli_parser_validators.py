# tests\unit\cli\test_cli_parser_validators.py 
# Parser-level CLI tests: verify argparse wiring for typed/validated options
# (path/date/int/extension and mutually-exclusive groups) across commands.
# These tests check input parsing/validation only, not handler business flow.


import argparse
import io
from contextlib import redirect_stderr
from dataclasses import dataclass

import pytest

from gdrive_cleaner.cli import build_parser, valid_date, valid_path


@dataclass(frozen=True)
class ParserCase:
    name: str
    argv: list[str]
    reason: str


def assert_parse_error(argv: list[str], reason: str = "") -> None:
    parser = build_parser()
    with redirect_stderr(io.StringIO()):  # disable output parser help message to stderr
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(argv)
        assert exc_info.value.code == 2, reason


def parse_ok(argv: list[str]):
    parser = build_parser()
    return parser.parse_args(argv)


# direct validators
def test_valid_date_accepts_iso_date():
    parsed = valid_date("2025-01-31")
    assert parsed.year == 2025
    assert parsed.month == 1
    assert parsed.day == 31
    assert parsed.tzinfo is not None


@pytest.mark.parametrize("raw", ["31-01-2025", "2025/01/31", "not-a-date"])
def test_valid_date_rejects_invalid_format(raw):
    with pytest.raises(argparse.ArgumentTypeError):
        valid_date(raw)


def test_valid_path_accepts_existing_file(tmp_path):
    path = tmp_path / "existing.txt"
    path.write_text("x", encoding="utf-8")
    assert valid_path(str(path)) == str(path)


def test_valid_path_rejects_missing_file(tmp_path):
    missing = tmp_path / "missing.txt"
    with pytest.raises(argparse.ArgumentTypeError):
        valid_path(str(missing))


# parser wiring: --sa across all commands
SA_MISSING_CASES: list[ParserCase] = [
    ParserCase("list-missing-sa", ["list", "--sa", "MISSING"], "base --sa should validate path"),
    ParserCase("delete-missing-sa", ["delete", "--sa", "MISSING"], "base --sa should validate path"),
    ParserCase(
        "clear-folder-missing-sa",
        ["clear-folder", "folder-1", "--sa", "MISSING"],
        "base --sa should validate path",
    ),
    ParserCase("fetch-missing-sa", ["fetch", "id-1", "--sa", "MISSING"], "base --sa should validate path"),
    ParserCase("copy-missing-sa", ["copy", "id-1", "--sa", "MISSING"], "base --sa should validate path"),
    ParserCase("quota-missing-sa", ["quota", "--sa", "MISSING"], "base --sa should validate path"),
]


@pytest.mark.parametrize("case", SA_MISSING_CASES, ids=lambda c: c.name)
def test_parser_rejects_missing_sa_for_all_commands(case, tmp_path):
    missing = tmp_path / "missing-sa.json"
    argv = [str(missing) if token == "MISSING" else token for token in case.argv]
    assert_parse_error(argv, case.reason)


def test_parser_accepts_existing_sa_path_on_list(tmp_path):
    sa_file = tmp_path / "service_account.json"
    sa_file.write_text('{"type":"service_account"}', encoding="utf-8")
    args = parse_ok(["list", "--sa", str(sa_file)])
    assert args.sa == str(sa_file)


def test_parser_rejects_removed_short_sa_option():
    assert_parse_error(["list", "-s", "service_account.json"], "short -s alias is removed")


# global verbose count
@pytest.mark.parametrize(
    ("argv", "expected_verbose"),
    [
        (["list"], 0),
        (["list", "-v"], 1),
        (["list", "-vv"], 2),
    ],
    ids=["verbose-default", "verbose-single", "verbose-double"],
)
def test_parser_parses_global_verbose_count(argv, expected_verbose):
    args = parse_ok(argv)
    assert args.verbose == expected_verbose


# list command
LIST_TYPED_INVALID_CASES: list[ParserCase] = [
    ParserCase("list-before-invalid-date", ["list", "--before", "2025/01/01"], "--before uses valid_date"),
    ParserCase("list-after-invalid-date", ["list", "--after", "2025/01/01"], "--after uses valid_date"),
    ParserCase("list-older-non-int", ["list", "--older", "abc"], "--older uses int"),
    ParserCase("list-newer-non-int", ["list", "--newer", "abc"], "--newer uses int"),
    ParserCase("list-limit-non-int", ["list", "--limit", "abc"], "--limit uses int"),
    ParserCase("list-csv-wrong-ext", ["list", "--csv", "report.txt"], "--csv uses .csv validator"),
    ParserCase("list-xlsx-wrong-ext", ["list", "--xlsx", "report.csv"], "--xlsx uses .xlsx validator"),
]

LIST_MUTEX_CASES: list[ParserCase] = [
    ParserCase(
        "list-older-before-mutex",
        ["list", "--older", "1", "--before", "2025-01-01"],
        "--older and --before are mutually exclusive",
    ),
    ParserCase(
        "list-newer-after-mutex",
        ["list", "--newer", "1", "--after", "2025-01-01"],
        "--newer and --after are mutually exclusive",
    ),
    ParserCase(
        "list-name-contains-mutex",
        ["list", "--name", "x", "--contains", "y"],
        "--name and --contains are mutually exclusive",
    ),
]


@pytest.mark.parametrize("case", LIST_TYPED_INVALID_CASES + LIST_MUTEX_CASES, ids=lambda c: c.name)
def test_parser_rejects_invalid_list_cases(case):
    assert_parse_error(case.argv, case.reason)


def test_parser_accepts_valid_list_typed_values():
    args = parse_ok(
        [
            "list",
            "--before",
            "2025-01-02",
            "--after",
            "2025-01-01",
            "--limit",
            "10",
            "--csv",
            "report.csv",
        ]
    )
    assert args.before.year == 2025
    assert args.after.year == 2025
    assert args.limit == 10
    assert args.csv == "report.csv"


# delete command
DELETE_TYPED_INVALID_CASES: list[ParserCase] = [
    ParserCase("delete-before-invalid-date", ["delete", "--before", "bad-date"], "--before uses valid_date"),
    ParserCase("delete-after-invalid-date", ["delete", "--after", "bad-date"], "--after uses valid_date"),
    ParserCase("delete-older-non-int", ["delete", "--older", "abc"], "--older uses int"),
    ParserCase("delete-newer-non-int", ["delete", "--newer", "abc"], "--newer uses int"),
    ParserCase("delete-csv-wrong-ext", ["delete", "--csv", "report.txt"], "--csv uses .csv validator"),
]

DELETE_MUTEX_CASES: list[ParserCase] = [
    ParserCase(
        "delete-older-before-mutex",
        ["delete", "--older", "1", "--before", "2025-01-01"],
        "--older and --before are mutually exclusive",
    ),
    ParserCase(
        "delete-newer-after-mutex",
        ["delete", "--newer", "1", "--after", "2025-01-01"],
        "--newer and --after are mutually exclusive",
    ),
    ParserCase(
        "delete-name-contains-mutex",
        ["delete", "--name", "x", "--contains", "y"],
        "--name and --contains are mutually exclusive",
    ),
]


@pytest.mark.parametrize("case", DELETE_TYPED_INVALID_CASES + DELETE_MUTEX_CASES, ids=lambda c: c.name)
def test_parser_rejects_invalid_delete_cases(case):
    assert_parse_error(case.argv, case.reason)


def test_parser_rejects_missing_ids_file_path_on_delete(tmp_path):
    missing = tmp_path / "missing.csv"
    assert_parse_error(["delete", "--ids-file", str(missing)], "--ids-file uses valid_path")


def test_parser_accepts_existing_ids_file_path_on_delete(tmp_path):
    ids_file = tmp_path / "ids.csv"
    ids_file.write_text("id\nx\n", encoding="utf-8")
    args = parse_ok(["delete", "--ids-file", str(ids_file)])
    assert args.ids_file == str(ids_file)


def test_parser_accepts_delete_csv_without_path_as_auto():
    args = parse_ok(["delete", "--csv"])
    assert args.csv == "AUTO"


# clear-folder command
CLEAR_FOLDER_TYPED_INVALID_CASES: list[ParserCase] = [
    ParserCase(
        "clear-folder-before-invalid-date",
        ["clear-folder", "folder-1", "--before", "bad-date"],
        "--before uses valid_date",
    ),
    ParserCase(
        "clear-folder-after-invalid-date",
        ["clear-folder", "folder-1", "--after", "bad-date"],
        "--after uses valid_date",
    ),
    ParserCase(
        "clear-folder-older-non-int",
        ["clear-folder", "folder-1", "--older", "abc"],
        "--older uses int",
    ),
    ParserCase(
        "clear-folder-newer-non-int",
        ["clear-folder", "folder-1", "--newer", "abc"],
        "--newer uses int",
    ),
    ParserCase(
        "clear-folder-csv-wrong-ext",
        ["clear-folder", "folder-1", "--csv", "report.txt"],
        "--csv uses .csv validator",
    ),
]

CLEAR_FOLDER_MUTEX_CASES: list[ParserCase] = [
    ParserCase(
        "clear-folder-older-before-mutex",
        ["clear-folder", "folder-1", "--older", "1", "--before", "2025-01-01"],
        "--older and --before are mutually exclusive",
    ),
    ParserCase(
        "clear-folder-newer-after-mutex",
        ["clear-folder", "folder-1", "--newer", "1", "--after", "2025-01-01"],
        "--newer and --after are mutually exclusive",
    ),
    ParserCase(
        "clear-folder-name-contains-mutex",
        ["clear-folder", "folder-1", "--name", "x", "--contains", "y"],
        "--name and --contains are mutually exclusive",
    ),
]


@pytest.mark.parametrize(
    "case",
    CLEAR_FOLDER_TYPED_INVALID_CASES + CLEAR_FOLDER_MUTEX_CASES,
    ids=lambda c: c.name,
)
def test_parser_rejects_invalid_clear_folder_cases(case):
    assert_parse_error(case.argv, case.reason)


def test_parser_accepts_clear_folder_csv_without_path_as_auto():
    args = parse_ok(["clear-folder", "folder-1", "--csv"])
    assert args.csv == "AUTO"
