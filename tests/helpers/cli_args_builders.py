# tests/helpers/cli_args_builders.py
from argparse import Namespace


def build_delete_args(**overrides) -> Namespace:
    args = {
        "id": None,
        "ids_file": None,
        "older": None,
        "before": None,
        "newer": None,
        "after": None,
        "dry_run": False,
        "force": False,
        "csv": None,  # Path, not bool
        "name": None,
        "contains": None,
    }
    args.update(overrides)
    return Namespace(**args)


def build_list_args(**overrides) -> Namespace:
    args = {
        "id": None,
        "older": None,
        "before": None,
        "newer": None,
        "after": None,
        "dry_run": False,
        "force": False,
        "name": None,
        "contains": None,
        "csv": None,  # Path, not bool
        "xlsx": None,  # Path, not bool
    }
    args.update(overrides)
    return Namespace(**args)


def build_fetch_args(**overrides) -> Namespace:
    args = {
        "id": None,
        "older": None,
        "before": None,
        "newer": None,
        "after": None,
        "dry_run": False,
        "force": False,
        "name": None,
        "contains": None,
        "csv": None,  # Path, not bool
        "xlsx": None,  # Path, not bool
    }
    args.update(overrides)
    return Namespace(**args)

def build_copy_args(**overrides) -> Namespace:
    ...
def build_clear_folder_args(**overrides) -> Namespace:
    ...
