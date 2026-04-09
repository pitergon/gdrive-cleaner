# tests/helpers/cli_args_builders.py
from argparse import Namespace

BASE_ARGS = {
    "sa": None,
    "verbose": None,
    "dry_run": False,
    "force": False,
}
TIME_ARGS = {
    "older": None,
    "before": None,
    "newer": None,
    "after": None,
}
NAME_ARG = {
    "name": None,
    "contains": None,
}


def build_list_args(**overrides) -> Namespace:
    defaults = {
        "id": None,
        "folders_only": False,
        "resolve_parents": False,
        "limit": None,
        "csv": None,
        "xlsx": None,
    }
    args = {**BASE_ARGS, **TIME_ARGS, **NAME_ARG, **defaults, **overrides}
    return Namespace(**args)


def build_delete_args(**overrides) -> Namespace:
    defaults = {
        "id": None,
        "ids_file": None,
        "csv": None,
    }
    args = {**BASE_ARGS, **TIME_ARGS, **NAME_ARG, **defaults, **overrides}
    return Namespace(**args)


def build_clear_folder_args(**overrides) -> Namespace:
    defaults = {
        "folder_id": None,
        "csv": None,
    }
    args = {**BASE_ARGS, **TIME_ARGS, **NAME_ARG, **defaults, **overrides}
    return Namespace(**args)


def build_fetch_args(**overrides) -> Namespace:
    defaults = {
        "id": None,
        "path": None,
        "recursive": False,
        "export": False,
    }
    args = {**BASE_ARGS, **defaults, **overrides}
    return Namespace(**args)


def build_copy_args(**overrides) -> Namespace:
    defaults = {
        "id": None,
        "name": None,
        "target_id": None,
    }
    args = {**BASE_ARGS, **defaults, **overrides}
    return Namespace(**args)


def build_quota_args(**overrides) -> Namespace:
    args = {**BASE_ARGS, **overrides}
    return Namespace(**args)
