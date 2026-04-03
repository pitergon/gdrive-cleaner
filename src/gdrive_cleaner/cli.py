import argparse
import io
import logging
import os
import pydoc
import sys
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)

from gdrive_cleaner.drive_core import DriveCore, FileFilter, FileItem, OperationResult
from gdrive_cleaner.operations import DriveOperations, convert_size

CONSOLE_LIMIT = 200
EXPORT_FOLDER = "export"
DOWNLOAD_FOLDER = "download"
REPORT_FOLDER = "reports"

console = Console()
error_console = Console(stderr=True)
logger = logging.getLogger(__name__)


class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Special formatter for better help message"""

    def __init__(self, prog):
        super().__init__(prog, max_help_position=50, width=120)


class UserInputError(Exception):
    pass


# --- Validators ---
def valid_ext(extension):
    def check_path(path):
        if path == "AUTO":
            return path
        if not path.lower().endswith(extension):
            raise argparse.ArgumentTypeError(f"File must have {extension} extension")
        return path

    return check_path


def valid_path(path):
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"File not found: {path}")
    return path


def valid_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Wrong date format: {date_str}. Expected YYYY-MM-DD."
        ) from e


# --- UI & Helpers ---


def setup_basic_logging(level=logging.INFO):
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        fmt="%(asctime)s|%(levelname)-8s| %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def get_date_filters(args: argparse.Namespace):
    older = getattr(args, "older", None)
    before = getattr(args, "before", None)
    newer = getattr(args, "newer", None)
    after = getattr(args, "after", None)

    date_before = None
    date_after = None

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    if older is not None:
        date_before = today - timedelta(days=older)
    elif before:
        date_before = before

    if newer is not None:
        date_after = today - timedelta(days=newer - 1)
    elif after:
        date_after = after + timedelta(days=1)

    if date_before and date_after and date_before <= date_after:
        raise UserInputError(
            "Invalid date range: 'before' date must be greater than 'after' date at least 2 days apart."
        )

    return date_before, date_after


def get_name_filters(args: argparse.Namespace):
    name_exact = getattr(args, "name", None)
    name_contains = getattr(args, "contains", None)
    return name_exact, name_contains


def read_ids_file(file_path: Path) -> list[str]:
    """
    Reads a .csv or .xlsx file and extracts unique IDs from the 'id' column.
    """

    try:
        if file_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path, engine="calamine")
        else:
            df = pd.read_csv(file_path)
    except Exception as e:
        raise UserInputError(f"Error reading file {file_path}: {e}") from e

    if df.empty:
        return []

    if "id" in df.columns:
        return df["id"].dropna().astype(str).unique().tolist()

    return []


def confirm_deleting(args: argparse.Namespace, items_count: int, source_msg: str, has_folders=False) -> bool:
    if args.dry_run:
        return False

    content_info = f"{items_count} items"
    if has_folders:
        content_info += " (INCLUDING SUBFOLDERS AND THEIR CONTENT)"

    if args.force:
        error_console.print(f"WARNING: Force deleting {content_info} in {source_msg}...")
        return True

    # --force not set, asking for confirmation
    error_console.print(f"\n[!] READY TO DELETE: {content_info}")
    error_console.print(f"[!] LOCATION: {source_msg}")

    if has_folders:
        error_console.print(
            "DANGER: Deleting a folder will recursively remove ALL files and subfolders inside it!"
        )

    confirm = error_console.input("\nAre you sure you want to proceed? (yes/no): ")
    return confirm.lower() == "yes"


def confirm_copying(args: argparse.Namespace, source_msg: str, target_msg: str) -> bool:
    if args.dry_run:
        return False

    if args.force:
        error_console.print(f"WARNING: Force copying {source_msg} to {target_msg}...")
        return True

    # --force not set, asking for confirmation
    error_console.print(f"\n[!] READY TO COPY: {source_msg} to {target_msg}")

    confirm = error_console.input(f"\nCopy {source_msg} to {target_msg}? (yes/no): ")
    return confirm.lower() == "yes"


def confirm_saving_report() -> bool:
    confirm = error_console.input("\nSave detailed CSV report? (yes/no): ")
    return confirm.lower() == "yes"


def print_summary(result: OperationResult):
    success = result.success if result.success is not None else -1
    failed = result.failed if result.failed is not None else -1
    critical = int(result.success is None)

    if sys.stdout.isatty():
        error_console.print(
            f"[bold]Summary:[/bold]\n"
            f"total={result.total}\n"
            f"success=[green]{success}[/green]\n"
            f"failed=[red]{failed}[/red]\n"
            f"critical={critical}"
        )
    else:
        console.print(f"total={result.total} success={success} failed={failed} critical={critical}")

    if critical:
        error_console.print("[yellow]WARNING: critical termination[/yellow]")

    errors = [e for e in result.entries if e.error]
    if errors:
        error_console.print("Errors encountered during deletion:")
        for e in errors:
            error_console.print(f"error id={e.id} msg={e.error}")


def smart_print(items: list[FileItem], console_limit=CONSOLE_LIMIT):
    def get_safe_str(text, width):
        encoding = sys.stdout.encoding or "utf-8"
        safe_text = text.encode(encoding, errors="replace").decode(encoding)
        if len(safe_text) > width:
            return safe_text[: width - 3] + "..."
        return f"{safe_text:<{width}}"

    ID_W, NAME_W, SIZE_W, DATE_W = 33, 40, 10, 12

    # 1. OUTPUT TO FILE (no truncation, full data)
    if not sys.stdout.isatty():
        for item in items:
            line = f"{item.id} | {item.name} | {item.size} | {item.created_at.strftime('%Y-%m-%d')} | {item.mime_type}\n"
            sys.stdout.buffer.write(line.encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()
        return

    # 2. OUTPUT TO CONSOLE (truncate for better readability)
    total_count = len(items)
    display_items = items[:console_limit]
    lines = []

    for item in display_items:
        is_dir = "d" if item.mime_type == "application/vnd.google-apps.folder" else "f"

        safe_name = get_safe_str(item.name, NAME_W)

        line = (
            f"{is_dir} | "
            f"{item.id:<{ID_W}} | "
            f"{safe_name} | "
            f"{convert_size(item.size):>{SIZE_W}} | "
            f"{item.created_at.strftime('%Y-%m-%d'):<{DATE_W}}"
        )
        lines.append(line)

    # Summary to console
    if total_count > console_limit:
        lines.append(f"\n[!] Showed {console_limit} from {total_count} items.")
        lines.append("[i] For the whole list use export (--csv/--xlsx) or pipe ( > file.txt)")
    else:
        lines.append(f"\n--- Total items: {total_count} ---")

    pydoc.pager("\n".join(lines))


def save_operation_report(result, report_folder=Path(REPORT_FOLDER)):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"deleting_report_{timestamp}.csv".replace(" ", "_")
    filepath = report_folder / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)

    disclaimer = (
        "Note: Folders are deleted with all their content. Sub-items are not listed individually.\n"
    )

    data = [
        {
            "ID": e.id,
            "Name": e.name,
            "Type": e.type,
            "Size": e.size,
            "Status": e.status,
            "Error": e.error or "",
        }
        for e in result.entries
    ]

    df = pd.DataFrame(data)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(disclaimer)
        df.to_csv(f, index=False, lineterminator="\n")

    error_console.print(
        f"\n[green]\u2714[/green] Detailed report saved to: [bold]{filename}[/bold]"
    )


def build_parser():
    epilog = """
    Examples:
      # List files older than 30 days in a specific folder
      python cli.py list <FOLDER_ID> --older 30

      # Export list of all folders to CSV
      python cli.py list --folders-only --csv report.csv

      # Delete a single file by ID without confirmation
      python cli.py delete <FILE_ID> --force

      # Delete files listed in a CSV file
      python cli.py delete -i ids_to_delete.csv

      # Clear a folder of files created before 2025
      python cli.py clear-folder <FOLDER_ID> --before 2025-01-01

      # Download a folder recursively to a specific path
      python cli.py fetch <FOLDER_ID> -r -p ./backups
        """
    formatter = CustomHelpFormatter

    base_parent = argparse.ArgumentParser(add_help=False, formatter_class=formatter)

    base_parent.add_argument(
        "-s",
        "--sa",
        type=valid_path,
        metavar="<JSON_FILE>",
        default=None,
        help="Path to service account json (default: ENV or service_account.json)",
    )
    base_parent.add_argument(
        "-v", "--verbose", action="count", default=0, help="Verbose level: -v (INFO), -vv (DEBUG)"
    )
    base_parent.add_argument(
        "-d", "--dry-run", action="store_true", help="Show what would be deleted"
    )
    base_parent.add_argument(
        "--force", action="store_true", help="Don't ask confirmation"
    )  # hidden alias for -vv

    main_parser = argparse.ArgumentParser(
        prog="gdrive-cleaner",
        description="Google Drive maintenance tool for listing, deleting and downloading files.",
        epilog=epilog,
        formatter_class=formatter,
    )
    subparsers = main_parser.add_subparsers(dest="command", required=True)

    # time filter
    time_filters = argparse.ArgumentParser(add_help=False, formatter_class=formatter)
    upper_date_group = time_filters.add_mutually_exclusive_group()
    upper_date_group.add_argument(
        "-o", "--older", type=int, metavar="<DAYS>", help="Older than X days (created BEFORE now-X)"
    )
    upper_date_group.add_argument(
        "-b",
        "--before",
        type=valid_date,
        metavar="<YYYY-MM-DD>",
        help="Created before specific date",
    )
    lower_date_group = time_filters.add_mutually_exclusive_group()
    lower_date_group.add_argument(
        "-n", "--newer", type=int, metavar="<DAYS>", help="Newer than X days (created AFTER now-X)"
    )
    lower_date_group.add_argument(
        "-a", "--after", type=valid_date, metavar="<YYYY-MM-DD>", help="Created after date"
    )
    name_filters = argparse.ArgumentParser(add_help=False, formatter_class=formatter)
    name_group = name_filters.add_mutually_exclusive_group()
    name_group.add_argument(
        "--name",
        metavar="<NAME>",
        help="Filter by exact file/folder name",
    )
    name_group.add_argument(
        "--contains",
        metavar="<TEXT>",
        help="Filter by file/folder name containing text",
    )

    # list
    list_cmd = subparsers.add_parser(
        "list",
        parents=[base_parent, time_filters, name_filters],
        help="List files/folders",
        formatter_class=formatter,
    )
    list_cmd.add_argument(
        "id", nargs="?", metavar="<PARENT_ID>", help="List files in folder with given ID"
    )
    export_group = list_cmd.add_argument_group("Export options")
    export_group.add_argument(
        "--csv",
        type=valid_ext(".csv"),
        nargs="?",
        const="AUTO",
        default=None,
        metavar="<PATH>",
        help="Export to CSV file",
    )
    export_group.add_argument(
        "--xlsx",
        type=valid_ext(".xlsx"),
        nargs="?",
        const="AUTO",
        default=None,
        metavar="<PATH>",
        help="Export to XLSX file",
    )
    view_group = list_cmd.add_argument_group("View options")
    view_group.add_argument("-f", "--folders-only", action="store_true", help="List only folders")
    view_group.add_argument(
        "-r", "--resolve-parents", action="store_true", help="Resolve parent names"
    )
    view_group.add_argument(
        "-l", "--limit", type=int, metavar="<COUNT>", help="Limit files listed/exported"
    )

    # delete
    delete_cmd = subparsers.add_parser(
        "delete",
        parents=[base_parent, time_filters, name_filters],
        help="Delete files",
        formatter_class=formatter,
    )
    delete_group = delete_cmd.add_mutually_exclusive_group()
    delete_group.add_argument("id", nargs="?", metavar="<ID>", help="Single ID to delete")
    delete_group.add_argument(
        "-i",
        "--ids-file",
        metavar="<PATH>",
        type=valid_path,
        help="Path to file (.csv/.xlsx) with IDs",
    )
    delete_cmd.add_argument(
        "--csv", action="store_true", help="Always save detailed CSV report without asking"
    )

    # clear-folder
    clear_cmd = subparsers.add_parser(
        "clear-folder",
        parents=[base_parent, time_filters, name_filters],
        help="Clear files in specific folder. Subfolders will be deleted with their content!",
        formatter_class=formatter,
    )
    clear_cmd.add_argument(
        "folder_id", metavar="<FOLDER_ID>", help="Google Drive Folder ID to clear"
    )
    clear_cmd.add_argument(
        "--csv", action="store_true", help="Always save detailed CSV report without asking"
    )

    # fetch
    fetch_cmd = subparsers.add_parser(
        "fetch", parents=[base_parent], help="Download file or folder", formatter_class=formatter
    )
    fetch_cmd.add_argument("id", metavar="<ID>", help="Google Drive ID to download")
    fetch_cmd.add_argument(
        "-p",
        "--path",
        metavar="<PATH>",
        default=DOWNLOAD_FOLDER,
        help="Output path (default: current dir)",
    )
    fetch_cmd.add_argument(
        "-r", "--recursive", action="store_true", help="Download folder recursively"
    )
    fetch_cmd.add_argument(
        "-e",
        "--export",
        action="store_true",
        help="Enable export of Google Docs/Sheets/Slides to MS Office formats (docx/xlsx/pptx)",
    )

    # copy
    copy_cmd = subparsers.add_parser(
        "copy",
        parents=[base_parent],
        help="Copy single file to another location (folder). Support only files, not folders",
        formatter_class=formatter,
    )
    copy_cmd.add_argument("id", metavar="<ID>", help="Google Drive ID to copy")
    copy_cmd.add_argument("-n", "--name", metavar="<NEW_NAME>", help="New file name")
    copy_cmd.add_argument("-t", "--target_id", metavar="<FOLDER_ID>", help="Target folder ID to copy into")

    # quota
    subparsers.add_parser(
        "quota", parents=[base_parent], help="Show Drive storage quota", formatter_class=formatter
    )
    return main_parser


# --- Command Handlers ---


def handle_list(args: argparse.Namespace, ops: DriveOperations):
    date_before, date_after = get_date_filters(args)
    name_exact, name_contains = get_name_filters(args)

    file_filter = FileFilter(
        folder_id=args.id if args.id else None,
        created_before=date_before,
        created_after=date_after,
        name_exact=name_exact,
        name_contains=name_contains,
        mime_type="application/vnd.google-apps.folder" if args.folders_only else None,
    )

    limit = args.limit if args.limit and args.limit > 0 else None
    items = None
    exported_paths: list[Path] = []

    with error_console.status("[bold yellow]Processing file list ...") as status:

        def on_progress(message: str, progressed: int | None = None):
            msg = f"[bold yellow]{message}[/bold yellow]"
            if progressed is not None:
                msg += f"[bold yellow] Progress: {progressed}[/bold yellow]"
            status.update(msg)

        if args.csv or args.xlsx:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # with error_console.status("[bold yellow]Export file list ...") as status:
            if args.csv:
                fname = f"gdrive_analysis_{timestamp}.csv" if args.csv == "AUTO" else args.csv
                output_csv = (Path(EXPORT_FOLDER) / fname).resolve()
                ops.export_to_csv(
                    output_path=output_csv,
                    file_filter=file_filter,
                    limit=limit,
                    resolve_ext_parents=args.resolve_parents,
                    on_progress=on_progress,
                )
                exported_paths.append(output_csv)
            if args.xlsx:
                fname = f"gdrive_analysis_{timestamp}.xlsx" if args.xlsx == "AUTO" else args.xlsx
                output_xlsx = (Path(EXPORT_FOLDER) / fname).resolve()
                ops.export_to_xlsx(
                    output_path=output_xlsx,
                    file_filter=file_filter,
                    limit=limit,
                    resolve_ext_parents=args.resolve_parents,
                    on_progress=on_progress,
                )
                exported_paths.append(output_xlsx)
        else:
            # with error_console.status("[bold yellow]Getting file list from Gdrive API ...") as status:
            items = ops.list_files(file_filter=file_filter, limit=limit, on_progress=on_progress)

    if items is not None:
        smart_print(items)
        error_console.print(f"Command 'list' completed. Items: {len(items)}")
    elif exported_paths:
        error_console.print("Command 'list' completed. Exported:")
        for path in exported_paths:
            error_console.print(f"  - {path}")


def handle_delete(args: argparse.Namespace, ops: DriveOperations):
    is_tty = sys.stdout.isatty()
    date_before, date_after = get_date_filters(args)
    name_exact, name_contains = get_name_filters(args)

    has_ids_filter = bool(args.id or args.ids_file)
    has_date_filter = bool(date_before or date_after)
    has_name_filter = bool(name_exact or name_contains)
    requested_ids_count: int | None = None
    resolved_ids_count: int | None = None

    if has_ids_filter and (has_date_filter or has_name_filter):
        raise UserInputError("Use either ID/--ids-file or API filters (--older/--before/--newer/--after/--name/--contains), not both.")

    items = []
    with error_console.status("[bold yellow]Checking...") as status:

        def on_list_progress(message: str, progressed: int | None = None):
            msg = f"[bold yellow]{message}[/bold yellow]"
            if progressed is not None:
                msg += f" [bold yellow]Progress: {progressed}[/bold yellow]"
            status.update(msg)

        if has_ids_filter:
            if args.id:
                ids = [args.id]
            elif args.ids_file:
                status.update("Reading file...")
                ids = read_ids_file(Path(args.ids_file).resolve())
                requested_ids_count = len(ids)

            status.update(f"Checking metadata for {len(ids)} items...")
            items_map = ops.get_items_batch(ids)
            items = [item for item in items_map.values() if item is not None]
            resolved_ids_count = len(items)

        elif has_date_filter or has_name_filter:
            file_filter = FileFilter(
                created_before=date_before,
                created_after=date_after,
                name_exact=name_exact,
                name_contains=name_contains,
            )
            status.update("[bold yellow]Listing items...[/bold yellow]")
            items = ops.list_files(file_filter=file_filter, on_progress=on_list_progress)

        else:
            raise UserInputError(
                "Please specify ID, --ids-file or API filters (--older/--before/--newer/--after/--name/--contains)"
            )

    if not items:
        error_console.print("No items found to delete.")
        if requested_ids_count is not None and resolved_ids_count is not None:
            error_console.print(
                f"IDs in file: {requested_ids_count}, found in Drive: {resolved_ids_count}, missing: {requested_ids_count - resolved_ids_count}"
            )
        error_console.print("Command 'delete' completed: nothing to delete.")
        return

    if args.dry_run:
        limit_msg = f" (first {CONSOLE_LIMIT})" if len(items) > CONSOLE_LIMIT else ""
        error_console.print(
            f"Dry run enabled. The following files and folders with content would be deleted{limit_msg}"
        )
        smart_print([i for i in items[:CONSOLE_LIMIT] if i])
        return

    if requested_ids_count is not None and resolved_ids_count is not None:
        error_console.print(
            f"IDs in file: {requested_ids_count}, found in Drive: {resolved_ids_count}, "
            f"missing: {requested_ids_count - resolved_ids_count}"
        )

    has_folders = any(item.mime_type == "application/vnd.google-apps.folder" for item in items)
    if not confirm_deleting(args, len(items), "selected criteria", has_folders=has_folders):
        error_console.print("Operation cancelled")
        error_console.print("Command 'delete' cancelled.")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[red]Deleting...[/red]"),
        BarColumn(),
        MofNCompleteColumn(),
        console=error_console,
        refresh_per_second=5,
        disable=not is_tty,
        transient=True,
    ) as progress:
        task = progress.add_task("Cleaning Drive", total=len(items))
        result = ops.delete_items(
            items, on_progress=lambda count: progress.advance(task, advance=count)
        )

    # Summary
    if requested_ids_count is not None and resolved_ids_count is not None:
        error_console.print(
            f"IDs in file: {requested_ids_count}\n"
            f"found in Drive: {resolved_ids_count}\n"
            f"missing: {requested_ids_count - resolved_ids_count}\n"
        )
    print_summary(result)

    # 6. Asking for save report
    should_save = False
    if args.csv:
        should_save = True
    elif is_tty and confirm_saving_report():
        should_save = True

    if should_save:
        try:
            save_operation_report(result)
            error_console.print("[green]\u2714[/green] CSV report saved")
        except Exception as e:
            error_console.print(f"[yellow]\u26A0 Warning: Could not save CSV report:[/yellow] {e}")



    error_console.print("Command 'delete' completed.")


def handle_clear_folder(args: argparse.Namespace, ops: DriveOperations):
    is_tty = sys.stdout.isatty()
    date_before, date_after = get_date_filters(args)
    name_exact, name_contains = get_name_filters(args)

    with error_console.status("[bold yellow]Checking...") as status:

        def on_list_progress(message: str, progressed: int | None = None):
            msg = f"[bold yellow]{message}[/bold yellow]"
            if progressed is not None:
                msg += f" [bold yellow]Progress: {progressed}[/bold yellow]"
            status.update(msg)

        # 1. Checking
        folder = ops.get_item(file_id=args.folder_id)
        if not folder or folder.mime_type != "application/vnd.google-apps.folder":
            raise UserInputError(f"Folder {args.folder_id} not found.")
        # 2. Listing
        status.update(f"[bold yellow]Listing files in '{folder.name}'...[/bold yellow]")
        file_filter = FileFilter(
            folder_id=args.folder_id,
            created_before=date_before,
            created_after=date_after,
            name_exact=name_exact,
            name_contains=name_contains,
        )
        items = ops.list_files(file_filter=file_filter, on_progress=on_list_progress)

    if not items:
        error_console.print("No files found to delete.")
        error_console.print("Command 'clear-folder' completed: nothing to delete.")
        return

    has_folders = any(item.mime_type == "application/vnd.google-apps.folder" for item in items)

    # 3. Dry Run
    if args.dry_run:
        limit_msg = f" (first {CONSOLE_LIMIT})" if len(items) > CONSOLE_LIMIT else ""
        error_console.print(
            f"Dry run enabled. The following files and folders with content would be deleted{limit_msg}"
        )
        smart_print([i for i in items[:CONSOLE_LIMIT] if i])
        return

    # 4. Confirmation and deleting with progress
    if not confirm_deleting(args, len(items), f"folder '{folder.name}'", has_folders=has_folders):
        error_console.print("Operation cancelled")
        error_console.print("Command 'clear-folder' cancelled.")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[red]Deleting...[/red]"),
        BarColumn(),
        MofNCompleteColumn(),
        console=error_console,
        refresh_per_second=5,
        disable=not is_tty,
        transient=True,
    ) as progress:
        task = progress.add_task("Cleaning Drive", total=len(items))
        result = ops.delete_items(
            items, on_progress=lambda count: progress.advance(task, advance=count)
        )

    # 5 Summary
    print_summary(result)

    # 6. Asking for save report
    should_save = False
    if args.csv:
        should_save = True
    elif is_tty and confirm_saving_report():
        should_save = True

    if should_save:
        try:
            save_operation_report(result)
            error_console.print(
                f"[green]\u2714[/green] CSV report saved for folder: {folder.name}"
            )
        except Exception as e:
            error_console.print(f"[yellow]\u26A0 Warning: Could not save CSV report:[/yellow] {e}")

    error_console.print("Command 'clear-folder' completed.")


def handle_fetch(args, ops: DriveOperations):
    is_tty = sys.stdout.isatty()
    with error_console.status(f"[bold yellow]Fetching metadata for {args.id}...") as status:
        item = ops.get_item(file_id=args.id)

    if not item:
        raise UserInputError(f"Item with ID '{args.id}' not found.")

    with Progress(
        SpinnerColumn(),
        TextColumn("[red]Fetching...[/red]"),
        BarColumn(),
        MofNCompleteColumn(),
        console=error_console,
        refresh_per_second=5,
        disable=not is_tty,
        transient=True,
    ) as progress:
        tasks = {}

        def on_progress(file_id: str, name: str, completed: int, total: int, status: str):
            """Callback for fetch_item to update progress. Status can be 'progress', 'finished' or 'error'."""
            if not is_tty:
                if status == "finished":
                    error_console.print(f"DONE: {name}")
                elif status == "error":
                    error_console.print(f"FAIL: {name}")
                return

            if status == "progress":
                if file_id not in tasks:
                    tasks[file_id] = progress.add_task(
                        description=f"[cyan]Fetching {name}", total=total
                    )
                progress.update(tasks[file_id], completed=completed)

            elif status == "finished":
                if file_id in tasks:
                    progress.remove_task(tasks[file_id])
                progress.console.print(f"[green]\u2714[/green] Success | {name}")

            elif status == "error":
                if file_id in tasks:
                    progress.remove_task(tasks[file_id])
                progress.console.print(f"[red]\u2718[/red] Failed  | {name}")

        ops.fetch_item(
            item_or_id=item,
            output_path=Path(args.path),
            recursive=args.recursive,
            force=args.force,
            export=args.export,
            dry_run=args.dry_run,
            on_progress=on_progress,
        )

    error_console.print("Command 'fetch' completed.")


def handle_quota(args: argparse.Namespace, ops: DriveOperations):
    is_tty = sys.stdout.isatty()
    with error_console.status("[bold yellow]Fetching storage quota...[/bold yellow]"):
        quota = ops.get_quota_info()

    if is_tty:
        console.print("[bold]Storage Quota[/bold]")
        console.print(f"Used: [yellow]{quota['usage']}[/yellow]")
        console.print(f"Free: [green]{quota['free']}[/green]")
        console.print(f"Total: [cyan]{quota['limit']}[/cyan]")
    else:
        console.print(
            f"Storage Quota\nUsed: {quota['usage']}\nFree: {quota['free']}\nTotal: {quota['limit']}\n"
        )

    error_console.print("Command 'quota' completed.")


def handle_copy(args: argparse.Namespace, ops: DriveOperations):
    item = ops.get_item(file_id=args.id)
    with error_console.status("[bold yellow]Analysing...[/bold yellow]"):
        if not item:
            raise UserInputError(f"Item with ID '{args.id}' not found.")
        if item.mime_type == "application/vnd.google-apps.folder":
            raise UserInputError("Copying folders is not supported.")

        target = None
        if args.target_id:
            target = ops.get_item(file_id=args.target_id)
            if not target:
                raise UserInputError(
                    f"Target folder with ID '{args.target_id}' not found."
                )
            if target.mime_type != "application/vnd.google-apps.folder":
                raise UserInputError(f"Target ID '{args.target_id}' is not a folder.")

    target_id = target.id if target else None

    source_msg = f"{item.name} (ID: {item.id})"
    new_name = args.name if args.name else item.name
    target_msg = f"'{new_name}' in folder {target.name} (ID: {target.id})" if target else f"'{new_name}'"

    if args.dry_run:
        error_console.print(f"Dry run enabled. Would copy {source_msg} to {target_msg}.")
        error_console.print("Command 'copy' completed: nothing copied.")
        return

    if not confirm_copying(args, source_msg, target_msg):
        error_console.print("Copy operation cancelled.")
        error_console.print("Command 'copy' cancelled.")
        return

    with error_console.status(f"[bold yellow]Copying {item.name}...[/bold yellow]") as status:
        new_item = ops.copy_file(
            file_id=item.id,
            new_name=args.name,
            target_id=target_id,
        )

    if new_item is None:
        raise UserInputError("Copy finished but copied item metadata is unavailable.")

    error_console.print(f"[green]\u2714[/green] Copied '{item.name}' to {target_msg} with new ID: {new_item.id}")
    error_console.print("Command 'copy' completed.")

# --- Main ---
def main():
    if (sys.stdout.encoding or "").lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    parser = build_parser()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()

    # Logging setup
    log_level = {0: logging.WARNING, 1: logging.INFO}.get(args.verbose, logging.DEBUG)
    setup_basic_logging(log_level)

    # Core initialization
    sa_path = Path(
        args.sa or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "service_account.json"
    ).resolve()
    if not sa_path.exists():
        error_console.print(f"Error: Service account file not found: {sa_path}")
        logger.debug(f"Service account file not found: {sa_path}")
        sys.exit(1)

    drive = DriveCore(str(sa_path))
    ops = DriveOperations(drive)

    handlers: dict[str, Callable[[argparse.Namespace, DriveOperations], None]] = {
        "list": handle_list,
        "delete": handle_delete,
        "clear-folder": handle_clear_folder,
        "fetch": handle_fetch,
        "copy": handle_copy,
        "quota": handle_quota,
    }

    handler = handlers.get(args.command)
    if handler:
        try:
            handler(args, ops)

        except UserInputError as e:
            error_console.print(f"Error during command '{args.command}': {e}")
            logger.debug("UserInputError details", exc_info=True)
            sys.exit(1)
        except KeyboardInterrupt:
            error_console.print(
                f"\n[yellow]Operation '{args.command}' interrupted by user. Exiting...[/yellow]"
            )
            sys.exit(1)
        except Exception:
            error_console.print(f"Command '{args.command}' failed. Use -vv for details.")
            logger.exception("Unhandled exception details")
            sys.exit(1)


if __name__ == "__main__":
    main()
