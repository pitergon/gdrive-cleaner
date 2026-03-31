import logging
import sys
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from drive_core import DriveCore, FileFilter, FileItem, OperationResult

logger = logging.getLogger(__name__)


def convert_size(size_bytes: int | float):
    try:
        size_bytes = float(size_bytes)
    except (ValueError, TypeError):
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024


class DriveOperations:
    def __init__(self, drive: DriveCore, *, custom_logger: logging.Logger | None = None):
        self.logger = custom_logger or logger
        self.drive = drive
        self.items_cache: list[
            FileItem
        ] = []  # for simultaneously generating csv and xlsx reports without multiple API calls

    def list_files(
        self,
        file_filter: FileFilter | None = None,
        limit: int | None = None,
        on_progress: Callable | None = None,
    ) -> list[FileItem]:
        if self.items_cache:
            self.logger.debug(f"Using {len(self.items_cache)} cached items for listing files.")
            return self.items_cache

        def cb_wrapper(count):
            if on_progress:
                on_progress("Listing files...", count)

        items = self.drive.list_files(file_filter=file_filter, limit=limit, on_progress=cb_wrapper)
        logger.info(
            f"Listed {len(items)} items from Google Drive with filter: {file_filter} and limit: {limit}"
        )
        self.items_cache = items
        return items

    def _prepare_rows(self, items: list[FileItem], resolve_ext_parents: bool = False) -> list[dict]:
        self.logger.debug(f"Preparing rows for {len(items)} items...")
        rows = []
        items_by_id = {i.id: i for i in items}
        unnamed_parents_id = set()

        for item in items:
            parent_id = item.parents[0] if item.parents else None
            if parent_id == "root":
                parent_name = "ROOT"
            elif parent_id in items_by_id:
                parent_name = items_by_id[parent_id].name
            else:
                parent_name = None
                unnamed_parents_id.add(parent_id)

            row = {
                "id": item.id,
                "name": item.name,
                "owner_email": item.owner,
                "size_bytes": item.size if item.size else 0,
                "size_readable": convert_size(item.size),
                "is_folder": item.mime_type == "application/vnd.google-apps.folder",
                "created_at": item.created_at,
                "modified_at": item.modified_at,
                "parent_name": parent_name,
                "parent_id": parent_id,
                "mime_type": item.mime_type,
            }

            if item.created_at:
                row["created_date"] = item.created_at.date()
                row["created_time"] = item.created_at.time()

            if item.modified_at:
                row["modified_date"] = item.modified_at.date()
                row["modified_time"] = item.modified_at.time()

            rows.append(row)

        if resolve_ext_parents and unnamed_parents_id:
            self.logger.debug(
                f"Resolving names for {len(unnamed_parents_id)} external parent IDs..."
            )
            external_folder_names = self.drive.get_folder_names(unnamed_parents_id)
            for row in rows:
                if row["parent_id"] in external_folder_names:
                    row["parent_name"] = external_folder_names[row["parent_id"]]

        return rows

    def _prepare_df(self, rows: list[dict]) -> pd.DataFrame:
        self.logger.debug(f"Preparing DataFrame from {len(rows)} rows...")
        df = pd.DataFrame(rows)
        if df.empty:
            self.logger.warning("No data found for the report.")
            return df

        # dt_cols = df.select_dtypes(include=["datetime64[ns, UTC]", "datetimetz"]).columns
        dt_cols = df.select_dtypes(include=["datetimetz", "datetime"]).columns
        for col in dt_cols:
            #df[col] = df[col].dt.tz_localize(None)
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_localize(None)


        for col in ["created_date", "modified_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date

        for col in ["created_time", "modified_time"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.slice(0, 8)

        df.sort_values(
            by=["parent_name", "parent_id", "size_bytes"],
            ascending=[True, True, False],
            inplace=True,
        )

        return df

    def export_to_csv(
        self,
        output_path: Path,
        file_filter: FileFilter | None = None,
        limit: int | None = None,
        resolve_ext_parents: bool = False,
        on_progress: Callable[[str, int | None], None] | None = None,
    ):
        self.logger.info(f"Export data to: {output_path.name}...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if on_progress:
            on_progress("Listing files...", None)
        items = self.list_files(file_filter=file_filter, limit=limit, on_progress=on_progress)
        if on_progress:
            on_progress("Processing...", None)
        rows = self._prepare_rows(items, resolve_ext_parents=resolve_ext_parents)
        df = self._prepare_df(rows)
        if df.empty:
            self.logger.warning("No data to export")
            return
        if on_progress:
            on_progress("Saving CSV...", None)
        df.to_csv(output_path, date_format="%Y-%m-%d %H:%M:%S", index=False)
        self.logger.info(
            f"Export CSV successfully finished: {output_path} with {len(items)} records."
        )

    def export_to_xlsx(
        self,
        output_path: Path,
        file_filter: FileFilter | None = None,
        limit: int | None = None,
        resolve_ext_parents: bool = True,
        on_progress: Callable[[str, int | None], None] | None = None,
    ):
        self.logger.info(f"Export data to: {output_path.name}...")
        self.logger.debug(
            f"Export settings: file_filter: {file_filter}, limit: {limit}, resolve_ext_parents: {resolve_ext_parents}"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if on_progress:
            on_progress("Listing files...", None)
        items = self.list_files(file_filter=file_filter, limit=limit, on_progress=on_progress)
        if on_progress:
            on_progress("Processing...", None)
        rows = self._prepare_rows(items, resolve_ext_parents=resolve_ext_parents)
        base_df = self._prepare_df(rows)
        if base_df.empty:
            self.logger.warning("No data to export")
            return

        if on_progress:
            on_progress("Saving XLSX...", None)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # ---- Sheet 1. All items---
            base_df.to_excel(writer, sheet_name="All Items", index=False)
            # --- Sheet 2. Files only ---
            files_df = base_df[~base_df["is_folder"]].copy()
            files_df.to_excel(writer, sheet_name="Files", index=False)
            self.logger.info(f"Processed files: {len(files_df)}")

            # --- Sheet 3. Folders with size ---
            df_sizes = self._calculate_sizes_iterative(base_df)
            folders_df = df_sizes[df_sizes["is_folder"]].copy()
            folders_df.sort_values(
                by=["parent_name", "parent_id", "size_bytes"],
                ascending=[True, True, False],
                inplace=True,
            )
            folders_df.to_excel(writer, sheet_name="Folders", index=False)
            self.logger.info(f"Processed folders: {len(folders_df)}")

            # ---- Sheet 4. Summary ---
            quota = self.drive.get_quota()

            total_size_bytes = base_df["size_bytes"].sum()
            summary_data = [
                {"Metric": "Total Report Items", "Value": len(base_df)},
                {"Metric": "Total Report Files", "Value": len(files_df)},
                {"Metric": "Total Report Folders", "Value": len(folders_df)},
                {"Metric": "Total Report Size (Bytes)", "Value": total_size_bytes},
                {"Metric": "Total Report Size (Readable)", "Value": convert_size(total_size_bytes)},
                {"Metric": "GDrive Quota", "Value": convert_size(quota["limit"])},
                {"Metric": "GDrive Usage", "Value": convert_size(quota["usage"])},
                {"Metric": "GDrive Free", "Value": convert_size(quota["free"])},
            ]
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        self.logger.info(
            f"Export XSLX successfully finished: {output_path} with {len(base_df)} records."
        )

    def _calculate_sizes_iterative(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Calculating folder sizes iteratively...")
        df = df.copy()
        # 1. Prepare dicts for quick lookups. Dicts with sizes and childrens
        sizes = dict(zip(df["id"], df["size_bytes"].mask(df["is_folder"], 0)))
        parent_map = df.dropna(subset=["parent_id"]).set_index("id")["parent_id"].to_dict()

        # Dict with counts of how many children each parent has (only for folders, files are not parents)
        child_counts = df["parent_id"].dropna().value_counts().to_dict()

        # 2. Leaves queue. Leaves don't have children
        all_ids = set(df["id"])
        parents_with_children = set(child_counts.keys())
        leaves = list(all_ids - parents_with_children)

        # 3. Push sizes up the tree iteratively
        while leaves:
            current_id = leaves.pop()
            current_size = sizes.get(current_id, 0)
            parent_id = parent_map.get(current_id)

            if pd.notna(parent_id) and parent_id in sizes:
                sizes[parent_id] += current_size
                child_counts[parent_id] -= 1

                # if parent_id not in child_counts so we can add it to the leaves queue
                if child_counts[parent_id] == 0:
                    leaves.append(parent_id)

        # 4. Write back the calculated sizes to the DataFrame
        is_folder = df["is_folder"]
        df.loc[is_folder, "size_bytes"] = df.loc[is_folder, "id"].map(sizes)

        # update human-readable sizes for folders
        df.loc[is_folder, "size_readable"] = df.loc[is_folder, "size_bytes"].apply(convert_size)
        return df

    def delete_items(
        self,
        items: list[FileItem],
        batch_size: int = 100,
        on_progress: Callable[[int], None] | None = None,
    ) -> OperationResult:
        """
        Deletes the specified items from Google Drive, providing progress updates via the on_progress callback.
        """
        if not items:
            self.logger.debug("Delete_items called with empty list")
            return OperationResult(0, 0, 0, [])

        # Prepare metadata for all items to be deleted. This can be used by the core for logging, error handling, or progress reporting.
        metadata = {}
        for item in items:
            metadata[item.id] = {
                "name": item.name,
                "type": "dir" if item.mime_type == "application/vnd.google-apps.folder" else "file",
                "size": convert_size(item.size),
            }

        ids = [item.id for item in items]

        self.logger.debug(f"Prepared metadata for {len(ids)} items. Sending to core.")

        return self.drive.delete_ids(
            ids=ids, metadata=metadata, batch_size=batch_size, on_progress=on_progress
        )


    def fetch_item(
        self,
        item_or_id: FileItem | str,
        output_path: Path,
        recursive: bool = False,
        force: bool = False,
        export: bool = False,
        dry_run: bool = False,
        on_progress: Callable | None = None,
    ):
        item: FileItem | None
        if isinstance(item_or_id, str):
            item = self.drive.get_file_metadata(item_or_id)
        else:
            item = item_or_id

        if item is None:
            return

        # export mapping for Google Docs formats to MS Office formats
        mapping = {
            "application/vnd.google-apps.document": (
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ".docx",
            ),
            "application/vnd.google-apps.spreadsheet": (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ".xlsx",
            ),
            "application/vnd.google-apps.presentation": (
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ".pptx",
            ),
        }

        if item.mime_type == "application/vnd.google-apps.folder":
            folder_name = item.name
            self.logger.info(f"Downloading directory: {item.name}")
            if (output_path / folder_name).exists() and not force:
                folder_name = f"{folder_name}_{item.id}"
                self.logger.debug(
                    f"Directory with name '{item.name}' already exists. Use to '{folder_name}' to avoid conflict."
                )
            target_dir = output_path / folder_name

            if dry_run:
                print(f"[DRY-RUN] Would create directory: {target_dir}", file=sys.stderr)
            else:
                target_dir.mkdir(parents=True, exist_ok=True)

            children = self.drive.list_files(FileFilter(folder_id=item.id))
            for child in children:
                if child.mime_type == "application/vnd.google-apps.folder":
                    if recursive:
                        self.fetch_item(
                            child, target_dir, recursive, force, export, dry_run, on_progress
                        )
                else:
                    self._download_item(
                        child, target_dir, force, export, mapping, dry_run, on_progress
                    )
        else:
            self._download_item(item, output_path, force, export, mapping, dry_run, on_progress)

    def _download_item(
        self,
        item: FileItem,
        target_dir: Path,
        force: bool,
        export: bool,
        mapping: dict[str, tuple[str, str]],
        dry_run: bool,
        on_progress: Callable | None = None,
    ):
        """
        Downloads a single file item, handling Google Docs export, name conflicts, and progress reporting.
        """
        name = item.name
        is_google_doc = item.mime_type in mapping

        # 1. Handling Google Doc export
        if is_google_doc:
            if not export:
                self.logger.debug(f"Skip Google Doc export {name} (use -e)")
                return
            _, ext = mapping[item.mime_type]
            if not name.lower().endswith(ext):
                name += ext

        final_path = target_dir / name

        # 2. Check name Conflict Resolution
        if final_path.exists() and not force:
            # Check size match for non-Google Docs (for Google Docs we will export anyway, as size can differ)
            self.logger.debug(f"File '{final_path}' already exists. Checking for conflicts...")
            if not is_google_doc and final_path.stat().st_size == item.size:
                self.logger.debug(f"Skip (size match): {name}")
                if on_progress:
                    on_progress(item.id, item.name, item.size, item.size, "finished")
                return

            self.logger.debug("Conflict detected. Renaming the file to avoid overwrite.")
            final_path = target_dir / f"{final_path.stem}_{item.id}{final_path.suffix}"

        # 3. Dry-run
        if dry_run:
            action = "[DRY-RUN] Would export" if is_google_doc else "[DRY-RUN] Would download"
            self.logger.info(f"{action}: {item.name} -> {final_path} ({convert_size(item.size)})")
            return

        # 4. Prepare temp file
        temp_path = final_path.with_suffix(final_path.suffix + ".tmp")
        self.logger.debug(f"Downloading to temp: {temp_path}")

        try:
            # Wrapper for UI callback
            def cb_wrapper(bytes_downloaded, status):
                if on_progress:
                    on_progress(item.id, item.name, bytes_downloaded, item.size, status)

            if is_google_doc:
                export_mime, _ = mapping[item.mime_type]
                self.drive.export_media(item.id, export_mime, temp_path, on_progress=cb_wrapper)
            else:
                self.drive.download_media(item.id, temp_path, on_progress=cb_wrapper)

            # Finalize: move temp to final
            if final_path.exists():
                final_path.unlink()
            temp_path.rename(final_path)

            self.logger.info(f"Fetched: {final_path.name}")

        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            self.logger.error(f"Failed {name}: {e}")
            if on_progress:
                # callback with error status
                on_progress(item.id, item.name, 0, item.size, "error")
            raise e


    def _setup_test_structure(self, root_folder_id: str):
        self.logger.info("Creating test structure in folder: %s", root_folder_id)

        # 1. Create main test folder and subfolder
        test_folder_id = self.drive.create_item(
            "Test_Folder", "application/vnd.google-apps.folder", parent_id=root_folder_id
        )

        subfolder1_id = self.drive.create_item(
            "Subfolder1", "application/vnd.google-apps.folder", parent_id=test_folder_id
        )

        subfolder2_id = self.drive.create_item(
            "Subfolder2", "application/vnd.google-apps.folder", parent_id=subfolder1_id
        )

        # 2. Create files in root and subfolder
        files_to_create = [
            {"name": "test_file.txt", "parent": test_folder_id, "content": "Hello Root"},
            {"name": "sub_file_1.txt", "parent": subfolder1_id, "content": "Content 1"},
            {"name": "sub_file_2.txt", "parent": subfolder2_id, "content": "Content 2"},
            # File with the same name in root to test conflict handling
            {"name": "test_file.txt", "parent": subfolder1_id, "content": "I am a shadow"},
        ]

        for f in files_to_create:
            fid = self.drive.create_item(f["name"], "text/plain", f["parent"], f["content"])
            self.logger.info("Created file: %s (ID: %s)", f["name"], fid)

    def get_quota_info(self):
        try:
            quota = self.drive.get_quota()
            return {
                "limit": convert_size(quota.get("limit", 0)),
                "usage": convert_size(quota.get("usage", 0)),
                "free": convert_size(quota.get("free", 0)),
            }
        except Exception:
            return {"limit": "N/A", "usage": "N/A", "free": "N/A"}

    def get_item(self, file_id: str) -> FileItem | None:
        return self.drive.get_file_metadata(file_id)

    def get_items_batch(
        self, ids: list[str], limit: int | None = None, batch_size: int = 100
    ) -> dict[str, FileItem | None]:
        return self.drive.get_files_metadata_batch(ids=ids, limit=limit, batch_size=batch_size)
