# GDrive Cleaner CLI

Command-line utility for Google Drive inventory, cleanup, export, and download workflows using a Google Service Account.

## Important Disclaimer

**Use this tool at your own risk.**

This project is provided **"AS IS"**, without warranties of any kind (express or implied), including but not limited to merchantability, fitness for a particular purpose, and non-infringement.

By using this utility, you accept full responsibility for all actions and outcomes, including accidental or irreversible data loss, deletion of production data, permission-side effects, and operational downtime. The authors and contributors are **not liable** for any direct or indirect damage resulting from usage.

Always run destructive operations with `--dry-run` first, verify IDs, and test in a non-production Drive before using on real data.

## What the Tool Does

- Lists files/folders with time filters.
- Exports inventory to CSV/XLSX.
- Deletes by single ID, by IDs file, by date criteria, or by clearing a folder.
- Downloads a file or folder tree.
- Exports Google Docs/Sheets/Slides to Office formats on download.
- Shows storage quota.

## Architecture (from source)

- `cli.py`: parser, command dispatch, UX/status/progress, confirmations, summary/report prompts.
- `drive_core.py`: Google Drive API layer (auth, query builder, list/get/delete/download/export, retries).
- `operations.py`: higher-level operations (reports, folder-size aggregation, fetch collision logic, metadata mapping for reports).

## Requirements

- Python 3.10+
- Google Service Account JSON key with Drive API access
- Dependencies from `pyproject.toml`

Install:

```bash
python -m pip install -e .
```

Run:

```bash
gdrive-cleaner --help
# Alias
gdc --help
# Alternative:
python -m gdrive_cleaner.cli --help
```

Optional extras:

```bash
# Development dependencies (tests)
python -m pip install -e ".[dev]"

# Jupyter/Notebook dependencies (including PyCharm notebook workflow)
python -m pip install -e ".[notebook]"

# All optional groups
python -m pip install -e ".[dev,notebook]"
```

## Authentication

The CLI resolves credentials in this order:

1. `--sa <JSON_FILE>`
2. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. `service_account.json` in project root

If the resolved file does not exist, command exits with error.

## Global Options

Available for all commands:

- `-s, --sa <JSON_FILE>`: service account key path.
- `-v`, `-vv`: logging verbosity (`-v` = INFO, `-vv` = DEBUG).
- `-d, --dry-run`: preview mode.
- `--force`: skip confirmation prompts; for `fetch` it also changes collision behavior.

## Commands

### 1) `list`

Usage:

```bash
gdrive-cleaner list [PARENT_ID] [filters/options]
```

Options:

- `id` (positional, optional): parent folder ID.
- `-o, --older <DAYS>`: created before (today - DAYS).
- `-b, --before <YYYY-MM-DD>`: created before date (boundary day excluded).
- `-n, --newer <DAYS>`: created after (today - DAYS + 1).
- `-a, --after <YYYY-MM-DD>`: created after date (boundary day excluded).
- `--csv [PATH]`: export inventory to CSV. If path is omitted, auto-name is generated in `export/`.
- `--xlsx [PATH]`: export inventory to XLSX. If path is omitted, auto-name is generated in `export/`.
- `-f, --folders-only`: include folders only.
- `-r, --resolve-parents`: resolve external parent folder names in exports.
- `-l, --limit <COUNT>`: max items to list/export.
- `--name <NAME>`: exact file/folder name filter.
- `--contains <TEXT>`: file/folder name contains filter.

Notes:

- Date bounds are validated; invalid range aborts.
- Console output is paged and truncated for readability; redirected/piped stdout prints full rows.

### 2) `delete`

Usage:

```bash
gdrive-cleaner delete [ID] [-i IDS_FILE] [time-filters] [--force] [-d] [--csv]
```

Input source:

- `id` (single item ID), or
- `-i, --ids-file <PATH>` (`.csv/.xlsx/.xls` with `id` column), or
- time filters (`--older/--before/--newer/--after`).

Important parser/validation rules:

- `id` and `--ids-file` are mutually exclusive.
- Mixing ID source (`id`/`--ids-file`) with API filters (`--older/--before/--newer/--after/--name/--contains`) is rejected at runtime.
- `--csv` is independent from input source and can be combined with `id`, `--ids-file`, or API filters.

Behavior:

- Without `--force`, asks explicit `yes` confirmation.
- In `--dry-run`, shows up to first 200 entries.
- Batch deletion uses Drive API batch calls.
- `404 Not Found` during delete is counted as success.

### 3) `clear-folder`

Usage:

```bash
gdrive-cleaner clear-folder <FOLDER_ID> [time-filters] [--force] [-d] [--csv]
```

Options:

- `folder_id` (required): target folder ID.
- Time filters: `--older`, `--before`, `--newer`, `--after`.
- Name filters: `--name`, `--contains`.
- `--csv`: always save detailed CSV report without prompt.

Behavior:

- The folder itself is not deleted by this command.
- Matching child folders are deleted as items; deleting such folder items removes nested content recursively.
- `--dry-run` prints what would be deleted.

### 4) `fetch`

Usage:

```bash
gdrive-cleaner fetch <ID> [-p PATH] [-r] [-e] [--force] [-d]
```

Options:

- `id` (required): file or folder ID.
- `-p, --path <PATH>`: destination root (default `download`).
- `-r, --recursive`: recurse into subfolders.
- `-e, --export`: export Google Docs formats to Office files.
- `-d, --dry-run`: preview actions.
- `--force`: do not auto-rename on collisions; allows overwrite flow.

Google export mapping:

- Docs -> `.docx`
- Sheets -> `.xlsx`
- Slides -> `.pptx`

Download/collision mechanics:

- If target folder already exists and `--force` is **not** set, folder name is changed to `<name>_<folder_id>`.
- For files, if same path exists and `--force` is **not** set:
  - for non-Google files with equal size: file is skipped as already matching;
  - otherwise file name is changed to `<stem>_<file_id><suffix>`.
- With `--force`, existing destination file can be replaced (download is written to `*.tmp` then moved to final path).
- Google-native files are skipped unless `-e/--export` is enabled.

### 5) `copy`

Usage:

```bash
gdrive-cleaner copy <ID> [-n NEW_NAME] [-t FOLDER_ID] [--force] [-d]
```

Options:

- `id` (required): source file ID.
- `-n, --name <NEW_NAME>`: optional new file name for the copied item.
- `-t, --target_id <FOLDER_ID>`: optional target folder ID.
- `-d, --dry-run`: preview copy action.
- `--force`: skip confirmation prompt.

Behavior:

- Copies only files; copying folders is not supported.
- Without `--force`, asks explicit `yes` confirmation.
- In `--dry-run`, prints what would be copied and does not call Drive API copy.

### 6) `quota`

Usage:

```bash
gdrive-cleaner quota
```

Outputs: Used, Free, Total.

## Reports and Outputs

### Inventory export (`list --csv/--xlsx`)

- Purpose: snapshot of discovered items.
- Contains file/folder metadata (ID, name, owner, size, mime, timestamps, parent info, etc.).
- XLSX includes additional sheets (`All Items`, `Files`, `Folders`, `Summary`).

### Operation report (`delete --csv`, `clear-folder --csv`, or interactive prompt)

- File name: `report_<folder_name?>_<timestamp>.csv` in current working directory.
- Contains one record per processed top-level deletion item with columns:
  - `ID`, `Name`, `Type`, `Size`, `Status`, `Error`.
- First line is a disclaimer:
  - `Note: Folders are deleted with all their content. Sub-items are not listed individually.`
- Nested sub-items of deleted folders are not expanded row-by-row.

### stdout/stderr behavior

- Human UX/status/progress/warnings are printed to stderr-oriented console paths.
- Non-TTY summary for delete/clear-folder is machine-friendly (`total=... success=... failed=... critical=...`) in stdout.
- Errors list stays in stderr.

## IDs File Format (`delete -i`)

- Supported extensions: `.csv`, `.xlsx`, `.xls`.
- Required column: `id`.
- Empty/NaN values are ignored.
- IDs are deduplicated before batch metadata lookup/deletion.

## Testing

Run regular tests (offline, no real Drive access):

```bash
pytest
```

Run real Google Drive integration tests (explicit opt-in):

```bash
# 1) Copy local template (do not commit .env.test):
# cp .env.test.example .env.test
#
# 2) Fill values in .env.test:
# RUN_GDRIVE_INTEGRATION=1
# GOOGLE_APPLICATION_CREDENTIALS=/abs/path/to/service_account.json
# TEST_DRIVE_FOLDER_ID=<safe test parent folder id>
#
# 3) Run:
pytest -m integration
```

Notes:
- Integration tests create temporary items under `TEST_DRIVE_FOLDER_ID` and remove them in cleanup.
- Use a dedicated non-production test folder only.
- `pytest` auto-loads `.env` and `.env.test` if present; shell env vars have priority.

## Key Behavior and Risks

1. **Owner-only scope by default in API queries.**  
   Listing/filter queries use `'<service_account_email>' in owners` by default. Non-owned files may be invisible to list/delete selection logic.

2. **Folder deletion is recursive at Drive level.**  
   Deleting a folder item removes nested content by Drive behavior.

3. **Date filters use UTC-normalized day boundaries.**  
   `--before/--after/--older/--newer` are interpreted in UTC day logic.

4. **`--force` is high-risk for destructive operations.**  
   It bypasses interactive safety confirmation for delete flows.

5. **Dry-run is preview-only, not a lock.**  
   Data can change between preview and real execution.

6. **Delete error semantics.**  
   `404` on delete counts as success (already missing). Other API errors are tracked as failures.

7. **In-process list cache exists in operations layer.**  
   Designed to reuse one listing result for sequential exports in the same run.

## Practical Safe Workflow

1. Start with `list` and export (`--csv` / `--xlsx`).
2. Run destructive command with `--dry-run`.
3. Verify IDs and ownership model.
4. Execute with confirmation (avoid `--force` unless fully sure).
5. Save operation CSV report for audit trail.

## Examples

```bash
# List folders older than 90 days in a specific parent
gdrive-cleaner list <PARENT_ID> --folders-only --older 90

# Export analysis report to XLSX
gdrive-cleaner list <PARENT_ID> --xlsx audit.xlsx --resolve-parents

# Delete by date filter and always save operation report
gdrive-cleaner delete --older 120 --csv

# Delete by IDs file (interactive confirmation, optional prompt for report in TTY)
gdrive-cleaner delete -i to_delete.xlsx

# Clear old content in folder (preview)
gdrive-cleaner clear-folder <FOLDER_ID> --before 2025-01-01 --dry-run

# Recursively fetch a folder and export Google docs
gdrive-cleaner fetch <FOLDER_ID> -r -e -p ./backup

# Copy file with a new name into target folder
gdrive-cleaner copy <FILE_ID> -n "Copy of report.xlsx" -t <TARGET_FOLDER_ID>

# Show quota
gdrive-cleaner quota
```

## Exit/Failure Notes

- Missing credentials file -> immediate error exit.
- Invalid date format -> parser error (`YYYY-MM-DD` required).
- Invalid date range or incompatible filter combination -> command abort.
- Unhandled runtime errors -> non-zero exit with concise message (details available via verbose logging).
