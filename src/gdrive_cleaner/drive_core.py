#  drive_core.py
import io
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest, MediaIoBaseDownload
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


# ==============================
# FILTER MODEL
# ==============================


@dataclass
class FileFilter:
    folder_id: str | None = None
    folder_name: str | None = None
    name_exact: str | None = None
    name_contains: str | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    mime_type: str | None = None
    exclude_mime_type: str | None = None
    owner_only: bool = True
    trashed: bool = False

    @classmethod
    def folders(cls, **kwargs):
        return cls(mime_type=FOLDER_MIME, **kwargs)

    @classmethod
    def files(cls, **kwargs):
        return cls(exclude_mime_type=FOLDER_MIME, **kwargs)


# ==============================
# RESULT MODELS
# ==============================


@dataclass
class FileItem:
    id: str
    name: str
    size: int
    mime_type: str
    created_at: datetime | None
    modified_at: datetime | None
    parents: list[str]
    owner: str | None


@dataclass
class OperationEntry:
    id: str
    name: str
    type: str  # 'dir' or 'file'
    size: str
    status: str  # 'success' or 'error'
    error: str | None = None


@dataclass
class OperationResult:
    total: int
    success: int | None
    failed: int | None
    entries: list[OperationEntry] = field(default_factory=list)
    errors: list[dict[str, Any]] | None = field(default_factory=list)  # for legacy support. can be deleted later


# ==============================
# CORE
# ==============================
FOLDER_MIME = "application/vnd.google-apps.folder"


class DriveCore:
    def __init__(
            self, service_account_path: str, *, custom_logger: logging.Logger | None = None
    ):
        self.logger = custom_logger or logger
        self.service_account_path = service_account_path
        self.service_account_email = self._get_service_account_email()
        self.service = self._authenticate()

        self._retry = Retrying(
            before_sleep=before_sleep_log(self.logger, logging.WARNING),  # type: ignore
            retry=retry_if_exception(self._is_retryable),
            wait=wait_exponential(multiplier=1, min=1, max=60),
            stop=stop_after_attempt(5),
            reraise=True,
        )

    # ----------------------------------
    # AUTH
    # ----------------------------------

    def _get_service_account_email(self) -> str:
        with open(self.service_account_path) as f:
            return json.load(f)["client_email"]

    def _authenticate(self):
        self.logger.info("Authenticating with Google Drive API...")
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_path,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        self.logger.info("Authentication successful. Service account: %s", self.service_account_email)
        return service

    # ----------------------------------
    # EXECUTE WITH RETRIES
    # ----------------------------------

    @staticmethod
    def _is_retryable(e: BaseException) -> bool:
        if not isinstance(e, HttpError):
            return False
        return e.resp.status in (403, 429, 500, 503)

    def _execute(self, request):
        try:
            self.logger.debug("Executing Drive request: method=%s uri=%s", request.method, request.uri, )
            return self._retry(request.execute)
        # Only for logging the error message
        except HttpError as e:
            try:
                err_details = json.loads(e.content).get('error', {}).get('message', 'Unknown error')
            except (json.JSONDecodeError, KeyError):
                err_details = str(e)
            self.logger.debug("Drive API Error: status=%s, message=%s", e.resp.status, err_details)
            raise e

    def _execute_batch(self, batch: BatchHttpRequest):
        if not batch._requests:  # noqa
            self.logger.debug("Skipping empty batch request")
            return
        self.logger.debug("Executing batch request with %d sub-requests", len(batch._requests))  # noqa
        self._retry(batch.execute)

    # ----------------------------------
    # QUERY BUILDER
    # ----------------------------------

    @staticmethod
    def _escape_query_value(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _build_query(self, file_filter: FileFilter) -> str:
        conditions = []

        if file_filter.owner_only:
            conditions.append(f"'{self.service_account_email}' in owners")

        if file_filter.folder_id:
            conditions.append(f"'{file_filter.folder_id}' in parents")

        if file_filter.name_exact:
            safe = self._escape_query_value(file_filter.name_exact)
            conditions.append(f"name = '{safe}'")

        if file_filter.name_contains:
            safe = self._escape_query_value(file_filter.name_contains)
            conditions.append(f"name contains '{safe}'")

        if file_filter.created_after:
            iso = file_filter.created_after.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0,
                                                    microsecond=0).isoformat()
            conditions.append(f"createdTime > '{iso}'")

        if file_filter.created_before:
            iso = file_filter.created_before.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0,
                                                     microsecond=0).isoformat()
            conditions.append(f"createdTime < '{iso}'")

        if file_filter.mime_type:
            conditions.append(f"mimeType = '{file_filter.mime_type}'")

        if file_filter.exclude_mime_type:
            conditions.append(f"mimeType != '{file_filter.exclude_mime_type}'")

        if not file_filter.trashed:
            conditions.append("trashed = false")

        return " and ".join(conditions)

    # ----------------------------------
    # LIST FILES
    # ----------------------------------

    def list_files(
            self,
            file_filter: FileFilter | None = None,
            page_size: int = 1000,
            limit: int | None = None,
            on_progress: Callable[[int | None], None] | None = None
    ) -> list[FileItem]:

        if file_filter is None:
            file_filter = FileFilter()

        query = self._build_query(file_filter)

        files: list[FileItem] = []
        page_token = None

        while True:

            request = self.service.files().list(
                q=query,
                pageSize=page_size,
                pageToken=page_token,
                fields="nextPageToken, files(id,name,size,mimeType,parents,createdTime,modifiedTime,owners)",
            )

            response = self._execute(request)

            for item in response.get("files", []):
                files.append(
                    FileItem(
                        id=item["id"],
                        name=item["name"],
                        size=int(item.get("size", 0)),
                        mime_type=item.get("mimeType"),
                        created_at=self._parse_datetime(item.get("createdTime")),
                        modified_at=self._parse_datetime(item.get("modifiedTime")),
                        parents=item.get("parents", []),
                        owner=item["owners"][0]["emailAddress"] if item.get("owners") else None,
                    )
                )

                if limit and len(files) >= limit:
                    return files

            if on_progress:
                on_progress(len(files))

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return files

    # ----------------------------------
    # GET FILE INFO
    # ----------------------------------
    def get_file_metadata(self, file_id: str) -> FileItem | None:

        try:
            response = self._execute(
                self.service.files().get(
                    fileId=file_id,
                    fields="id,name,size,mimeType,parents,createdTime,modifiedTime,owners",
                )
            )

        except HttpError as e:
            if e.resp.status == 404:
                # No need log because error is logged in _execute()
                return None
            raise

        return FileItem(
            id=response["id"],
            name=response["name"],
            size=int(response.get("size", 0)),
            mime_type=response.get("mimeType"),
            created_at=self._parse_datetime(response.get("createdTime")),
            modified_at=self._parse_datetime(response.get("modifiedTime")),
            parents=response.get("parents", []),
            owner=response["owners"][0]["emailAddress"] if response.get("owners") else None,
        )

    def get_files_metadata_batch(
            self, ids: Iterable[str], limit: int | None = None, batch_size: int = 100,
    ) -> dict[str, FileItem | None]:

        result: dict[str, FileItem | None] = {}

        if not ids:
            return result

        ids = list(dict.fromkeys(ids))  # Deduplicate while preserving order

        def callback(request_id, response, exception):
            if exception:
                result[request_id] = None
            else:
                result[request_id] = FileItem(
                    id=response["id"],
                    name=response["name"],
                    size=int(response.get("size", 0)),
                    mime_type=response.get("mimeType"),
                    created_at=self._parse_datetime(response.get("createdTime")),
                    modified_at=self._parse_datetime(response.get("modifiedTime")),
                    parents=response.get("parents", []),
                    owner=response["owners"][0]["emailAddress"] if response.get("owners") else None,
                )

        ids_iterator = islice(ids, limit)

        while True:
            chunk = list(islice(ids_iterator, batch_size))
            if not chunk:
                break
            batch = self.service.new_batch_http_request(callback=callback)
            for fid in chunk:
                batch.add(
                    self.service.files().get(
                        fileId=fid,
                        fields="id,name,size,mimeType,parents,createdTime,modifiedTime,owners",
                    ),
                    request_id=fid,
                )
            self._execute_batch(batch)

        return result

    # ----------------------------------
    # GET FOLDER NAMES (BATCH)
    # ----------------------------------

    def get_folder_names(self, ids: Iterable[str], limit: int | None = None) -> dict[str, str]:

        metadata = self.get_files_metadata_batch(ids, limit)
        names_map = {}
        for fid, item in metadata.items():
            if item and item.mime_type == FOLDER_MIME:
                names_map[fid] = item.name

        return names_map

    # ----------------------------------
    # QUOTA
    # ----------------------------------

    def get_quota(self) -> dict[str, int]:

        request = self.service.about().get(fields="storageQuota")
        about = self._execute(request)
        quota = about["storageQuota"]

        limit = int(quota.get("limit", 0))
        usage = int(quota.get("usage", 0))

        return {
            "limit": limit,
            "usage": usage,
            "free": limit - usage,
        }

    # ----------------------------------
    # DELETE (BATCH)
    # ----------------------------------

    def delete_ids(self, ids: list[str], metadata: dict[str, dict[str, Any]] | None = None,
                   batch_size: int = 100, on_progress: Callable[[int], None] | None = None) -> OperationResult:
        """
        Delete files/folders by their IDs in batches, with detailed reporting and progress callback.
        :param ids: list of file IDs to delete.
        :param metadata: Map with metadata {id: {'name': '...', 'type': '...', 'size': '...'}} for reporting purposes.
                        If not provided, ID will be used as name and 'unknown' as type in the report.
        :param batch_size: Batch size for API calls (max 100).
        :param on_progress: UI callback function that receives the number of processed items in each batch.
        """
        if not ids:
            return OperationResult(total=0, success=0, failed=0, errors=[])

        ids = list(dict.fromkeys(ids))

        entries = []
        success_count = 0
        failed_count = 0

        def callback(request_id, response, exception):
            nonlocal success_count, failed_count

            # Metadata for reporting purposes; if not provided, will use ID as name and 'unknown' as type
            info = metadata.get(request_id, {}) if metadata else {}
            item_name = info.get('name', request_id)
            item_type = info.get('type', 'unknown')
            item_size = info.get('size', 'N/A')

            if exception:
                # 404 is treated as success because the item is effectively deleted/not found
                if isinstance(exception, HttpError) and exception.resp.status == 404:
                    success_count += 1
                    entries.append(OperationEntry(
                        id=request_id, name=item_name, type=item_type,
                        size=item_size, status='success'
                    ))
                else:
                    failed_count += 1
                    entries.append(OperationEntry(
                        id=request_id, name=item_name, type=item_type,
                        size=item_size, status='error', error=str(exception)
                    ))
                    logger.debug("Failed to delete file ID %s: %s", request_id, exception)

            else:
                success_count += 1
                entries.append(OperationEntry(
                    id=request_id, name=item_name, type=item_type,
                    size=item_size, status='success'
                ))

        ids_iterator = iter(ids)

        while True:
            chunk = list(islice(ids_iterator, batch_size))
            if not chunk:
                break

            self.logger.debug(f"Starting batch delete for {len(chunk)} items")
            batch = self.service.new_batch_http_request(callback=callback)
            for fid in chunk:
                batch.add(self.service.files().delete(fileId=fid), request_id=fid)

            self._execute_batch(batch)

            # UI Callback with number of processed items in the batch
            if on_progress:
                on_progress(len(chunk))
            self.logger.debug(f"Completed batch delete for {len(chunk)} items")

        return OperationResult(
            total=len(ids),
            success=success_count,
            failed=failed_count,
            entries=entries
        )

    # ----------------------------------
    # DOWNLOAD FILE CONTENT
    # ----------------------------------
    def download_media(self, file_id: str, destination_path: Path, on_progress: Callable | None = None):
        """Download file content (for non-Google formats) to the specified path."""
        request = self.service.files().get_media(fileId=file_id)
        self._execute_download(request, destination_path, on_progress)

    def export_media(self, file_id: str, mime_type: str, destination_path: Path,
                     on_progress: Callable | None = None):
        """Export Google Docs/Sheets/Slides to the specified MIME type and save to the path."""
        request = self.service.files().export_media(fileId=file_id, mimeType=mime_type)
        self._execute_download(request, destination_path, on_progress)

    def _execute_download(self, request, destination_path: Path,
                          on_progress: Callable[[int, str], None] | None = None):
        """
        Inner logic for downloading or exporting media with retries and progress callback.
        """

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        filename = destination_path.name

        self.logger.debug(f"HTTP request started for: {filename}")
        try:
            with io.FileIO(str(destination_path), "wb") as file_handler:
                downloader = MediaIoBaseDownload(file_handler, request, chunksize=1024 * 1024)
                done = False
                while not done:
                    status, done = self._retry(downloader.next_chunk)
                    if status and on_progress:
                        # UI Callback
                        on_progress(int(status.resumable_progress), "progress")

            self.logger.debug(f"HTTP request finished successfully: {filename}")
            if on_progress:
                on_progress(0, "finished")  # success callback with 0 progress to indicate completion

        except Exception as e:
            self.logger.debug(f"HTTP request failed: {filename} | Error: {str(e)}")
            if destination_path.exists():
                destination_path.unlink()
            if on_progress:
                on_progress(
                    0, "error"
                )  # Error callback with 0 progress to indicate failure
            raise e

    # -----------------------------------
    # COPY FILE
    # -----------------------------------

    def copy_file(self, file_id: str, new_name: str | None = None, target_id: str | None = None,
                  fields: str = "id,name,parents") -> dict:
        """
        Create a copy of a file. Returns response dict (contains at least 'id').
        :param file_id: ID of source file
        :param new_name: optional new name for the copy
        :param target_id: optional parent folder ID to place the copy into
        :param fields: fields to request back from API
        """
        body = {}
        if new_name:
            body["name"] = new_name
        if target_id:
            body["parents"] = [target_id]

        request = self.service.files().copy(fileId=file_id, body=body, fields=fields)
        return self._execute(request)

    # ----------------------------------
    # CREATE FILE OR FOLDER (TESTING PURPOSES)
    # ----------------------------------
    def create_item(self, name: str, mime_type: str, parent_id: str | None = None,
                    content: str | None = None) -> str:
        """
        Create a new file or folder. If content is provided, creates a file with that content;
        otherwise, creates an empty file or folder.
        For testing purposes.
        """
        file_metadata = {'name': name, 'mimeType': mime_type}
        if parent_id:
            file_metadata['parents'] = [parent_id]

        if content:
            from googleapiclient.http import MediaIoBaseUpload
            media = MediaIoBaseUpload(io.BytesIO(content.encode()), mimetype=mime_type, resumable=True)
            return self._execute(self.service.files().create(body=file_metadata, media_body=media, fields='id'))['id']
        else:
            return self._execute(self.service.files().create(body=file_metadata, fields='id'))['id']

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    # ----------------------------------
    # MOVE FILE OR FOLDER (TESTING PURPOSES)
    # ----------------------------------

    def move_items_batch(self, ids: list[str], new_parent_id: str, remove_old_parents: bool = True,
                         supports_all_drives: bool = False, batch_size: int = 100,
                         on_progress: Callable = None) -> OperationResult:
        """Move multiple files/folders by updating parents using files.update in batches (no copy+delete)."""

        ids = list(dict.fromkeys(ids))

        item_list = self.get_files_metadata_batch(ids)

        entries = []
        success_count = 0
        failed_count = 0

        def callback(request_id, response, exception):
            nonlocal success_count, failed_count
            item = item_list.get(request_id)
            if item:
                item_name = item.name
                item_type = 'dir' if item.mime_type == FOLDER_MIME else 'file'
                item_size = item.size if item.mime_type != FOLDER_MIME else 'N/A'
            else:
                item_name = request_id
                item_type = 'unknown'
                item_size = 'N/A'

            if exception:
                failed_count += 1
                status = 'error'
                error = str(exception)
                logger.error(f"Failed to move {request_id}: {exception}")
            else:
                success_count += 1
                status = 'success'
                error = None
                logger.debug(f"File {request_id} moved successfully")

            entry = OperationEntry(
                id=request_id, name=item_name, type=item_type, size=item_size, status=status, error=error
            )
            entries.append(entry)

        ids_iterator = iter(ids)
        while True:
            chunk = list(islice(ids_iterator, batch_size))
            if not chunk:
                break

            self.logger.debug(f"Starting batch moving for {len(chunk)} items")
            batch = self.service.new_batch_http_request(callback=callback)
            for fid in chunk:
                if fid not in item_list or item_list[fid] is None:
                    logger.error(f"Skipping move for {fid} because metadata could not be retrieved")
                    continue
                item = item_list[fid]
                remove_parents = [p for p in item.parents if p != new_parent_id] if remove_old_parents else []

                request_kwargs = {
                    "fileId": fid,
                }
                if new_parent_id:
                    request_kwargs["addParents"] = new_parent_id
                if remove_parents:
                    request_kwargs["removeParents"] = ",".join(remove_parents)
                if supports_all_drives:
                    request_kwargs["supportsAllDrives"] = True

                batch.add(self.service.files().update(**request_kwargs), request_id=fid)

            self._execute_batch(batch)

            # UI Callback with number of processed items in the batch
            if on_progress:
                on_progress(len(chunk))

            self.logger.debug(f"Completed batch moving for {len(chunk)} items")

        return OperationResult(
            total=len(ids),
            success=success_count,
            failed=failed_count,
            entries=entries
        )

    def move_item(self, file_id: str, new_parent_id: str, remove_old_parents: bool = True,
                  supports_all_drives: bool = False, fields: str = "id,parents") -> dict:
        """
        Move file/folder by updating parents using files.update (no copy+delete).
        If remove_old_parents is True, current parents (except new_parent_id) will be removed.
        """

        meta = self.get_file_metadata(file_id)
        current_parents = meta.parents if meta else []

        remove_parents = [p for p in current_parents if p != new_parent_id] if remove_old_parents else []

        request_kwargs = {
            "fileId": file_id,
            "fields": fields,
        }
        if new_parent_id:
            request_kwargs["addParents"] = new_parent_id
        if remove_parents:
            request_kwargs["removeParents"] = ",".join(remove_parents)
        if supports_all_drives:
            request_kwargs["supportsAllDrives"] = True

        request = self.service.files().update(**request_kwargs)
        return self._execute(request)
