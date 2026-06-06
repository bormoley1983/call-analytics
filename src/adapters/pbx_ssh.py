# src/adapters/pbx_ssh.py
from __future__ import annotations

import logging
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import Callable, List, Optional

import paramiko

logger = logging.getLogger(__name__)


class PbxSshDownloader:
    """
    Downloads new call recordings from PBX via SFTP.
    Call recordings are expected to be in a flat or date-structured remote directory.
    """

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "asterisk",
        password: Optional[str] = None,
        key_path: Optional[str] = None,
        known_hosts_path: Optional[str] = None,
        remote_dir: str = "/var/spool/asterisk/monitor",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_path = key_path
        self.known_hosts_path = known_hosts_path
        self.remote_dir = remote_dir
        self._client: Optional[paramiko.SSHClient] = None

    def connect(self) -> None:
        logger.info(
            "Connecting to PBX via SSH: host=%s port=%s user=%s remote_dir=%s",
            self.host,
            self.port,
            self.username,
            self.remote_dir,
        )
        self._client = paramiko.SSHClient()
        if self.known_hosts_path and Path(self.known_hosts_path).exists():
            self._client.load_host_keys(self.known_hosts_path)
        else:
            self._client.load_system_host_keys()
        self._client.set_missing_host_key_policy(paramiko.RejectPolicy())  # secure default

        if self.key_path:
            self._client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                key_filename=self.key_path,
            )
        elif self.password:
            self._client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
        else:
            self._client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
            )
        logger.info("PBX SSH connection established")

    def close(self) -> None:
        if self._client:
            self._client.close()
            logger.info("PBX SSH connection closed")

    def _iter_scan_roots(
        self,
        remote_root: str,
        allowed_days: Optional[set[str]] = None,
    ) -> List[tuple[str, str]]:
        root = remote_root.rstrip("/")
        if not allowed_days:
            return [("", root)]

        scan_roots: List[tuple[str, str]] = []
        for day in sorted(allowed_days):
            parts = [part for part in day.split("/") if part]
            if len(parts) != 3:
                logger.warning("Skipping unsupported PBX day filter: %s", day)
                continue
            rel_prefix = "/".join(parts)
            scan_roots.append((rel_prefix, f"{root}/{rel_prefix}"))
        return scan_roots or [("", root)]

    def _iter_remote_files(
        self,
        sftp: paramiko.SFTPClient,
        remote_root: str,
        allowed_days: Optional[set[str]] = None,
    ) -> Iterator[str]:
        """
        Recursively yield file paths under remote_root as POSIX-relative paths.
        """
        stack: List[tuple[str, str]] = list(reversed(self._iter_scan_roots(remote_root, allowed_days)))
        directories_scanned = 0
        files_seen = 0

        while stack:
            rel_prefix, current_dir = stack.pop()
            try:
                entries = sftp.listdir_attr(current_dir)
            except OSError as exc:
                if allowed_days and rel_prefix:
                    logger.info("Skipping unavailable PBX day directory: %s (%s)", current_dir, exc)
                    continue
                raise

            directories_scanned += 1
            if directories_scanned % 100 == 0:
                logger.info(
                    "PBX scan progress: directories=%d files_seen=%d current_dir=%s",
                    directories_scanned,
                    files_seen,
                    current_dir,
                )

            for entry in entries:
                rel_path = f"{rel_prefix}/{entry.filename}" if rel_prefix else entry.filename
                remote_path = f"{current_dir}/{entry.filename}"
                mode = entry.st_mode if entry.st_mode is not None else 0
                if stat.S_ISDIR(mode):
                    stack.append((rel_path, remote_path))
                else:
                    files_seen += 1
                    if files_seen % 1000 == 0:
                        logger.info(
                            "PBX scan progress: directories=%d files_seen=%d latest_file=%s",
                            directories_scanned,
                            files_seen,
                            rel_path,
                        )
                    yield rel_path

    def _extract_day_key(self, rel_path: str) -> str | None:
        parts = Path(rel_path).parts
        if len(parts) < 4:
            return None
        y, m, d = parts[0], parts[1], parts[2]
        if len(y) == 4 and y.isdigit() and len(m) == 2 and m.isdigit() and len(d) == 2 and d.isdigit():
            return f"{y}/{m}/{d}"
        return None

    def download_new(
        self,
        local_dir: Path,
        extensions: tuple = (".wav", ".mp3"),
        on_download: Optional[Callable[[str], None]] = None,
        allowed_days: Optional[set[str]] = None,
    ) -> List[Path]:
        """
        Download files not already present in local_dir.
        Returns list of newly downloaded paths.
        """
        assert self._client, "Call connect() first"
        local_dir.mkdir(parents=True, exist_ok=True)

        downloaded: List[Path] = []
        scanned = 0
        matched_extension = 0
        skipped_existing = 0
        skipped_day_filter = 0
        logger.info(
            "Scanning remote PBX recordings: remote_dir=%s local_dir=%s allowed_days=%s",
            self.remote_dir,
            local_dir,
            sorted(allowed_days) if allowed_days else "all",
        )
        with self._client.open_sftp() as sftp:
            for rel_path in self._iter_remote_files(sftp, self.remote_dir, allowed_days=allowed_days):
                scanned += 1
                if allowed_days:
                    day_key = self._extract_day_key(rel_path)
                    if day_key is None or day_key not in allowed_days:
                        skipped_day_filter += 1
                        continue

                name = Path(rel_path).name
                if not any(name.lower().endswith(ext.lower()) for ext in extensions):
                    continue
                matched_extension += 1

                local_path = local_dir / rel_path
                if local_path.exists():
                    skipped_existing += 1
                    continue

                local_path.parent.mkdir(parents=True, exist_ok=True)
                remote_path = f"{self.remote_dir.rstrip('/')}/{rel_path}"
                logger.info("Downloading PBX recording: %s", rel_path)
                sftp.get(remote_path, str(local_path))
                downloaded.append(local_path)
                if on_download:
                    on_download(rel_path)

        logger.info(
            "PBX scan complete: scanned=%d matched=%d downloaded=%d skipped_existing=%d skipped_day_filter=%d",
            scanned,
            matched_extension,
            len(downloaded),
            skipped_existing,
            skipped_day_filter,
        )
        return downloaded
