# src/adapters/pbx_ssh.py
from __future__ import annotations

import stat
from pathlib import Path
from typing import Callable, List, Optional

import paramiko


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

    def close(self) -> None:
        if self._client:
            self._client.close()

    def _iter_remote_files(self, sftp: paramiko.SFTPClient, remote_root: str) -> List[str]:
        """
        Recursively collect file paths under remote_root as POSIX-relative paths.
        """
        root = remote_root.rstrip("/")
        stack: List[tuple[str, str]] = [("", root)]
        files: List[str] = []

        while stack:
            rel_prefix, current_dir = stack.pop()
            for entry in sftp.listdir_attr(current_dir):
                rel_path = f"{rel_prefix}/{entry.filename}" if rel_prefix else entry.filename
                remote_path = f"{current_dir}/{entry.filename}"
                mode = entry.st_mode if entry.st_mode is not None else 0
                if stat.S_ISDIR(mode):
                    stack.append((rel_path, remote_path))
                else:
                    files.append(rel_path)

        return files

    def download_new(
        self,
        local_dir: Path,
        extensions: tuple = (".wav", ".mp3"),
        on_download: Optional[Callable[[str], None]] = None,
    ) -> List[Path]:
        """
        Download files not already present in local_dir.
        Returns list of newly downloaded paths.
        """
        assert self._client, "Call connect() first"
        local_dir.mkdir(parents=True, exist_ok=True)

        downloaded: List[Path] = []
        with self._client.open_sftp() as sftp:
            for rel_path in self._iter_remote_files(sftp, self.remote_dir):
                name = Path(rel_path).name
                if not any(name.lower().endswith(ext.lower()) for ext in extensions):
                    continue

                local_path = local_dir / rel_path
                if local_path.exists():
                    continue

                local_path.parent.mkdir(parents=True, exist_ok=True)
                remote_path = f"{self.remote_dir.rstrip('/')}/{rel_path}"
                sftp.get(remote_path, str(local_path))
                downloaded.append(local_path)
                if on_download:
                    on_download(rel_path)

        return downloaded