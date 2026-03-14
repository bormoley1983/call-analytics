# src/adapters/pbx_ssh.py
from __future__ import annotations

import os
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
        remote_dir: str = "/var/spool/asterisk/monitor",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_path = key_path
        self.remote_dir = remote_dir
        self._client: Optional[paramiko.SSHClient] = None

    def connect(self) -> None:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.RejectPolicy())  # secure default
        connect_kwargs = dict(hostname=self.host, port=self.port, username=self.username)
        if self.key_path:
            connect_kwargs["key_filename"] = self.key_path
        elif self.password:
            connect_kwargs["password"] = self.password
        self._client.connect(**connect_kwargs)

    def close(self) -> None:
        if self._client:
            self._client.close()

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
        existing = {p.name for p in local_dir.iterdir() if p.is_file()}

        downloaded: List[Path] = []
        with self._client.open_sftp() as sftp:
            for entry in sftp.listdir_attr(self.remote_dir):
                name = entry.filename
                if not any(name.lower().endswith(ext) for ext in extensions):
                    continue
                if name in existing:
                    continue
                remote_path = f"{self.remote_dir}/{name}"
                local_path = local_dir / name
                sftp.get(remote_path, str(local_path))
                downloaded.append(local_path)
                if on_download:
                    on_download(name)

        return downloaded