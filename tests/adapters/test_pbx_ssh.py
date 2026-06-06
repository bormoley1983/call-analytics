import stat
from types import SimpleNamespace

from adapters.pbx_ssh import PbxSshDownloader


class FakeSftp:
    def __init__(self, entries_by_dir):
        self.entries_by_dir = entries_by_dir
        self.calls = []

    def listdir_attr(self, path):
        self.calls.append(path)
        if path not in self.entries_by_dir:
            raise FileNotFoundError(path)
        return self.entries_by_dir[path]


def _dir(name: str):
    return SimpleNamespace(filename=name, st_mode=stat.S_IFDIR)


def _file(name: str):
    return SimpleNamespace(filename=name, st_mode=stat.S_IFREG)


def test_iter_remote_files_scans_only_requested_day_directories():
    sftp = FakeSftp(
        {
            '/monitor/2026/06/06': [_dir('nested'), _file('call.wav')],
            '/monitor/2026/06/06/nested': [_file('call-2.wav')],
        }
    )
    downloader = PbxSshDownloader(host='pbx', remote_dir='/monitor')

    files = list(downloader._iter_remote_files(sftp, '/monitor', allowed_days={'2026/06/06'}))

    assert files == ['2026/06/06/call.wav', '2026/06/06/nested/call-2.wav']
    assert sftp.calls == ['/monitor/2026/06/06', '/monitor/2026/06/06/nested']


def test_iter_remote_files_skips_missing_requested_day_directory():
    sftp = FakeSftp({})
    downloader = PbxSshDownloader(host='pbx', remote_dir='/monitor')

    files = list(downloader._iter_remote_files(sftp, '/monitor', allowed_days={'2026/06/06'}))

    assert files == []
    assert sftp.calls == ['/monitor/2026/06/06']
