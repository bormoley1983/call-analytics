"""PBX connection settings as an explicit dataclass.

Previously the PBX host/port/user/key were read ad-hoc from ``os.environ`` in
two different places (``api/runner.py`` and ``cli.py``), with inconsistent
defaults — and ``runner._run_sync_once`` used ``os.environ["PBX_HOST"]`` which
raises ``KeyError`` when the variable is unset. Both call sites now build a
``PbxConfig`` via :func:`load_pbx_config`, which fails fast with a clear error
message instead of an opaque KeyError.
"""

from __future__ import annotations

from domain.config import PbxConfig, get_pbx_config

__all__ = ["PbxConfig", "load_pbx_config"]


def load_pbx_config() -> PbxConfig:
    """Build a :class:`PbxConfig` from environment variables.

    Env reads live in ``domain.config`` (sanctioned config layer); this is a
    thin alias kept for call-site compatibility.

    Raises:
        RuntimeError: if ``PBX_HOST`` is not set (fail fast, clear message).
    """
    return get_pbx_config()
