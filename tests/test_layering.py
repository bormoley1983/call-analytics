"""Guardrail tests for the hexagonal layering rules.

Uses ``ast`` to parse every module under ``src/`` and asserts:

- ``domain/*`` and ``ports/*`` import only stdlib / third-party packages;
- ``core/*`` imports only ``domain``, ``ports``, stdlib, third-party
  (no ``adapters``, no ``api``);
- ``adapters/*`` imports only ``domain``, ``ports``, stdlib, third-party
  (no ``core``, no ``api``).

Also enforces env-var hygiene: ``os.getenv`` / ``os.environ`` reads are
allowed only in the sanctioned composition-root modules.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

# Top-level packages that make up the application (everything else is
# stdlib or third-party).
APP_PACKAGES = {"domain", "ports", "core", "adapters", "api"}

# Modules allowed to read environment variables directly (composition roots
# and the config layer). Everything else must go through domain.config.
ENV_ALLOWED_MODULES = {
    "domain/config.py",
    "domain/envutil.py",
    "api/app.py",
    "api/runner.py",
    "cli.py",
    "logging_config.py",
    "migrate_storage.py",
    "stt_compare.py",
    "stt_replay.py",
}


def _python_files() -> list[Path]:
    return sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)


def _module_relpath(path: Path) -> str:
    return path.relative_to(SRC).as_posix()


def _top_level_package(relpath: str) -> str | None:
    head = relpath.split("/", 1)[0]
    return head if head in APP_PACKAGES else None


def _imported_top_packages(tree: ast.AST) -> set[str]:
    """Return the set of app top-level packages imported by a module."""
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                pkg = alias.name.split(".", 1)[0]
                if pkg in APP_PACKAGES:
                    found.add(pkg)
        elif isinstance(node, ast.ImportFrom):
            # Relative imports (level > 0) resolve within the same package.
            if node.level == 0 and node.module:
                pkg = node.module.split(".", 1)[0]
                if pkg in APP_PACKAGES:
                    found.add(pkg)
    return found


def _env_reads(tree: ast.AST) -> list[int]:
    """Return line numbers of direct env access via os or imported aliases."""
    lines: list[int] = []
    imported_getenv_names: set[str] = set()
    imported_environ_names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "os":
            for alias in node.names:
                if alias.name == "getenv":
                    imported_getenv_names.add(alias.asname or alias.name)
                elif alias.name == "environ":
                    imported_environ_names.add(alias.asname or alias.name)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in imported_getenv_names:
                lines.append(node.lineno)
                continue
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                if func.value.id in imported_environ_names and func.attr == "get":
                    lines.append(node.lineno)

        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id == "os" and node.attr in {"getenv", "environ"}:
                lines.append(node.lineno)
            elif node.value.id in imported_environ_names and node.attr == "get":
                lines.append(node.lineno)

    return sorted(set(lines))


def test_layering_domain_and_ports_import_nothing_internal() -> None:
    """domain may only import domain (self); ports may import domain.

    Neither may import core, adapters, or api.
    """
    violations: list[str] = []
    for path in _python_files():
        rel = _module_relpath(path)
        pkg = _top_level_package(rel)
        if pkg not in {"domain", "ports"}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported = _imported_top_packages(tree)
        # domain may only self-import; ports may import domain and itself.
        allowed = {"domain"} if pkg == "domain" else {"domain", "ports"}
        bad = imported - allowed
        if bad:
            violations.append(f"{rel}: imports {sorted(bad)}")
    assert not violations, (
        "domain/ports must not import core/adapters/api:\n" + "\n".join(violations)
    )


def test_ports_self_imports_are_allowed() -> None:
    tree = ast.parse(
        "from ports.foo import Example\nfrom ports.bar import Thing\n",
        filename="ports/example.py",
    )
    imported = _imported_top_packages(tree)
    assert imported == {"ports"}


def test_env_reads_detects_imported_getenv_aliases() -> None:
    tree = ast.parse(
        "from os import getenv as g\nfrom os import environ as env\n"
        "value = g('A')\nother = env.get('B')\n",
        filename="example.py",
    )
    assert _env_reads(tree) == [3, 4]


def test_layering_core_imports_no_adapters_or_api() -> None:
    violations: list[str] = []
    for path in _python_files():
        rel = _module_relpath(path)
        pkg = _top_level_package(rel)
        if pkg != "core":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported = _imported_top_packages(tree)
        bad = imported & {"adapters", "api"}
        if bad:
            violations.append(f"{rel}: imports {sorted(bad)}")
    assert not violations, "core must not import adapters/api:\n" + "\n".join(violations)


def test_layering_adapters_import_no_core_or_api() -> None:
    violations: list[str] = []
    for path in _python_files():
        rel = _module_relpath(path)
        pkg = _top_level_package(rel)
        if pkg != "adapters":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported = _imported_top_packages(tree)
        bad = imported & {"core", "api"}
        if bad:
            violations.append(f"{rel}: imports {sorted(bad)}")
    assert not violations, "adapters must not import core/api:\n" + "\n".join(violations)


def test_env_reads_only_in_sanctioned_modules() -> None:
    violations: list[str] = []
    for path in _python_files():
        rel = _module_relpath(path)
        if rel in ENV_ALLOWED_MODULES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        lines = _env_reads(tree)
        if lines:
            violations.append(f"{rel}: os.getenv/os.environ at lines {lines}")
    assert not violations, (
        "os.getenv/os.environ only allowed in sanctioned modules:\n"
        + "\n".join(violations)
    )
