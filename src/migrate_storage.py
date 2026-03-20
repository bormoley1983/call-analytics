from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Literal

import psycopg2

from adapters.storage_postgres import DDL, PostgresStorage
from domain.config import ANALYSIS, OUT, TRANS
from logging_config import setup_logging

Entity = Literal["transcripts", "analyses"]


@dataclass
class Record:
    entity: Entity
    call_id: str
    data: Dict[str, Any]


def _read_json_file(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to read JSON file: {path}: {exc}") from exc


def _write_json_file(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


class JsonBackend:
    def __init__(self, out_dir: Path, transcripts_dir: Path, analyses_dir: Path):
        self.out_dir = out_dir
        self.transcripts_dir = transcripts_dir
        self.analyses_dir = analyses_dir

    def ensure_ready(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)
        self.analyses_dir.mkdir(parents=True, exist_ok=True)

    def iter_records(self, entity: Entity) -> Iterator[Record]:
        base = self.transcripts_dir if entity == "transcripts" else self.analyses_dir
        if not base.exists():
            return
        for path in sorted(base.glob("*.json")):
            call_id = path.stem
            data = _read_json_file(path)
            yield Record(entity=entity, call_id=call_id, data=data)

    def upsert(self, record: Record) -> None:
        base = self.transcripts_dir if record.entity == "transcripts" else self.analyses_dir
        base.mkdir(parents=True, exist_ok=True)
        out = base / f"{record.call_id}.json"
        _write_json_file(out, record.data)


class PostgresBackend:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pg = PostgresStorage(dsn)
        self._conn: psycopg2.extensions.connection | None = None

    def ensure_ready(self) -> None:
        self.pg.ensure_ready()
        # PostgresStorage already opened one connection; keep it for writes.
        # Open a dedicated read connection for iterating source records.
        self._conn = psycopg2.connect(self.dsn)
        with self._conn.cursor() as cur:
            cur.execute(DDL)
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
        self.pg.close()

    def iter_records(self, entity: Entity) -> Iterator[Record]:
        if not self._conn:
            raise RuntimeError("Postgres backend is not initialized")

        table = "transcripts" if entity == "transcripts" else "analyses"
        query = f"SELECT call_id, data FROM {table} ORDER BY call_id"
        with self._conn.cursor() as cur:
            cur.execute(query)
            for call_id, data in cur.fetchall():
                yield Record(entity=entity, call_id=call_id, data=data or {})

    def upsert(self, record: Record) -> None:
        if record.entity == "transcripts":
            self.pg.upsert_transcript(record.call_id, record.data)
        else:
            self.pg.upsert_analysis(record.call_id, record.data)


def parse_entities(raw: str) -> list[Entity]:
    value = raw.strip().lower()
    if value == "both":
        return ["transcripts", "analyses"]
    if value == "transcripts":
        return ["transcripts"]
    if value == "analyses":
        return ["analyses"]
    raise ValueError(f"Unsupported entity value: {raw}")


def build_backend(kind: str, args: argparse.Namespace):
    if kind == "json":
        out_dir = Path(args.json_out_dir).resolve()
        transcripts_dir = Path(args.json_transcripts_dir).resolve()
        analyses_dir = Path(args.json_analyses_dir).resolve()
        return JsonBackend(out_dir=out_dir, transcripts_dir=transcripts_dir, analyses_dir=analyses_dir)

    if kind == "postgres":
        dsn = args.postgres_dsn or os.getenv("POSTGRES_DSN")
        if not dsn:
            raise ValueError("Postgres DSN is required. Pass --postgres-dsn or set POSTGRES_DSN")
        return PostgresBackend(dsn=dsn)

    raise ValueError(f"Unsupported backend: {kind}")


def migrate_entities(
    source,
    target,
    entities: Iterable[Entity],
    dry_run: bool,
    stop_on_error: bool,
) -> Dict[str, Dict[str, int]]:
    stats: Dict[str, Dict[str, int]] = {}

    for entity in entities:
        copied = 0
        failed = 0
        seen = 0

        for rec in source.iter_records(entity):
            seen += 1
            try:
                if not dry_run:
                    target.upsert(rec)
                copied += 1
            except Exception:
                failed += 1
                if stop_on_error:
                    raise

        stats[entity] = {"seen": seen, "copied": copied, "failed": failed}

    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Universal storage migration tool. Supports json<->postgres migration.",
    )
    parser.add_argument("--source", choices=["json", "postgres"], required=True, help="Source backend")
    parser.add_argument("--target", choices=["json", "postgres"], required=True, help="Target backend")
    parser.add_argument(
        "--entities",
        default="both",
        choices=["transcripts", "analyses", "both"],
        help="Which datasets to migrate",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and count records but do not write to target",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop migration on first write/read error",
    )

    # JSON backend options
    parser.add_argument("--json-out-dir", default=str(OUT), help="JSON output root directory")
    parser.add_argument("--json-transcripts-dir", default=str(TRANS), help="JSON transcripts directory")
    parser.add_argument("--json-analyses-dir", default=str(ANALYSIS), help="JSON analyses directory")

    # Postgres backend options
    parser.add_argument("--postgres-dsn", default=None, help="Postgres DSN (or set POSTGRES_DSN)")
    return parser


def main() -> int:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()

    if args.source == args.target:
        parser.error("Source and target must be different")

    entities = parse_entities(args.entities)
    source = build_backend(args.source, args)
    target = build_backend(args.target, args)

    source.ensure_ready()
    target.ensure_ready()

    try:
        stats = migrate_entities(
            source=source,
            target=target,
            entities=entities,
            dry_run=args.dry_run,
            stop_on_error=args.stop_on_error,
        )
    finally:
        if hasattr(source, "close"):
            source.close()
        if hasattr(target, "close"):
            target.close()

    print("Migration completed")
    print(f"source={args.source} target={args.target} dry_run={args.dry_run}")
    for entity in entities:
        s = stats[entity]
        print(f"{entity}: seen={s['seen']} copied={s['copied']} failed={s['failed']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
