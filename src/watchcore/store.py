"""Parquet observation store, queried by DuckDB.

The persisted store is date-partitioned Parquet — one fixed-name file per
``observed_at`` date (``observed_at_date=YYYY-MM-DD/data.parquet``). DuckDB reads
it directly; there is no separate database file to commit. This is the first
real pipeline stage in watchcore beyond the Observation model, so every watch
reuses it.

Two properties this module is built to guarantee:

* **Upsert on obs_id (stage-one exact dedup).** Re-ingesting the same item
  updates ``last_seen`` and never duplicates a row. ``first_seen`` and any
  ``cluster_id`` already assigned are preserved across re-ingest.
* **Partition isolation (cheap commits).** An ingest reads, merges, and rewrites
  only the date partitions its batch touches. Untouched partitions are never
  rewritten, so a 6-hour run that adds mostly-new dates produces a minimal git
  diff instead of churning one growing blob.
"""
from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from watchcore.observation import SCHEMA_VERSION, Observation

# (name, duckdb type, is_json) in stored column order — the schema spec's
# storage mapping.
_COLUMNS: list[tuple[str, str, bool]] = [
    ("schema_version", "INTEGER", False),
    ("obs_id", "VARCHAR", False),
    ("source", "JSON", True),
    ("fetched_at", "TIMESTAMP", False),
    ("observed_at", "TIMESTAMP", False),
    ("obs_type", "VARCHAR", False),
    ("title", "VARCHAR", False),
    ("reliability", "VARCHAR", False),
    ("credibility", "TINYINT", False),
    ("raw", "JSON", True),
    ("summary", "VARCHAR", False),
    ("entities", "JSON", True),
    ("geo", "JSON", True),
    ("tags", "JSON", True),
    ("valid_from", "TIMESTAMP", False),
    ("valid_to", "TIMESTAMP", False),
    ("confidence", "VARCHAR", False),
    ("content_hash", "VARCHAR", False),
    ("cluster_id", "VARCHAR", False),
    ("first_seen", "TIMESTAMP", False),
    ("last_seen", "TIMESTAMP", False),
    ("supersedes", "VARCHAR", False),
]


def _utc_naive(dt: datetime | None) -> datetime | None:
    """Store timestamps as naive-UTC TIMESTAMP (the schema spec's mapping), so a
    consistent UTC instant lands in Parquet without a pytz dependency."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt is not None else None
_NAMES = [c[0] for c in _COLUMNS]
_DDL = ", ".join(f'"{n}" {t}' for n, t, _ in _COLUMNS)
_INSERT_PLACEHOLDERS = ", ".join("?::JSON" if is_json else "?" for _, _, is_json in _COLUMNS)


class ParquetStore:
    """A date-partitioned Parquet store rooted at ``root``."""

    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)

    # -- paths ---------------------------------------------------------------
    def _partition_file(self, date_str: str) -> Path:
        return self.root / f"observed_at_date={date_str}" / "data.parquet"

    def _glob(self) -> str:
        return (self.root / "observed_at_date=*" / "data.parquet").as_posix()

    def dataset_glob(self) -> str:
        """Public glob over every partition file. The read-side query helpers in
        ``watchcore.query`` use this so they can push filters into DuckDB without
        reaching into a private."""
        return self._glob()

    def partition_dates(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(
            p.name.split("=", 1)[1]
            for p in self.root.glob("observed_at_date=*")
            if (p / "data.parquet").exists()
        )

    # -- row construction ----------------------------------------------------
    @staticmethod
    def _row(obs: Observation, now: datetime) -> tuple:
        """An Observation as a tuple in stored column order. cluster_id is NULL
        and first/last_seen are `now`; the merge preserves existing values."""
        geo = obs.geo.model_dump_json() if obs.geo else None
        return (
            SCHEMA_VERSION,
            obs.obs_id,
            obs.source.model_dump_json(),
            _utc_naive(obs.fetched_at),
            _utc_naive(obs.observed_at),
            obs.obs_type.value,
            obs.title,
            obs.reliability.value,
            int(obs.credibility),
            json.dumps(obs.raw),
            obs.summary,
            json.dumps([e.model_dump(mode="json") for e in obs.entities]),
            geo,
            json.dumps(obs.tags),
            _utc_naive(obs.valid_from),
            _utc_naive(obs.valid_to),
            obs.confidence.value if obs.confidence else None,
            Observation.make_content_hash(obs.title, obs.summary),
            None,  # cluster_id — set by the dedup stage
            _utc_naive(now),  # first_seen (kept only when the row is new)
            _utc_naive(now),  # last_seen (always updated)
            obs.supersedes,
        )

    # -- ingest --------------------------------------------------------------
    def ingest(
        self, observations: Iterable[Observation], now: datetime | None = None
    ) -> dict:
        """Upsert observations keyed on obs_id. Returns which partitions were
        written and how many rows came in."""
        now = now or datetime.now(timezone.utc)
        # Stage-one exact dedup within the batch: the same obs_id collapses to a
        # single row (e.g. one article surfaced by several feed queries). Last
        # occurrence wins. This also keeps obs_id unique per partition, so the
        # upsert merge's join never fans out.
        total = len(observations) if hasattr(observations, "__len__") else None
        unique: dict[str, Observation] = {obs.obs_id: obs for obs in observations}

        by_date: dict[str, list[tuple]] = {}
        for obs in unique.values():
            date_str = obs.observed_at.astimezone(timezone.utc).date().isoformat()
            by_date.setdefault(date_str, []).append(self._row(obs, now))

        for date_str, rows in by_date.items():
            self._upsert_partition(date_str, rows)
        return {
            "rows_in": total if total is not None else len(unique),
            "unique_obs_ids": len(unique),
            "partitions_written": sorted(by_date),
        }

    def _upsert_partition(self, date_str: str, rows: list[tuple]) -> None:
        pfile = self._partition_file(date_str)
        pfile.parent.mkdir(parents=True, exist_ok=True)
        tmp = pfile.with_suffix(".parquet.tmp")
        con = duckdb.connect()
        try:
            con.execute(f"CREATE TABLE incoming ({_DDL})")
            con.executemany(
                f'INSERT INTO incoming BY NAME '  # noqa: S608 — names are static
                f'SELECT * FROM (VALUES ({_INSERT_PLACEHOLDERS})) '
                f'AS t({", ".join(_NAMES)})',
                rows,
            )
            cols = ", ".join(f'"{n}"' for n in _NAMES)
            if pfile.exists():
                con.execute(
                    f"CREATE TABLE existing AS SELECT {cols} "
                    f"FROM read_parquet('{pfile.as_posix()}')"
                )
                merged = (
                    f"SELECT {cols} FROM existing "
                    f"WHERE obs_id NOT IN (SELECT obs_id FROM incoming) "
                    f"UNION ALL "
                    f"SELECT {self._merge_select()} "
                    f"FROM incoming i LEFT JOIN existing e USING (obs_id) "
                    f"ORDER BY obs_id"
                )
            else:
                merged = f"SELECT {cols} FROM incoming ORDER BY obs_id"
            con.execute(f"COPY ({merged}) TO '{tmp.as_posix()}' (FORMAT PARQUET)")
        finally:
            con.close()
        os.replace(tmp, pfile)

    @staticmethod
    def _merge_select() -> str:
        """Incoming-branch columns: preserve existing first_seen and cluster_id
        on an obs_id match; take the new value for everything else."""
        parts = []
        for n in _NAMES:
            if n in ("first_seen", "cluster_id"):
                parts.append(f'COALESCE(e."{n}", i."{n}") AS "{n}"')
            else:
                parts.append(f'i."{n}" AS "{n}"')
        return ", ".join(parts)

    # -- read ----------------------------------------------------------------
    def scan(self, columns: list[str] | None = None) -> list[dict]:
        """Read the whole store as a list of dict rows (with observed_at_date
        from the partition path). Enough for dedup and analysis; the richer
        query helpers are Phase 4."""
        if not self.partition_dates():
            return []
        sel = ", ".join(columns) if columns else "*"
        con = duckdb.connect()
        try:
            rel = con.execute(
                f"SELECT {sel} FROM read_parquet('{self._glob()}', "
                f"hive_partitioning = true)"
            )
            names = [d[0] for d in rel.description]
            return [dict(zip(names, r)) for r in rel.fetchall()]
        finally:
            con.close()

    def count(self) -> int:
        if not self.partition_dates():
            return 0
        con = duckdb.connect()
        try:
            return con.execute(
                f"SELECT count(*) FROM read_parquet('{self._glob()}')"
            ).fetchone()[0]
        finally:
            con.close()

    # -- cluster write-back (used by the dedup stage) ------------------------
    def update_clusters(self, assignments: Mapping[str, str | None]) -> dict:
        """Set cluster_id for the given obs_ids, rewriting only the partitions
        that actually change (keeps commits minimal on the dedup pass too)."""
        changed: list[str] = []
        for date_str in self.partition_dates():
            pfile = self._partition_file(date_str)
            con = duckdb.connect()
            try:
                rows = con.execute(
                    f"SELECT obs_id, cluster_id FROM read_parquet('{pfile.as_posix()}')"
                ).fetchall()
                if not any(
                    oid in assignments and assignments[oid] != cur
                    for oid, cur in rows
                ):
                    continue
                con.execute(
                    f"CREATE TABLE p AS SELECT * FROM read_parquet('{pfile.as_posix()}')"
                )
                # only this partition's obs_ids — keeps the update local
                con.executemany(
                    "UPDATE p SET cluster_id = ? WHERE obs_id = ?",
                    [(assignments[oid], oid) for oid, _ in rows if oid in assignments],
                )
                tmp = pfile.with_suffix(".parquet.tmp")
                con.execute(
                    f"COPY (SELECT * FROM p ORDER BY obs_id) "
                    f"TO '{tmp.as_posix()}' (FORMAT PARQUET)"
                )
            finally:
                con.close()
            os.replace(pfile.with_suffix(".parquet.tmp"), pfile)
            changed.append(date_str)
        return {"partitions_rewritten": changed}
