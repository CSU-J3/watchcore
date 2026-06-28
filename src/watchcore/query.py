"""Read-side query helpers over the Parquet observation store.

Reusable core: every watch serves its dashboard by querying the same store the
same way — reverse-chronological on ``observed_at``, optionally narrowed to one
or more ``obs_type`` values and a date range, and collapsed so each near-dup
cluster appears once. The view *shapes* (JSON field names, file layout) belong to
the consuming watch; these helpers only return store rows. So this lives in
watchcore, next to the store and dedup stages it reads — not in any one watch.

Time axis: ordering and date-range filtering are always on ``observed_at`` (when
the event was observed), never ``fetched_at`` (when the collector happened to
run). Callers cannot opt out of that — it is the contract the dashboards chart
on.

Cluster collapse needs no re-grading here: the dedup stage already set every
clustered row's ``cluster_id`` to its highest-grade member's ``obs_id`` (and left
singletons NULL), so keeping ``cluster_id IS NULL OR cluster_id = obs_id`` yields
exactly one representative per cluster plus every singleton.
"""
from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from watchcore.store import ParquetStore


def _naive_utc(dt: datetime) -> datetime:
    """Match the store's naive-UTC TIMESTAMP encoding so comparisons line up."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def select_observations(
    store: ParquetStore,
    *,
    obs_types: list[str] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    collapse_clusters: bool = True,
    reverse_chron: bool = True,
    limit: int | None = None,
) -> list[dict]:
    """Query the store and return matching rows as a list of dicts.

    ``obs_types`` filters on ``obs_type``; ``since``/``until`` bound
    ``observed_at`` (``since`` inclusive, ``until`` exclusive);
    ``collapse_clusters`` keeps one representative per near-dup cluster;
    ``reverse_chron`` orders newest ``observed_at`` first; ``limit`` caps the row
    count. All filter values are bound as parameters — only static column names
    are formatted into the SQL.
    """
    if not store.partition_dates():
        return []

    where: list[str] = []
    params: list[object] = []
    if obs_types:
        placeholders = ", ".join("?" for _ in obs_types)
        where.append(f"obs_type IN ({placeholders})")
        params.extend(obs_types)
    if since is not None:
        where.append("observed_at >= ?")
        params.append(_naive_utc(since))
    if until is not None:
        where.append("observed_at < ?")
        params.append(_naive_utc(until))
    if collapse_clusters:
        where.append("(cluster_id IS NULL OR cluster_id = obs_id)")

    sql = (
        f"SELECT * FROM read_parquet('{store.dataset_glob()}', "  # noqa: S608 — static glob + bound params
        f"hive_partitioning = true)"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY observed_at DESC, obs_id" if reverse_chron else " ORDER BY observed_at, obs_id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    con = duckdb.connect()
    try:
        rel = con.execute(sql, params)
        names = [d[0] for d in rel.description]
        return [dict(zip(names, r)) for r in rel.fetchall()]
    finally:
        con.close()
