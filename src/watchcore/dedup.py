"""Two-stage near-duplicate clustering.

Stage one (exact): records with equal ``content_hash`` are the same item and
share a ``cluster_id`` directly. Stage two (fuzzy): a MinHash signature over
3-word shingles of the normalized ``title`` + ``summary`` groups records whose
Jaccard similarity is ``>= threshold`` (default 0.7, the settled value), via
``datasketch`` MinHashLSH. The two stages are unioned; each multi-member group
gets a ``cluster_id`` (the highest-grade member's ``obs_id`` represents it).

Reusable core — every watch dedups the same way — so it lives in watchcore.

Scope note (measured, not assumed): lexical similarity clusters exact
re-surfacing and **verbatim / near-verbatim** cross-outlet republication. It
does NOT cluster the same event **reworded** by different outlets — their
headlines are not lexically similar at any sane threshold (the closest
cross-outlet pair measured ~0.58). Clustering reworded cross-outlet reporting
needs a semantic method (sentence-embedding cosine) and is deferred to the
detect stage, where cross-outlet corroboration should drive the grade bump.
"""
from __future__ import annotations

from watchcore.observation import Observation

DEFAULT_THRESHOLD = 0.7
DEFAULT_NUM_PERM = 128
SHINGLE_K = 3

_REL_RANK = {r: i for i, r in enumerate("ABCDEF")}


def grade_key(reliability: str, credibility: int) -> tuple[int, int]:
    """Sort key for picking a cluster's highest-grade representative. Lower is
    better (Admiralty A1 is best): reliability A..F then credibility 1..6."""
    return (_REL_RANK.get(reliability, len(_REL_RANK)), int(credibility))


def _shingles(title: str, summary: str | None) -> set[str]:
    """3-word shingles of the normalized title+summary (same normalization the
    content_hash uses, so the two stages agree on text)."""
    words = Observation._norm(f"{title} {summary or ''}").split()
    if len(words) < SHINGLE_K:
        return set(words)
    return {" ".join(words[i : i + SHINGLE_K]) for i in range(len(words) - SHINGLE_K + 1)}


class _UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def assign_clusters(
    store, threshold: float = DEFAULT_THRESHOLD, num_perm: int = DEFAULT_NUM_PERM
) -> dict:
    """Cluster the whole stored corpus and write ``cluster_id`` back. Multi-member
    groups get the highest-grade member's obs_id as cluster_id; singletons get
    None. Returns summary stats."""
    from datasketch import MinHash, MinHashLSH

    rows = store.scan(
        ["obs_id", "content_hash", "title", "summary", "reliability", "credibility"]
    )
    n = len(rows)
    if n == 0:
        return {"rows": 0, "clusters": 0, "clustered_rows": 0, "threshold": threshold}

    index = {r["obs_id"]: i for i, r in enumerate(rows)}
    uf = _UnionFind(n)

    # stage one — exact content_hash
    by_hash: dict[str, list[int]] = {}
    for i, r in enumerate(rows):
        if r["content_hash"]:
            by_hash.setdefault(r["content_hash"], []).append(i)
    for members in by_hash.values():
        for k in range(1, len(members)):
            uf.union(members[0], members[k])

    # stage two — MinHash LSH over title+summary shingles
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    sigs: dict[int, MinHash] = {}
    for i, r in enumerate(rows):
        shingles = _shingles(r["title"], r["summary"])
        if not shingles:
            continue
        m = MinHash(num_perm=num_perm)
        for s in shingles:
            m.update(s.encode("utf-8"))
        sigs[i] = m
        lsh.insert(r["obs_id"], m)
    for i, m in sigs.items():
        for oid in lsh.query(m):
            j = index[oid]
            if j != i:
                uf.union(i, j)

    # form groups; assign cluster_id (highest-grade member's obs_id) for size > 1
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(uf.find(i), []).append(i)

    assignments: dict[str, str | None] = {}
    clusters = clustered_rows = 0
    for members in groups.values():
        if len(members) == 1:
            assignments[rows[members[0]]["obs_id"]] = None
            continue
        clusters += 1
        clustered_rows += len(members)
        rep = min(
            members,
            key=lambda i: (
                grade_key(rows[i]["reliability"], rows[i]["credibility"]),
                rows[i]["obs_id"],
            ),
        )
        cid = rows[rep]["obs_id"]
        for i in members:
            assignments[rows[i]["obs_id"]] = cid

    store.update_clusters(assignments)
    return {
        "rows": n,
        "clusters": clusters,
        "clustered_rows": clustered_rows,
        "threshold": threshold,
    }
