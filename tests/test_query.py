from datetime import datetime, timezone

from watchcore import Observation, Source
from watchcore.dedup import assign_clusters
from watchcore.query import select_observations
from watchcore.store import ParquetStore


def _obs(obs_id, *, observed_at, obs_type="naval", title="t", summary=None,
         reliability="C", credibility=3):
    return Observation(
        obs_id=obs_id,
        source=Source(collector="news_rss", source_type="rss", native_id=obs_id),
        fetched_at=datetime(2026, 6, 20, tzinfo=timezone.utc),
        observed_at=observed_at,
        obs_type=obs_type,
        title=title,
        summary=summary,
        reliability=reliability,
        credibility=credibility,
        raw={},
    )


def _store(tmp_path, observations):
    store = ParquetStore(tmp_path / "store")
    store.ingest(observations)
    return store


def test_empty_store_returns_empty(tmp_path):
    assert select_observations(ParquetStore(tmp_path / "nope")) == []


def test_reverse_chron_on_observed_at(tmp_path):
    d = lambda day: datetime(2026, 6, day, tzinfo=timezone.utc)  # noqa: E731
    store = _store(tmp_path, [
        _obs("a", observed_at=d(1)),
        _obs("b", observed_at=d(3)),
        _obs("c", observed_at=d(2)),
    ])
    rows = select_observations(store)
    assert [r["obs_id"] for r in rows] == ["b", "c", "a"]
    # forward order is the exact reverse
    fwd = select_observations(store, reverse_chron=False)
    assert [r["obs_id"] for r in fwd] == ["a", "c", "b"]


def test_obs_type_filter(tmp_path):
    d = datetime(2026, 6, 1, tzinfo=timezone.utc)
    store = _store(tmp_path, [
        _obs("a", observed_at=d, obs_type="naval"),
        _obs("b", observed_at=d, obs_type="air"),
        _obs("c", observed_at=d, obs_type="ground"),
    ])
    rows = select_observations(store, obs_types=["naval", "air"])
    assert {r["obs_id"] for r in rows} == {"a", "b"}


def test_date_range_is_on_observed_at(tmp_path):
    store = _store(tmp_path, [
        _obs("a", observed_at=datetime(2026, 6, 1, tzinfo=timezone.utc)),
        _obs("b", observed_at=datetime(2026, 6, 5, tzinfo=timezone.utc)),
        _obs("c", observed_at=datetime(2026, 6, 9, tzinfo=timezone.utc)),
    ])
    rows = select_observations(
        store,
        since=datetime(2026, 6, 5, tzinfo=timezone.utc),
        until=datetime(2026, 6, 9, tzinfo=timezone.utc),  # exclusive
    )
    assert [r["obs_id"] for r in rows] == ["b"]


def test_limit(tmp_path):
    d = lambda day: datetime(2026, 6, day, tzinfo=timezone.utc)  # noqa: E731
    store = _store(tmp_path, [_obs(f"o{i}", observed_at=d(i)) for i in range(1, 6)])
    rows = select_observations(store, limit=2)
    assert [r["obs_id"] for r in rows] == ["o5", "o4"]


def test_collapse_keeps_one_representative_per_cluster(tmp_path):
    # Two verbatim-identical titles -> one cluster; the higher-grade member (B2)
    # is the representative and is the only one kept when collapsed.
    d = datetime(2026, 6, 1, tzinfo=timezone.utc)
    store = _store(tmp_path, [
        _obs("low", observed_at=d, title="Carrier enters Strait of Hormuz",
             reliability="C", credibility=3),
        _obs("high", observed_at=d, title="Carrier enters Strait of Hormuz",
             reliability="B", credibility=2),
        _obs("solo", observed_at=d, title="Unrelated logistics convoy note"),
    ])
    assign_clusters(store)

    collapsed = select_observations(store, collapse_clusters=True)
    ids = {r["obs_id"] for r in collapsed}
    assert ids == {"high", "solo"}  # cluster collapses to its B2 representative

    full = select_observations(store, collapse_clusters=False)
    assert {r["obs_id"] for r in full} == {"low", "high", "solo"}
