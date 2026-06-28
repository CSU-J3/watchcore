# watchcore

The shared core under your OSINT watch projects. It defines the `Observation`
record that every collector emits and every pipeline stage consumes, so each
watch reuses one ingest engine instead of rebuilding it.

This repo is not a watch. It has no collectors and no dashboard. Those live in
the individual watch repos (Burgerreich-watch, Fleet-watch, and so on), which
import this package. Think of it as the engine; the watches are the cars.

## Install

From another project, depend on it straight from the repo:

```
pip install git+https://github.com/CSU-J3/watchcore
```

For the near-dup stage, install the extra:

```
pip install "watchcore[dedup] @ git+https://github.com/CSU-J3/watchcore"
```

## The contract

`Observation` is documented in [docs/observation-schema.md](docs/observation-schema.md):
the fields, who sets each one, the Admiralty grade as data, the storage mapping
for DuckDB/Parquet and SQLite, and the collector contract. Read that before
writing a collector.

Quick shape:

```python
from datetime import datetime, timezone
from watchcore import Observation, Source

t = datetime(2026, 6, 17, 9, 30, tzinfo=timezone.utc)
obs = Observation(
    obs_id=Observation.make_id("dvids", t, native_id="centcom-2026-0617-a"),
    source=Source(collector="dvids", source_type="rss", publisher="CENTCOM"),
    fetched_at=datetime.now(timezone.utc),
    observed_at=t,
    obs_type="naval",
    title="Carrier strike group transits Strait of Hormuz",
    reliability="B",
    credibility=2,
    raw={"guid": "centcom-2026-0617-a"},
)
print(obs.grade_code())  # B2
```

## Status

Schema version 1. The `Observation` model is implemented. The pipeline stages
(normalize, dedup, store, grade, detect, serve) are next, built behind this same
record.

## Renaming the package

If `watchcore` is a placeholder, change three things: the directory
`src/watchcore`, the `name` in `pyproject.toml` plus the
`[tool.hatch.*]` paths, and the imports in `src/watchcore/__init__.py` and the
tests.
