# Observation record — schema spec

The `Observation` is the contract at the center of the shared core. Every collector emits Observations. Every core stage (normalize, dedup, store, grade, detect, serve) reads and writes them. Get this shape right and the rest of the core is plumbing around it. Change this shape carelessly and every collector and every stored Parquet file breaks at once.

Package name `watchcore` below is a placeholder. Rename to whatever you call the core.

## The one rule

A collector's only job is to turn one source item into one valid `Observation`. It does not touch storage, grading logic, dedup, or the frontend. If a collector needs to know anything about how data is stored or served, the abstraction has leaked and you should pull that concern back into the core.

Within a major `schema_version`, changes are additive only. Adding an optional field is free. Renaming a field, removing one, or changing what a field means is a version bump.

## Record shape

Three columns matter when reading this table: who sets the field, and whether it is required. "Collector" means the collector must produce it. "Core" means a core stage fills it in during ingest. "Optional" means either side may set it and the record is valid without it.

| Field | Type | Set by | Required | Holds |
|---|---|---|---|---|
| `schema_version` | int | core | yes | Version of this shape. Currently `1`. |
| `obs_id` | str | core | yes | Deterministic id. Hash of `collector` + `native_id` (or `url`, or `title`) + `observed_at`. |
| `source` | Source | collector | yes | Where it came from. See sub-object below. |
| `fetched_at` | datetime (UTC) | collector | yes | When the collector retrieved it. |
| `observed_at` | datetime (UTC) | collector | yes | When the event happened or was published. The time-series axis. |
| `obs_type` | enum | collector | yes | Domain category. Controlled vocab, extensible. |
| `title` | str | collector | yes | Normalized short label. |
| `reliability` | enum A–F | collector | yes | Source reliability (Admiralty). |
| `credibility` | enum 1–6 | collector | yes | Information credibility (Admiralty). |
| `raw` | object | collector | yes | Unmodified source payload. Written once, never mutated. |
| `summary` | str | either | no | Normalized longer text. |
| `entities` | Entity[] | either | no | Typed extracted entities (vessels, units, places). |
| `geo` | Geo or null | either | no | Coordinates. Null when not geolocatable. |
| `tags` | str[] | either | no | Free-form labels for project-specific filtering. |
| `valid_from` | datetime or null | either | no | Start of a duration (deployment window, exercise). |
| `valid_to` | datetime or null | either | no | End of a duration. |
| `confidence` | enum or null | core | no | Derived analyst confidence (high/moderate/low). |
| `content_hash` | str or null | core | no | Hash of normalized content, for near-dup detection. |
| `cluster_id` | str or null | core | no | Groups near-duplicate observations of one event. |
| `first_seen` | datetime or null | core | no | When the pipeline first saw this (or its cluster). |
| `last_seen` | datetime or null | core | no | When the pipeline last saw this (or its cluster). |
| `supersedes` | str or null | either | no | `obs_id` of a prior observation this one updates. |

### Why `fetched_at` and `observed_at` are separate

This is the fix for the thing Burgerreich got wrong. `fetched_at` is ingest time, `observed_at` is real-world time. A press release published Monday and collected Wednesday has two different timestamps, and only `observed_at` belongs on a timeline. Conflating them means you can never honestly chart when things happened, only when you happened to scrape them.

### Why `obs_id` is a hash, not a UUID

`obs_id` is a deterministic hash of `(collector, native_id, observed_at)`. Re-ingesting the same RSS item produces the same id, so the store upserts instead of duplicating. That is your stage-one (exact) dedup, for free, with no separate logic. A random UUID per run would reinsert the same item every six hours.

`content_hash` and `cluster_id` are the separate concern: the same real-world event reported by two outlets with different wording. `obs_id` will differ (different source), but `content_hash` lets the dedup stage detect the overlap and `cluster_id` groups them. That split maps directly onto the two-stage dedup you already built for Follow-the-Money: exact match first, near match second.

## The grade (Admiralty as data)

Grading happens at ingest, not as an afterthought. Every collector sets a baseline `reliability` and `credibility` from its config (an official `.mil` feed is not the same trust level as a scraped aggregator), and a later stage can revise upward when a second source corroborates.

`reliability` (source reliability):

| Code | Meaning |
|---|---|
| A | Completely reliable |
| B | Usually reliable |
| C | Fairly reliable |
| D | Not usually reliable |
| E | Unreliable |
| F | Cannot be judged |

`credibility` (information credibility):

| Code | Meaning |
|---|---|
| 1 | Confirmed by other sources |
| 2 | Probably true |
| 3 | Possibly true |
| 4 | Doubtful |
| 5 | Improbable |
| 6 | Cannot be judged |

Store the two components separately so you can query and filter on them (`reliability <= 'B'`). The combined code (`B2`) is a display concern, derived with a helper, not a stored field.

`confidence` is the ICA-style scale (high, moderate, low) from your skill. It is optional and applies to derived assessments the detect stage produces, not to raw observations. Leave it null at ingest.

## Sub-objects

`Source`:

| Field | Type | Required | Holds |
|---|---|---|---|
| `collector` | str | yes | Collector id that produced this (`dvids`, `ais_aisstream`, `news_googlerss`). |
| `source_type` | enum | yes | `rss`, `api`, `ais`, `scrape`, `feed`, `manual`. |
| `publisher` | str or null | no | Upstream outlet (`CENTCOM`, `USNI News`). |
| `url` | str or null | no | Canonical link to the item. |
| `native_id` | str or null | no | Source's own id (RSS guid, API id, MMSI+timestamp). |

`Geo`:

| Field | Type | Required | Holds |
|---|---|---|---|
| `lat` | float | yes | Latitude. |
| `lon` | float | yes | Longitude. |
| `precision_m` | float or null | no | Radius of uncertainty in meters. |
| `place_name` | str or null | no | Human-readable location. |
| `geohash` | str or null | no | Geohash, if you index spatially. |

`Entity`:

| Field | Type | Required | Holds |
|---|---|---|---|
| `type` | enum | yes | `unit`, `vessel`, `aircraft`, `person`, `org`, `location`, `platform`, `other`. |
| `name` | str | yes | Display name. |
| `value` | str or null | no | Normalized id (MMSI, ICAO hex). |
| `role` | str or null | no | Role in this observation. |

## Invariants

These hold for every record, always. They are the things that make the store trustworthy.

1. `obs_id` is deterministic and stable. The same source item always hashes to the same id. Never random per run.
2. `raw` is written once and never mutated. Re-normalization rebuilds derived fields from `raw`; it does not touch `raw` itself.
3. `fetched_at` and `observed_at` are both UTC and timezone-aware. In the normal case `fetched_at >= observed_at`. A violation is flagged, not silently stored.
4. Every record carries `schema_version`. Readers tolerate older versions rather than crashing on them.
5. `reliability` and `credibility` are always present. An ungraded collector sets its configured baseline. Grading is part of ingest.
6. Changes within a major version are additive only.

## Storage mapping

### DuckDB / Parquet (the Blockade-tracker store)

One row per observation. Scalar fields map to columns. `source`, `geo`, `entities`, and `raw` are JSON.

| Column | Type |
|---|---|
| `schema_version` | INTEGER |
| `obs_id` | VARCHAR (primary key, upsert target) |
| `source` | JSON |
| `fetched_at` | TIMESTAMP |
| `observed_at` | TIMESTAMP |
| `obs_type` | VARCHAR |
| `title` | VARCHAR |
| `reliability` | VARCHAR |
| `credibility` | TINYINT |
| `raw` | JSON |
| `summary` | VARCHAR |
| `entities` | JSON |
| `geo` | JSON |
| `tags` | JSON |
| `valid_from` | TIMESTAMP |
| `valid_to` | TIMESTAMP |
| `confidence` | VARCHAR |
| `content_hash` | VARCHAR |
| `cluster_id` | VARCHAR |
| `first_seen` | TIMESTAMP |
| `last_seen` | TIMESTAMP |
| `supersedes` | VARCHAR |

Partition Parquet by `observed_at` date (`observed_at_date=YYYY-MM-DD/`). Time-range queries are the whole point of a watch, and date partitioning makes them cheap. Optionally add a second partition on `source.collector` if one collector dwarfs the others. Sort within a partition by `observed_at`. For spatial queries, load DuckDB's `spatial` extension and index on `geo`; skip it until you actually need radius queries.

### SQLite (the Fleet-watch store)

A single `observations` table. JSON fields are `TEXT`, queried with `json_extract`. Unique index on `obs_id`; indexes on `observed_at`, `obs_type`, and `cluster_id`. Same schema, lighter engine, for projects that do not need columnar scans.

## Collector contract

When you write collector number N, this is the entire job. Produce these fields:

- `source` with `collector` and `source_type` set, plus `publisher`, `url`, and `native_id` wherever the source provides them
- `fetched_at` and `observed_at`, both UTC and timezone-aware
- `obs_type` and `title`
- `reliability` and `credibility` from the collector's configured baseline
- `raw`, the untouched source payload

The core fills the rest: `obs_id` (hashed), `schema_version`, `content_hash`, `cluster_id`, `first_seen`, `last_seen`, and any normalization of `summary`, `entities`, or `geo` the collector did not do itself. A collector that wants to enrich (extract entities, geolocate) may set the optional fields directly; otherwise it leaves them empty and the core handles them.

## Pydantic v2 model

This is the buildable form. `Observation.model_json_schema()` emits a JSON Schema from it, so there is no separate schema file to maintain.

```python
# watchcore/observation.py
from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum, IntEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


class SourceType(str, Enum):
    rss = "rss"
    api = "api"
    ais = "ais"
    scrape = "scrape"
    feed = "feed"
    manual = "manual"


class ObsType(str, Enum):
    naval = "naval"
    air = "air"
    ground = "ground"
    posture = "posture"
    exercise = "exercise"
    logistics = "logistics"
    alert = "alert"
    financial = "financial"
    cyber = "cyber"
    other = "other"


class Reliability(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"


class Credibility(IntEnum):
    confirmed = 1
    probably_true = 2
    possibly_true = 3
    doubtful = 4
    improbable = 5
    cannot_judge = 6


class Confidence(str, Enum):
    high = "high"
    moderate = "moderate"
    low = "low"


class EntityType(str, Enum):
    unit = "unit"
    vessel = "vessel"
    aircraft = "aircraft"
    person = "person"
    org = "org"
    location = "location"
    platform = "platform"
    other = "other"


class Source(BaseModel):
    model_config = ConfigDict(extra="forbid")
    collector: str
    source_type: SourceType
    publisher: str | None = None
    url: str | None = None
    native_id: str | None = None


class Geo(BaseModel):
    model_config = ConfigDict(extra="forbid")
    lat: float
    lon: float
    precision_m: float | None = None
    place_name: str | None = None
    geohash: str | None = None


class Entity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: EntityType
    name: str
    value: str | None = None
    role: str | None = None


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    obs_id: str
    source: Source
    fetched_at: AwareDatetime
    observed_at: AwareDatetime
    obs_type: ObsType
    title: str
    reliability: Reliability
    credibility: Credibility
    raw: dict

    summary: str | None = None
    entities: list[Entity] = Field(default_factory=list)
    geo: Geo | None = None
    tags: list[str] = Field(default_factory=list)
    valid_from: AwareDatetime | None = None
    valid_to: AwareDatetime | None = None
    confidence: Confidence | None = None

    content_hash: str | None = None
    cluster_id: str | None = None
    first_seen: AwareDatetime | None = None
    last_seen: AwareDatetime | None = None
    supersedes: str | None = None

    @staticmethod
    def _norm(text: str) -> str:
        import re
        return re.sub(r"[^a-z0-9 ]", "", text.lower()).strip()

    @staticmethod
    def make_id(
        collector: str,
        observed_at: datetime,
        native_id: str | None = None,
        url: str | None = None,
        title: str | None = None,
    ) -> str:
        key = native_id or url or Observation._norm(title or "")
        basis = f"{collector}|{key}|{observed_at.isoformat()}"
        return hashlib.blake2b(basis.encode(), digest_size=16).hexdigest()

    @staticmethod
    def make_content_hash(title: str, summary: str | None = None) -> str:
        text = Observation._norm(f"{title} {summary or ''}")
        return hashlib.blake2b(text.encode(), digest_size=8).hexdigest()

    def grade_code(self) -> str:
        return f"{self.reliability.value}{int(self.credibility)}"
```

A collector builds the required fields, then the core calls `Observation.make_id(...)` and constructs the record. `extra="forbid"` means a typo in a field name raises instead of silently writing a column nobody reads.

## Worked example

A DVIDS press release, fully populated after ingest:

```json
{
  "schema_version": 1,
  "obs_id": "9f2b7c1d4e8a05f3a6b9c2d1e4f70a8b",
  "source": {
    "collector": "dvids",
    "source_type": "rss",
    "publisher": "U.S. Central Command",
    "url": "https://www.centcom.mil/MEDIA/PRESS-RELEASES/example",
    "native_id": "centcom-2026-0617-a"
  },
  "fetched_at": "2026-06-19T14:02:11Z",
  "observed_at": "2026-06-17T09:30:00Z",
  "obs_type": "naval",
  "title": "Carrier strike group transits Strait of Hormuz",
  "reliability": "B",
  "credibility": 2,
  "summary": "CENTCOM announced a carrier strike group transit in coordination with regional partners.",
  "entities": [
    { "type": "platform", "name": "Carrier strike group", "value": null, "role": "subject" },
    { "type": "location", "name": "Strait of Hormuz", "value": null, "role": "location" }
  ],
  "geo": { "lat": 26.57, "lon": 56.25, "precision_m": null, "place_name": "Strait of Hormuz", "geohash": null },
  "tags": ["centcom", "transit"],
  "valid_from": null,
  "valid_to": null,
  "confidence": null,
  "content_hash": "c14a9f0b2d6e8137",
  "cluster_id": null,
  "first_seen": "2026-06-19T14:02:11Z",
  "last_seen": "2026-06-19T14:02:11Z",
  "supersedes": null
}
```

An AIS observation differs mainly in `source.source_type` (`ais`), `native_id` (MMSI plus timestamp), a populated `geo`, and a vessel `entity` carrying the MMSI in `value`. Same shape, different boxes filled.

## Versioning and migration

`schema_version` is `1`. Additive changes (new optional fields) stay on version `1`; existing stored rows remain valid because the new field reads as null. A breaking change (rename, removal, or a changed meaning) increments to `2`, ships with a one-time migration over the stored Parquet, and readers keep handling `1` rows until they are migrated or aged out.

## Decisions

The four points the draft left open are settled here, matching the two-stage approach you already run. If your Follow-the-Money numbers differ, change them in one place.

1. **`obs_id` fallback.** When `native_id` is absent, fall back to the canonical `url`, then to a blake2b hash of the normalized `title`. All three combine with `observed_at`, so any one source item resolves to one stable id. Implemented in `make_id` above.
2. **Near-dup detection.** Stage one is exact: `content_hash` is a blake2b hash of the normalized `title` plus `summary` (lowercased, punctuation stripped, whitespace collapsed). Equal hashes are the same item and share a `cluster_id` directly. Stage two is fuzzy: build a MinHash signature over 3-word shingles of the normalized text and group records whose Jaccard similarity is `>= 0.7` into one cluster, using `datasketch` MinHashLSH. `make_content_hash` above produces the stage-one hash.
3. **Spatial indexing.** Store a precision-6 `geohash` whenever `geo` is present (cheap, and enough for grouping by area), but do not load DuckDB's `spatial` extension until a project needs true radius queries. Turn it on per project, not in the core.
4. **`raw` retention.** Keep `raw` inline in the store indefinitely. Parquet compresses the JSON well and your volumes are modest at a six-hour cadence. Add compaction only if a store grows past the point where scans slow down.
