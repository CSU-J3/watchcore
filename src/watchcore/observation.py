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
