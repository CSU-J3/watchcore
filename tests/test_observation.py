from datetime import datetime, timezone

import pytest

from watchcore import Observation, Source


def _obs(**overrides):
    base = dict(
        obs_id="x",
        source=Source(collector="dvids", source_type="rss", native_id="abc"),
        fetched_at=datetime(2026, 6, 19, 14, tzinfo=timezone.utc),
        observed_at=datetime(2026, 6, 17, 9, 30, tzinfo=timezone.utc),
        obs_type="naval",
        title="Carrier transit",
        reliability="B",
        credibility=2,
        raw={"k": "v"},
    )
    base.update(overrides)
    return Observation(**base)


def test_make_id_is_deterministic():
    t = datetime(2026, 6, 17, 9, 30, tzinfo=timezone.utc)
    assert Observation.make_id("dvids", t, native_id="abc") == \
        Observation.make_id("dvids", t, native_id="abc")


def test_make_id_fallback_order():
    t = datetime(2026, 6, 17, tzinfo=timezone.utc)
    # native_id missing -> url is used, giving a different basis than title-only
    assert Observation.make_id("c", t, url="http://x/1") != \
        Observation.make_id("c", t, title="Hello World")
    # title fallback is stable under normalization
    assert Observation.make_id("c", t, title="Hello, World!") == \
        Observation.make_id("c", t, title="hello world")


def test_content_hash_is_stable_under_normalization():
    assert Observation.make_content_hash("Carrier Transit!", "Strait of Hormuz") == \
        Observation.make_content_hash("carrier transit", "strait of hormuz")


def test_grade_code():
    assert _obs().grade_code() == "B2"


def test_extra_fields_are_forbidden():
    with pytest.raises(Exception):
        _obs(nonsense_field=1)
