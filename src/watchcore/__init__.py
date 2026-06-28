"""watchcore: the shared core under OSINT watch projects.

The package centers on one contract, the Observation record. Collectors emit
Observations; the pipeline stages (normalize, dedup, store, grade, detect,
serve) read and write them. Individual watch projects depend on this package
and add their own collectors and frontend.
"""

from watchcore.observation import (
    SCHEMA_VERSION,
    Confidence,
    Credibility,
    Entity,
    EntityType,
    Geo,
    Observation,
    ObsType,
    Reliability,
    Source,
    SourceType,
)

__version__ = "0.1.0"

__all__ = [
    "SCHEMA_VERSION",
    "Confidence",
    "Credibility",
    "Entity",
    "EntityType",
    "Geo",
    "Observation",
    "ObsType",
    "Reliability",
    "Source",
    "SourceType",
    "__version__",
]
