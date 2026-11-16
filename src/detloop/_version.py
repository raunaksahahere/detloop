"""Single source of truth for the detloop version."""

__version__ = "0.1.0"

#: Version of the canonical trace schema (see docs/TRACE_FORMAT.md).
TRACE_SCHEMA = "detloop-trace/1"
#: Version of the replay manifest schema.
MANIFEST_SCHEMA = "detloop-manifest/1"
#: Version of the exploration SQLite schema.
DB_SCHEMA_VERSION = 1
#: Version of the PRNG / decision-stream derivation (see docs/DETERMINISM.md).
PRNG_VERSION = "detloop-prng/1"
