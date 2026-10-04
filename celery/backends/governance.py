"""Storage-side capacity governance for key/value result backends.

This module is opt-in via :setting:`result_governance_enabled`.  When
governance is disabled the result backends behave exactly as they always
have: results are stored until ``result_expires`` removes them (either via
storage native TTLs or via the periodic ``celery.backend_cleanup`` task).

When enabled it adds three things on top of the existing expiry behaviour:

1. **Retention tiers** - per final task state retention
   (:setting:`result_governance_retention`), e.g. successful results may be
   kept for an hour while failures are kept for a month.
2. **Hard capacity ceilings** - both a result count ceiling
   (:setting:`result_governance_max_results`) and a payload byte ceiling
   (:setting:`result_governance_max_bytes`); configuring just one of them
   is enough and exceeding either dimension triggers eviction.  A ceiling
   of ``0`` stops new results from being written at all.
3. **Observability and manual control** - a read-only inventory
   (:meth:`~celery.backends.base.KeyValueStoreBackend.inspect_results`)
   and a manual cleanup entry point
   (:meth:`~celery.backends.base.KeyValueStoreBackend.cleanup_results`).

Eviction always removes the oldest ready results (ordered by their storage
write time) first.  Results that are not finished, results that still
belong to a group whose metadata is present in the store, and chord
counter/unlock keys are never removed.  Whether a group is still alive is
decided solely from the group metadata that currently exists in the
store - never from in-memory markers or caller supplied identifiers.
"""
from collections import namedtuple
from datetime import datetime, timedelta
from numbers import Real

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger
from celery.utils.time import is_naive, make_aware, maybe_iso8601

__all__ = (
    'ResultInventoryItem', 'StateUsage', 'ResultInventory', 'CleanupReport',
    'StoredResult', 'ScannedValue', 'CleanupFilters',
    'prepare_retention_policy', 'prepare_capacity_limit',
    'normalize_value_set', 'coerce_time_window', 'parse_date_done',
    'E_GOVERNANCE_UNSUPPORTED',
)

logger = get_logger(__name__)

E_GOVERNANCE_UNSUPPORTED = """\
result_governance_enabled is set, but the {backend} result backend does not
support storage-side capacity governance.  Governance requires the backend
to be able to enumerate the stored result keys.
"""

E_GOVERNANCE_RETENTION_MAPPING = """\
result_governance_retention must be a mapping of final task state to
retention time in seconds (int/float) or datetime.timedelta, got: {value!r}.
"""

E_GOVERNANCE_RETENTION_STATE = """\
result_governance_retention has an unknown state {state!r}. Retention can
only be configured for final task states: {valid}.
"""

E_GOVERNANCE_RETENTION_VALUE = """\
result_governance_retention[{state!r}] must be a non-negative number of
seconds or a datetime.timedelta, got: {value!r}.
"""

E_GOVERNANCE_LIMIT_VALUE = """\
{name} must be a non-negative integer (or None), got: {value!r}.
A value of 0 stops new results from being written.
"""

E_GOVERNANCE_WINDOW = """\
{name} must be an epoch timestamp (int/float), a datetime.datetime or an
ISO-8601 string, got: {value!r}.
"""

E_GOVERNANCE_DISABLED = """\
Storage-side result governance is disabled; set
result_governance_enabled = True to use it.
"""

#: One result key as seen during a storage scan.
#:
#: ``raw`` is only retained so the deletion step can re-verify that the key
#: still holds the same value before removing it (the scan and the deletion
#: may be interleaved with concurrent writers).
StoredResult = namedtuple('StoredResult', (
    'key', 'task_id', 'status', 'date_done', 'written_at',
    'size', 'group_id', 'raw',
))

#: A (key, raw payload, on-disk size, storage-side write timestamp) tuple
#: yielded by a backend specific enumerator.  ``size``/``written_at`` may be
#: ``None`` when the backend cannot provide them cheaply.
ScannedValue = namedtuple(
    'ScannedValue', ('key', 'raw', 'size', 'written_at'),
)

#: Filters shared by inventory queries and manual cleanup.
CleanupFilters = namedtuple(
    'CleanupFilters', ('states', 'task_ids', 'since', 'until'),
)

#: One row of :class:`ResultInventory`.
ResultInventoryItem = namedtuple('ResultInventoryItem', (
    'task_id', 'status', 'date_done', 'size_bytes', 'group_id',
    'retention_seconds', 'expires_at', 'protected', 'protection_reason',
))

#: Number of results and bytes occupied by one state.
StateUsage = namedtuple('StateUsage', ('count', 'bytes'))

#: Read-only inventory answer.
ResultInventory = namedtuple('ResultInventory', (
    'items', 'total_count', 'total_bytes', 'by_state',
))

#: Result of a (possibly dry-run) cleanup run.
CleanupReport = namedtuple('CleanupReport', (
    'deleted_count', 'freed_bytes', 'keys',
    'retention_deleted', 'capacity_deleted',
    'protected_count', 'inspected_count', 'inspected_bytes',
))


def normalize_value_set(value, name):
    """Normalize a string / iterable filter into a ``set`` (or ``None``)."""
    if value is None:
        return None
    if isinstance(value, str):
        return {value}
    try:
        return set(value)
    except TypeError as exc:
        raise TypeError(f'{name} must be a string or an iterable') from exc


def _coerce_retention_seconds(value, state):
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ImproperlyConfigured(
            E_GOVERNANCE_RETENTION_VALUE.format(state=state, value=value))
    if value < 0:
        raise ImproperlyConfigured(
            E_GOVERNANCE_RETENTION_VALUE.format(state=state, value=value))
    return float(value)


def prepare_retention_policy(value):
    """Validate/normalize ``result_governance_retention`` at startup."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ImproperlyConfigured(
            E_GOVERNANCE_RETENTION_MAPPING.format(value=value))
    policy = {}
    for state, retention in value.items():
        if state not in states.READY_STATES:
            raise ImproperlyConfigured(
                E_GOVERNANCE_RETENTION_STATE.format(
                    state=state, valid=', '.join(sorted(states.READY_STATES))))
        policy[state] = _coerce_retention_seconds(retention, state)
    return policy


def prepare_capacity_limit(value, name):
    """Validate a ``result_governance_max_*`` setting at startup."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ImproperlyConfigured(E_GOVERNANCE_LIMIT_VALUE.format(
            name=name, value=value))
    if value < 0:
        raise ImproperlyConfigured(E_GOVERNANCE_LIMIT_VALUE.format(
            name=name, value=value))
    return value


def parse_date_done(value, tz):
    """Parse a stored ``date_done`` ISO string into an aware datetime."""
    if not value:
        return None
    dt = maybe_iso8601(value)
    if is_naive(dt):
        dt = make_aware(dt, tz)
    return dt


def coerce_time_window(value, name, tz):
    """Normalize an epoch number / datetime / ISO string filter value."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, bool):
        raise ValueError(E_GOVERNANCE_WINDOW.format(name=name, value=value))
    elif isinstance(value, Real):
        dt = datetime.fromtimestamp(float(value), tz=tz)
    elif isinstance(value, str):
        dt = maybe_iso8601(value)
    else:
        raise ValueError(E_GOVERNANCE_WINDOW.format(name=name, value=value))
    if dt is not None and is_naive(dt):
        dt = make_aware(dt, tz)
    return dt
