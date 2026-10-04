"""Reconcilable event continuity channel.

When :setting:`event_continuity_enabled` is enabled every event published by
a worker carries two extra fields:

* ``session`` -- an opaque identifier that is constant for the lifetime of a
  worker process and changes whenever the process is restarted.  It survives
  broker reconnects (a reconnect is *not* a restart).
* ``seq`` -- a strictly monotonically increasing, never repeated sequence
  number allocated within that session.  Allocation is not interrupted by
  buffered (``*.multi``) batch sends, remote event toggling or offline
  buffering/replay.

A monitor uses :class:`ContinuityTracker` to feed the event stream through
:meth:`~ContinuityTracker.observe`:

* Events are delivered to the local state machine in per-session sequence
  order; reordered events (e.g. an immediate heartbeat overtaking a buffered
  task batch) are held until the gap fills.
* A missing sequence range is a *gap*.  Small gaps wait for late delivery
  (``event_continuity_gap_tolerance``); larger gaps and session changes
  trigger exactly one state *resync* request per gap episode.
* A resync fetches a snapshot of the worker's active, reserved and recently
  completed tasks and merges it into the local state without ever regressing
  tasks that newer (post-snapshot) events already advanced.
* A session change is an *unrecoverable* gap for the old session: the dead
  process can never re-send its missing events.
* Resync requests run with a timeout and retries; when all attempts fail the
  gap stays open/unfilled.

Three accounting queries are provided: :meth:`continuity_state`,
:meth:`gap_count` and :meth:`resync_count`.

When the feature is disabled (the default) no ``session``/``seq`` fields are
published and events from old workers (or client ``task-sent`` events) that
lack these fields pass through untouched, so mixed-version clusters keep
working.
"""
import threading
import time

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

__all__ = (
    'ContinuityTracker', 'validate_continuity_settings',
    'make_inspect_requester',
)

logger = get_logger(__name__)

#: Gap episode is waiting for late (reordered) events or a resync.
GAP_OPEN = 'open'
#: All missing sequence numbers arrived as events.
GAP_FILLED = 'filled'
#: Missing events did not arrive but state was corrected from a snapshot.
GAP_RESYNCED = 'resynced'
#: Gap can never be filled (producer session ended, or snapshot cannot
#: cover it).
GAP_UNRECOVERABLE = 'unrecoverable'

_RESOLVED_STATUSES = (GAP_FILLED, GAP_RESYNCED, GAP_UNRECOVERABLE)

_SETTING_STATE_TTL = 'event_continuity_state_ttl'
_SETTING_RESYNC_TIMEOUT = 'event_continuity_resync_timeout'
_SETTING_RESYNC_RETRIES = 'event_continuity_resync_retries'
_SETTING_RESYNC_RANGE = 'event_continuity_resync_range'
_SETTING_GAP_TOLERANCE = 'event_continuity_gap_tolerance'


def validate_continuity_settings(conf):
    """Validate continuity related settings.

    Raises:
        celery.exceptions.ImproperlyConfigured: if any value is invalid.
            Called while the worker/monitor is starting, before it starts
            serving.
    """
    if not getattr(conf, 'event_continuity_enabled', False):
        return

    def _number(name, value):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ImproperlyConfigured(
                f'{name} must be a positive number, got {value!r}.')
        return value

    ttl = _number(_SETTING_STATE_TTL, getattr(conf, _SETTING_STATE_TTL))
    if ttl <= 0:
        raise ImproperlyConfigured(
            f'{_SETTING_STATE_TTL} must be greater than 0, got {ttl!r}.')

    timeout = _number(_SETTING_RESYNC_TIMEOUT,
                      getattr(conf, _SETTING_RESYNC_TIMEOUT))
    if timeout <= 0:
        raise ImproperlyConfigured(
            f'{_SETTING_RESYNC_TIMEOUT} must be greater than 0, '
            f'got {timeout!r}.')

    for name, min_value in ((_SETTING_RESYNC_RETRIES, 0),
                            (_SETTING_RESYNC_RANGE, 1),
                            (_SETTING_GAP_TOLERANCE, 0)):
        value = getattr(conf, name)
        # bool is a subclass of int -- reject it explicitly.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ImproperlyConfigured(
                f'{name} must be an integer, got {value!r}.')
        if value < min_value:
            raise ImproperlyConfigured(
                f'{name} must be >= {min_value}, got {value!r}.')


def make_inspect_requester(app, timeout, retries, range_limit):
    """Build a snapshot requester on top of the remote control client.

    Returns a callable ``(hostname, since_seq) -> snapshot dict | None``.
    The call never raises: timeouts, transport errors and absent replies
    result in :const:`None`, leaving any open gap unfilled.
    """

    def request_snapshot(hostname, since_seq):
        for _ in range(retries + 1):
            try:
                reply = app.control.inspect(
                    destination=[hostname], timeout=timeout,
                ).event_snapshot(since_seq=since_seq, limit=range_limit)
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    'Event continuity resync request to %s failed: %r',
                    hostname, exc)
                reply = None
            if isinstance(reply, dict):
                result = reply.get(hostname)
                if isinstance(result, dict) and 'ok' in result:
                    return result['ok']
        return None

    return request_snapshot


class ContinuityTracker:
    """Track per-session event continuity on the monitor side.

    Arguments:
        app: Celery app used for logging/default configuration.
        state (celery.events.State): Optional cluster state that resync
            snapshots are merged into.  May be attached later with
            :meth:`bind_state`.
        request_snapshot (Callable): ``(hostname, since_seq)`` returning a
            snapshot mapping, or :const:`None` to use the remote-control
            based default requester.
        state_ttl (float): Seconds to retain resolved gap/window records and
            idle session state.
        gap_tolerance (int): Number of missing events tolerated without
            requesting a resync; such gaps may still be filled by reordered
            events.
    """

    def __init__(self, app=None, state=None, request_snapshot=None,
                 state_ttl=3600.0, resync_timeout=1.0, resync_retries=2,
                 resync_range=1000, gap_tolerance=32,
                 timefun=time.monotonic):
        self.app = app
        self.state = state
        self.state_ttl = state_ttl
        self.resync_timeout = resync_timeout
        self.resync_retries = resync_retries
        self.resync_range = resync_range
        self.gap_tolerance = gap_tolerance
        self._time = timefun
        if request_snapshot is None:
            request_snapshot = make_inspect_requester(
                app, resync_timeout, resync_retries, resync_range)
        self._request_snapshot = request_snapshot
        self._lock = threading.RLock()
        # hostname -> live session tracking node
        self._nodes = {}
        # hostname -> cumulative counters (survives session restarts)
        self._stats = {}
        # resolved gap records (filled/resynced/unrecoverable), TTL pruned
        self._windows = []
        self._gap_id = 0
        # events without continuity fields (old workers, task-sent clients)
        self.legacy_events = 0

    @classmethod
    def from_app(cls, app, state=None, request_snapshot=None):
        """Create a tracker configured from application settings."""
        validate_continuity_settings(app.conf)
        return cls(
            app=app, state=state, request_snapshot=request_snapshot,
            state_ttl=app.conf.event_continuity_state_ttl,
            resync_timeout=app.conf.event_continuity_resync_timeout,
            resync_retries=app.conf.event_continuity_resync_retries,
            resync_range=app.conf.event_continuity_resync_range,
            gap_tolerance=app.conf.event_continuity_gap_tolerance,
        )

    def bind_state(self, state):
        """Attach the cluster :class:`~celery.events.State` to correct."""
        self.state = state

    # -- event ingestion ---------------------------------------------------

    def observe(self, event):
        """Feed one received event, returning events safe to apply now.

        Events are returned in per-session sequence order; they may be the
        given event itself, an empty list (event held, duplicate or
        superseded), or a list that also contains previously held events
        that have become contiguous (possibly after a resync snapshot).

        Events without ``session``/``seq`` fields (old workers, client
        ``task-sent`` events, malformed data) are returned unchanged.
        """
        hostname, session, seq = self._continuity_fields(event)
        if hostname is None:
            with self._lock:
                self.legacy_events += 1
            return [event]

        resync = None
        with self._lock:
            self._sweep_locked()
            stats = self._stats.setdefault(hostname, self._new_stats())
            node = self._nodes.get(hostname)
            if node is None:
                node = self._new_node(session, seq, event)
                self._nodes[hostname] = node
                deliverable = [event]
            elif node['session'] != session:
                # Producer process restarted: the old session can never
                # finish its stream.
                self._finish_gap_locked(
                    node, stats, GAP_UNRECOVERABLE,
                    window_end=event.get('timestamp'))
                node = self._new_node(session, seq, event)
                self._nodes[hostname] = node
                deliverable = [event]
                # A restart is always a discontinuity; request one snapshot
                # from the new process so stale local state can be corrected.
                node['resyncing'] = True
                node['resync_hi'] = node['high'] + 1
                node['_resync'] = (hostname, seq)
            else:
                deliverable = self._accept_locked(hostname, node, stats, event)
            resync = self._pop_resync_locked()

        cascade = []
        if resync is not None:
            cascade = self._do_resync(hostname, resync)
        return deliverable + cascade

    @staticmethod
    def _continuity_fields(event):
        if not isinstance(event, dict):
            return None, None, None
        seq = event.get('seq')
        session = event.get('session')
        hostname = event.get('hostname')
        if (isinstance(seq, bool) or not isinstance(seq, int) or seq < 1 or
                not isinstance(session, str) or not session or
                not isinstance(hostname, str) or not hostname):
            return None, None, None
        return hostname, session, seq

    def _new_node(self, session, seq, event):
        return {
            'session': session,
            'expected': seq + 1,
            'high': seq,
            'last_ts': event.get('timestamp'),
            'seen': self._time(),
            'pending': {},
            'gap': None,
            'resyncing': False,
            'resync_hi': 0,
        }

    @staticmethod
    def _new_stats():
        return {
            'gaps': 0,
            'open': 0,
            'filled': 0,
            'resynced': 0,
            'unrecoverable': 0,
            'resync_requests': 0,
            'resync_ok': 0,
            'resync_failed': 0,
            'duplicates': 0,
        }

    def _accept_locked(self, hostname, node, stats, event):
        seq = event['seq']
        ts = event.get('timestamp')
        node['seen'] = self._time()
        if seq < node['expected']:
            # Duplicate delivery, or an event superseded by a snapshot.
            stats['duplicates'] += 1
            return []

        if seq == node['expected']:
            out = [event]
            node['expected'] = seq + 1
            node['last_ts'] = ts
            pending = node['pending']
            while node['expected'] in pending:
                ev = pending.pop(node['expected'])
                out.append(ev)
                node['last_ts'] = ev.get('timestamp')
                node['expected'] += 1
            node['high'] = max(node['high'], node['expected'] - 1)
            if not pending:
                # Every missing event of the current episode arrived.
                self._finish_gap_locked(node, stats, GAP_FILLED)
            self._arm_resync_locked(hostname, node)
            return out

        # seq > expected: a hole in the sequence.
        node['pending'][seq] = event
        node['high'] = max(node['high'], seq)
        gap = node['gap']
        if gap is None:
            self._gap_id += 1
            gap = {
                'id': self._gap_id,
                'hostname': hostname,
                'session': node['session'],
                'lo': node['expected'],
                'hi': seq + 1,
                'opened': self._time(),
                'window_start': node['last_ts'],
                'window_end': ts,
            }
            node['gap'] = gap
            stats['gaps'] += 1
            stats['open'] += 1
        else:
            gap['hi'] = max(gap['hi'], seq + 1)
            if gap['window_end'] is None:
                gap['window_end'] = ts
        self._arm_resync_locked(hostname, node)
        return []

    def _missing_count_locked(self, node):
        gap = node['gap']
        if gap is None:
            return 0
        received_ahead = sum(
            1 for seq in node['pending'] if seq < gap['hi'])
        return gap['hi'] - max(node['expected'], gap['lo']) - received_ahead

    def _arm_resync_locked(self, hostname, node):
        if node['resyncing']:
            return
        gap = node['gap']
        if gap is None:
            # Session-change resyncs are armed explicitly.
            return
        missing = self._missing_count_locked(node)
        if missing > self.gap_tolerance and gap['hi'] > node['resync_hi']:
            node['resyncing'] = True
            node['resync_hi'] = gap['hi']
            node['_resync'] = (hostname, node['expected'] - 1)

    def _pop_resync_locked(self):
        for node in self._nodes.values():
            request = node.pop('_resync', None)
            if request is not None:
                return request
        return None

    # -- resync -------------------------------------------------------------

    def request_resync(self, hostname):
        """Explicitly request a resync for ``hostname`` (e.g. after a
        detected session restart).  Returns the events that can now be
        applied after the snapshot (newest first ordering is preserved)."""
        with self._lock:
            node = self._nodes.get(hostname)
            if node is None or node['resyncing']:
                return []
            node['resyncing'] = True
            node['resync_hi'] = node['high'] + 1
            since_seq = node['expected'] - 1
        return self._do_resync(hostname, (hostname, since_seq))

    def _do_resync(self, hostname, request):
        _hostname, since_seq = request
        stats = self._stats.setdefault(hostname, self._new_stats())
        stats['resync_requests'] += 1
        try:
            snapshot = self._request_snapshot(hostname, since_seq)
        except Exception as exc:  # defensive: requester should not raise
            logger.warning(
                'Event continuity resync request to %s raised: %r',
                hostname, exc)
            snapshot = None

        cascade = []
        with self._lock:
            node = self._nodes.get(hostname)
            if not self._snapshot_usable_locked(node, snapshot):
                stats['resync_failed'] += 1
                if node is not None:
                    node['resyncing'] = False
                # Gap intentionally left open/unfilled.
                return []
            stats['resync_ok'] += 1
            node['resyncing'] = False
            cascade = self._apply_snapshot_locked(node, stats, snapshot)
        return cascade

    @staticmethod
    def _snapshot_usable_locked(node, snapshot):
        if not isinstance(snapshot, dict):
            return False
        continuity = snapshot.get('continuity') or {}
        session = continuity.get('session')
        seq = continuity.get('seq')
        if node is None or session != node['session']:
            # No reply, an old worker without continuity, or a stale reply
            # from a worker that restarted since the request was sent.
            return False
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            return False
        return True

    def _apply_snapshot_locked(self, node, stats, snapshot):
        snap_seq = snapshot['continuity']['seq']
        hostname = snapshot.get('hostname') or next(
            (name for name, n in self._nodes.items() if n is node), None)

        self._merge_snapshot_state(hostname, snapshot, snap_seq)

        # Events with seq <= snap_seq predate (and are represented by) the
        # snapshot; drop held copies and skip any late redelivery.
        pending = node['pending']
        for seq in [s for s in pending if s <= snap_seq]:
            del pending[seq]
        if snap_seq + 1 > node['expected']:
            node['expected'] = snap_seq + 1
        node['high'] = max(node['high'], snap_seq)

        # Newer events that arrived during the resync are merged by sequence:
        # apply everything that is contiguous now, after the snapshot.
        out = []
        while node['expected'] in pending:
            ev = pending.pop(node['expected'])
            out.append(ev)
            node['last_ts'] = ev.get('timestamp')
            node['expected'] += 1
        node['high'] = max(node['high'], node['expected'] - 1)

        gap = node['gap']
        if gap is not None and not pending:
            # Missing events are not replayed; state was corrected from the
            # snapshot, so record the episode as resynced (its exact event
            # history is an unrecoverable time window).
            self._finish_gap_locked(
                node, stats, GAP_RESYNCED,
                window_end=None)
        elif gap is not None:
            # Snapshot only covered part of the hole; keep the rest open.
            gap['lo'] = node['expected']
            gap['hi'] = node['high'] + 1
        return out

    def _merge_snapshot_state(self, hostname, snapshot, snap_seq):
        state = self.state
        if state is None or not hostname:
            return
        ts = snapshot.get('timestamp')
        if not isinstance(ts, (int, float)):
            ts = time.time()

        def worker():
            obj, _ = state.get_or_create_worker(hostname)
            return obj

        def index_task(task, name):
            if name:
                task.name = name
                state._seen_types.add(name)
                state.tasks_by_type[name].add(task)
                state.tasks_by_worker[hostname].add(task)

        def advance(info, target, time_attr):
            if isinstance(info, dict):
                task_id = info.get('id') or info.get('uuid')
                name = info.get('name')
            else:
                task_id, name = info, None
            if not task_id:
                return
            task, _ = state.get_or_create_task(task_id)
            # Never regress: lower precedence index means a later state.
            # A snapshot entry only wins over strictly older local state.
            if states.precedence(task.state) <= states.precedence(target):
                return
            task.state = target
            setattr(task, time_attr, ts)
            task.timestamp = ts
            task.worker = worker()
            index_task(task, name)

        # Recently completed tasks unblock tasks stuck in non-ready states
        # because their terminal event was lost.
        for task_id in snapshot.get('completed') or ():
            if not task_id:
                continue
            task, _ = state.get_or_create_task(task_id)
            if task.state in states.READY_STATES:
                continue
            task.state = states.SUCCESS
            task.succeeded = ts
            task.timestamp = ts
            task.worker = worker()

        for info in snapshot.get('reserved') or ():
            advance(info, states.RECEIVED, 'received')
        for info in snapshot.get('active') or ():
            advance(info, states.STARTED, 'started')

    def _finish_gap_locked(self, node, stats, status, window_end=None):
        gap = node.get('gap') if node else None
        if gap is None:
            return
        node['gap'] = None
        stats['open'] -= 1
        stats[status] += 1
        record = dict(gap)
        record['status'] = status
        record['resolved'] = self._time()
        if window_end is not None and record.get('window_end') is None:
            record['window_end'] = window_end
        self._windows.append(record)

    # -- housekeeping -------------------------------------------------------

    def _sweep_locked(self):
        cutoff = self._time() - self.state_ttl
        if self._windows:
            self._windows = [w for w in self._windows
                             if w['resolved'] >= cutoff]
        for hostname, node in list(self._nodes.items()):
            if (node['gap'] is None and not node['pending'] and
                    not node['resyncing'] and node['seen'] < cutoff):
                self._nodes.pop(hostname, None)
                self._stats.pop(hostname, None)

    # -- queries ------------------------------------------------------------

    def continuity_state(self, hostname=None):
        """Return continuity state, per worker hostname.

        Includes session id, sequence watermarks, open gap records,
        resolved (including unrecoverable) time windows and resync
        counters.
        """
        with self._lock:
            self._sweep_locked()
            result = {}
            hostnames = [hostname] if hostname else list(self._nodes)
            windows_by_host = {}
            for window in self._windows:
                if hostname and window['hostname'] != hostname:
                    continue
                windows_by_host.setdefault(window['hostname'], []).append(
                    self._window_public(window))
            for name in hostnames:
                node = self._nodes.get(name)
                stats = self._stats.get(name, self._new_stats())
                entry = {
                    'legacy_events': self.legacy_events,
                    'resync': {
                        'requests': stats['resync_requests'],
                        'ok': stats['resync_ok'],
                        'failed': stats['resync_failed'],
                    },
                    'filled_gaps': stats['filled'],
                    'resynced_gaps': stats['resynced'],
                    'unrecoverable_gaps': stats['unrecoverable'],
                    'duplicates': stats['duplicates'],
                    'windows': windows_by_host.get(name, []),
                }
                if node is not None:
                    entry.update({
                        'session': node['session'],
                        'next_seq': node['expected'],
                        'high_water': node['high'],
                        'last_event_ts': node['last_ts'],
                        'pending': len(node['pending']),
                        'resyncing': node['resyncing'],
                        'open_gaps': ([self._gap_public(node['gap'])]
                                      if node['gap'] else []),
                    })
                else:
                    entry.update({
                        'session': None,
                        'next_seq': None,
                        'high_water': None,
                        'last_event_ts': None,
                        'pending': 0,
                        'resyncing': False,
                        'open_gaps': [],
                    })
                result[name] = entry
            return result

    def gap_count(self, hostname=None, status=None):
        """Number of detected gaps.

        Arguments:
            hostname: Restrict to one worker hostname.
            status: Restrict to ``open``, ``filled``, ``resynced`` or
                ``unrecoverable``.  Defaults to all gaps.
        """
        with self._lock:
            self._sweep_locked()
            if status is not None and status not in (GAP_OPEN,) + \
                    _RESOLVED_STATUSES:
                raise ValueError(f'Unknown gap status: {status!r}')
            total = 0
            if status is None or status == GAP_OPEN:
                for name, node in self._nodes.items():
                    if hostname and name != hostname:
                        continue
                    if node['gap'] is not None:
                        total += 1
            if status is None:
                records = self._windows
                total += sum(
                    1 for w in records
                    if not hostname or w['hostname'] == hostname)
            elif status in _RESOLVED_STATUSES:
                total += sum(
                    1 for w in self._windows
                    if w['status'] == status and
                    (not hostname or w['hostname'] == hostname))
            return total

    def resync_count(self, hostname=None):
        """Resync request counters.

        Returns ``{'requests', 'ok', 'failed'}`` (globally or for one
        hostname).
        """
        with self._lock:
            self._sweep_locked()
            totals = [0, 0, 0]
            for name, stats in self._stats.items():
                if hostname and name != hostname:
                    continue
                totals[0] += stats['resync_requests']
                totals[1] += stats['resync_ok']
                totals[2] += stats['resync_failed']
            return {'requests': totals[0], 'ok': totals[1],
                    'failed': totals[2]}

    @staticmethod
    def _gap_public(gap):
        return {
            'id': gap['id'],
            'session': gap['session'],
            'lo': gap['lo'],
            'hi': gap['hi'],
            'opened': gap['opened'],
            'window_start': gap['window_start'],
            'window_end': gap['window_end'],
            'status': GAP_OPEN,
        }

    def _window_public(self, window):
        return {
            'id': window['id'],
            'session': window['session'],
            'lo': window['lo'],
            'hi': window['hi'],
            'status': window['status'],
            'window_start': window.get('window_start'),
            'window_end': window.get('window_end'),
            'resolved': window['resolved'],
        }
