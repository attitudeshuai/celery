"""Tests for the reconcilable event continuity channel."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from celery import states
from celery.events.continuity import (
    ContinuityTracker,
    make_inspect_requester,
    validate_continuity_settings,
)
from celery.events.dispatcher import process_continuity
from celery.exceptions import ImproperlyConfigured
from celery.utils.collections import AttributeDict
from celery.worker import control
from celery.worker import state as worker_state
from celery.worker.request import Request


def conf(**overrides):
    values = dict(
        event_continuity_enabled=True,
        event_continuity_state_ttl=3600.0,
        event_continuity_resync_timeout=1.0,
        event_continuity_resync_retries=2,
        event_continuity_resync_range=1000,
        event_continuity_gap_tolerance=32,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def make_event(seq, session='s1', hostname='w1',
               type='worker-heartbeat', timestamp=None, **extra):
    event = {
        'type': type,
        'hostname': hostname,
        'session': session,
        'seq': seq,
        'clock': seq,
        'timestamp': timestamp if timestamp is not None else float(seq),
        'local_received': float(seq),
    }
    event.update(extra)
    return event


# -- configuration ----------------------------------------------------------

class test_ContinuityConfig:

    def test_defaults_disabled(self):
        assert self.app.conf.event_continuity_enabled is False
        assert self.app.conf.event_continuity_state_ttl == 3600.0
        assert self.app.conf.event_continuity_resync_timeout == 1.0
        assert self.app.conf.event_continuity_resync_retries == 2
        assert self.app.conf.event_continuity_resync_range == 1000
        assert self.app.conf.event_continuity_gap_tolerance == 32

    def test_disabled_never_validates(self):
        # even nonsensical values are fine while the feature is off.
        validate_continuity_settings(
            conf(event_continuity_enabled=False,
                 event_continuity_state_ttl=-1,
                 event_continuity_resync_retries='nope'))

    @pytest.mark.parametrize('field,bad', [
        ('event_continuity_state_ttl', 0),
        ('event_continuity_state_ttl', '10'),
        ('event_continuity_state_ttl', True),
        ('event_continuity_resync_timeout', -1.0),
        ('event_continuity_resync_timeout', None),
        ('event_continuity_resync_retries', -1),
        ('event_continuity_resync_retries', 1.5),
        ('event_continuity_resync_retries', True),
        ('event_continuity_resync_range', 0),
        ('event_continuity_resync_range', '1000'),
        ('event_continuity_gap_tolerance', -3),
        ('event_continuity_gap_tolerance', False),
    ])
    def test_invalid_values_raise(self, field, bad):
        with pytest.raises(ImproperlyConfigured):
            validate_continuity_settings(conf(**{field: bad}))

    def test_valid_values_pass(self):
        validate_continuity_settings(conf())

    def test_tracker_from_app_validates(self):
        self.app.conf.event_continuity_enabled = True
        self.app.conf.event_continuity_resync_range = 0
        try:
            with pytest.raises(ImproperlyConfigured):
                ContinuityTracker.from_app(self.app)
        finally:
            self.app.conf.event_continuity_enabled = False
            self.app.conf.event_continuity_resync_range = 1000


# -- dispatcher (sender) ----------------------------------------------------

class MockProducer:

    raise_on_publish = False

    def __init__(self, *args, **kwargs):
        self.sent = []

    def publish(self, msg, *args, **kwargs):
        if self.raise_on_publish:
            raise KeyError()
        self.sent.append(msg)

    def close(self):
        pass


class test_DispatcherContinuity:

    def setup_method(self):
        process_continuity.reset()
        self._enabled = self.app.conf.event_continuity_enabled
        self.app.conf.event_continuity_enabled = True
        producer_connection = Mock()
        producer_connection.transport.driver_type = 'amqp'
        self.connection = producer_connection

    def teardown_method(self):
        self.app.conf.event_continuity_enabled = self._enabled
        process_continuity.reset()

    def eventer(self, **kwargs):
        eventer = self.app.events.Dispatcher(
            self.connection, enabled=False,
            buffer_while_offline=True, **kwargs)
        eventer.producer = MockProducer()
        eventer.enabled = True
        return eventer

    def test_disabled_events_have_no_continuity_fields(self):
        self.app.conf.event_continuity_enabled = False
        eventer = self.eventer()
        eventer.send('worker-heartbeat')
        event = eventer.producer.sent[0]
        assert 'seq' not in event
        assert 'session' not in event
        assert eventer.continuity_info() == {'enabled': False}

    def test_sequence_monotonic_and_shared_session(self):
        eventer = self.eventer()
        eventer.send('worker-heartbeat')
        eventer.send('worker-online')
        eventer.send('worker-heartbeat')
        seqs = [e['seq'] for e in eventer.producer.sent]
        sessions = {e['session'] for e in eventer.producer.sent}
        assert seqs == [1, 2, 3]
        assert len(sessions) == 1

    def test_batch_events_get_individual_consecutive_numbers(self):
        eventer = self.eventer(buffer_group={'task'}, buffer_limit=10)
        eventer.send('task-received', uuid='a')
        eventer.send('task-started', uuid='a')
        eventer.send('task-succeeded', uuid='a', runtime=1.0)
        eventer.flush()
        (batch,) = eventer.producer.sent
        assert [e['seq'] for e in batch] == [1, 2, 3]
        assert len({e['session'] for e in batch}) == 1
        # an event sent after the batch continues the sequence
        eventer.send('worker-heartbeat')
        assert eventer.producer.sent[-1]['seq'] == 4

    def test_offline_replay_keeps_sequence_numbers(self):
        eventer = self.eventer()
        eventer.producer.raise_on_publish = True
        eventer.send('worker-heartbeat')
        eventer.send('worker-heartbeat')
        eventer.send('worker-heartbeat')
        assert len(eventer._outbound_buffer) == 3
        buffered_seqs = [e['seq'] for e, _ in eventer._outbound_buffer]
        assert buffered_seqs == [1, 2, 3]

        eventer.producer.raise_on_publish = False
        eventer.flush()
        sent_seqs = [e['seq'] for e in eventer.producer.sent]
        assert sent_seqs == [1, 2, 3]

        eventer.send('worker-heartbeat')
        assert eventer.producer.sent[-1]['seq'] == 4

    def test_batch_replay_keeps_sequence_numbers(self):
        eventer = self.eventer(buffer_group={'task'}, buffer_limit=10)
        eventer.send('task-received', uuid='a')
        eventer.send('task-started', uuid='a')
        eventer.producer.raise_on_publish = True
        eventer.flush()
        assert len(eventer._outbound_buffer) == 1
        eventer.producer.raise_on_publish = False
        eventer.flush()
        (batch,) = eventer.producer.sent
        assert [e['seq'] for e in batch] == [1, 2]

    def test_remote_toggling_groups_keeps_counter(self):
        eventer = self.eventer()
        eventer.groups = {'worker'}
        eventer.send('worker-heartbeat')
        # remote control toggling task event groups
        eventer.groups.discard('task')
        eventer.groups.add('task')
        eventer.send('worker-heartbeat')
        assert [e['seq'] for e in eventer.producer.sent] == [1, 2]

    def test_session_changes_with_pid_and_survives_reconnect(self):
        eventer = self.eventer()
        eventer.send('worker-heartbeat')
        first_session = eventer.producer.sent[0]['session']

        # a new dispatcher in the same process (broker reconnect) inherits
        # the same session and counter through the process-wide registry.
        eventer2 = self.eventer()
        eventer2.send('worker-heartbeat')
        assert eventer2.producer.sent[0]['session'] == first_session
        assert eventer2.producer.sent[0]['seq'] == 2

        # a different process (restart/fork) starts a new session at seq 1.
        eventer2.pid = 2 ** 22 + 7
        eventer2.send('worker-heartbeat')
        restarted = eventer2.producer.sent[-1]
        assert restarted['session'] != first_session
        assert restarted['seq'] == 1

    def test_drops_are_recorded_when_buffer_is_full(self):
        eventer = self.eventer()
        eventer.CONTINUITY_OUTBOUND_LIMIT = 2
        eventer.producer.raise_on_publish = True
        eventer.send('worker-heartbeat')
        eventer.send('worker-heartbeat')
        eventer.send('worker-heartbeat')
        assert len(eventer._outbound_buffer) == 2
        assert eventer.continuity_dropped == 1
        info = eventer.continuity_info()
        assert info['enabled'] is True
        assert info['seq'] == 3
        assert info['dropped'] == 1
        assert info['dropped_seqs'] == [3]

        eventer.producer.raise_on_publish = False
        eventer.flush()
        assert [e['seq'] for e in eventer.producer.sent] == [1, 2]
        # allocation continues uninterrupted after the drop
        eventer.send('worker-heartbeat')
        assert eventer.producer.sent[-1]['seq'] == 4


# -- tracker (monitor) ------------------------------------------------------

class test_ContinuityTracker:

    def tracker(self, request_snapshot=None, **kwargs):
        options = dict(gap_tolerance=32, state_ttl=3600.0)
        options.update(kwargs)
        return ContinuityTracker(request_snapshot=request_snapshot, **options)

    def test_in_order_delivery(self):
        tracker = self.tracker()
        assert tracker.observe(make_event(1)) == [make_event(1)]
        assert tracker.observe(make_event(2))[0]['seq'] == 2
        state = tracker.continuity_state('w1')
        assert state['w1']['next_seq'] == 3
        assert state['w1']['high_water'] == 2
        assert state['w1']['open_gaps'] == []

    def test_reordered_events_held_then_delivered_in_order(self):
        tracker = self.tracker(gap_tolerance=32)
        # seq 1 is already contiguous; a heartbeat (immediate) overtakes
        # the buffered task batch 2..5 and arrives ahead of it.
        tracker.observe(make_event(1))
        assert tracker.observe(make_event(6)) == []
        state = tracker.continuity_state('w1')
        assert state['w1']['pending'] == 1
        delivered = []
        for seq in range(2, 6):
            delivered.extend(tracker.observe(make_event(seq)))
        assert [e['seq'] for e in delivered] == [2, 3, 4, 5, 6]
        assert tracker.gap_count() == 1
        assert tracker.gap_count(status='filled') == 1
        assert tracker.gap_count(status='open') == 0

    def test_duplicate_numbers_are_dropped(self):
        tracker = self.tracker()
        tracker.observe(make_event(1))
        tracker.observe(make_event(2))
        assert tracker.observe(make_event(1)) == []
        assert tracker.observe(make_event(2)) == []
        assert tracker.continuity_state()['w1']['duplicates'] == 2

    def test_legacy_events_pass_through(self):
        tracker = self.tracker()
        legacy = {'type': 'task-sent', 'hostname': 'client1'}
        assert tracker.observe(legacy) == [legacy]
        # malformed continuity fields do not crash the monitor either
        assert tracker.observe(
            {'type': 'worker-heartbeat', 'hostname': 'w1',
             'session': 's1', 'seq': 'x'})[0]['type'] == 'worker-heartbeat'
        assert tracker.legacy_events == 2

    def test_small_gap_within_tolerance_does_not_resync(self):
        requests = []
        tracker = self.tracker(
            request_snapshot=lambda host, since: requests.append((host, since)),
            gap_tolerance=32)
        tracker.observe(make_event(1))
        assert tracker.observe(make_event(5)) == []
        assert requests == []
        assert tracker.gap_count(status='open') == 1

    def test_gap_beyond_tolerance_triggers_one_resync(self):
        requests = Mock(return_value={
            'hostname': 'w1',
            'timestamp': 99.0,
            'continuity': {'session': 's1', 'seq': 10},
            'active': [], 'reserved': [], 'completed': [],
        })
        state = self.app.events.State()
        tracker = self.tracker(request_snapshot=requests,
                               gap_tolerance=2, state=state)
        tracker.observe(make_event(1))
        # missing 2..9 (8 events) > tolerance 2 -> resync, seq 10 is
        # covered by the snapshot and therefore not delivered again.
        assert tracker.observe(make_event(10)) == []
        requests.assert_called_once_with('w1', 1)
        assert tracker.gap_count(status='resynced') == 1
        assert tracker.resync_count('w1') == {
            'requests': 1, 'ok': 1, 'failed': 0}
        # later events continue normally
        assert tracker.observe(make_event(11))[0]['seq'] == 11

    def test_snapshot_corrects_stuck_and_reserved_tasks_without_regression(self):
        stuck = make_event(
            1, type='task-started', uuid='t-stuck',
            timestamp=1.0, name='tasks.stuck')
        failed = make_event(
            1, type='task-failed', uuid='t-failed',
            timestamp=1.0, name='tasks.fail')
        # two workers would collide on seq 1, so use one stream for stuck
        # and seed the failed task directly into state.
        state = self.app.events.State()
        state.event(stuck)
        state.event(failed)
        # reserved-only task, unknown to the monitor
        snapshot = {
            'hostname': 'w1',
            'timestamp': 20.0,
            'continuity': {'session': 's1', 'seq': 10},
            'active': [{'id': 't-stuck', 'name': 'tasks.stuck'}],
            'reserved': [{'id': 't-reserved', 'name': 'tasks.reserved'}],
            'completed': ['t-stuck', 't-done', 't-failed'],
        }
        requests = Mock(return_value=snapshot)
        tracker = self.tracker(request_snapshot=requests,
                               gap_tolerance=0, state=state)
        tracker.observe(make_event(1))
        tracker.observe(make_event(10))

        stuck_task = state.tasks['t-stuck']
        assert stuck_task.state == states.SUCCESS
        assert stuck_task.succeeded == 20.0
        assert state.tasks['t-done'].state == states.SUCCESS
        reserved_task = state.tasks['t-reserved']
        assert reserved_task.state == states.RECEIVED
        assert reserved_task.name == 'tasks.reserved'
        # failure must not be regressed to success by the snapshot
        assert state.tasks['t-failed'].state == states.FAILURE

    def test_events_arriving_during_resync_merge_after_snapshot(self):
        state = self.app.events.State()
        started = make_event(
            1, type='task-started', uuid='t1',
            timestamp=1.0, name='tasks.t1')

        def requester(hostname, since):
            # the success event arrives while the request is in flight
            arrived = tracker.observe(
                make_event(11, type='task-succeeded', uuid='t1',
                           timestamp=11.0, runtime=1.0))
            assert arrived == []  # held until snapshot is merged
            return {
                'hostname': 'w1',
                'timestamp': 10.5,
                'continuity': {'session': 's1', 'seq': 10},
                'active': [{'id': 't1', 'name': 'tasks.t1'}],
                'reserved': [], 'completed': [],
            }

        tracker = self.tracker(request_snapshot=requester, gap_tolerance=0,
                               state=state)
        for event in tracker.observe(started):
            state.event(event)
        assert state.tasks['t1'].state == states.STARTED
        # seq 10 is superseded by the snapshot; seq 11 (newer) is merged
        # after it and wins.
        delivered = tracker.observe(make_event(10))
        assert [e['seq'] for e in delivered] == [11]
        for event in delivered:
            state.event(event)
        assert state.tasks['t1'].state == states.SUCCESS

    def test_failed_resync_keeps_gap_open_and_retries_on_growth(self):
        requests = Mock(return_value=None)
        tracker = self.tracker(request_snapshot=requests, gap_tolerance=2)
        tracker.observe(make_event(1))
        assert tracker.observe(make_event(10)) == []
        assert tracker.gap_count(status='open') == 1
        assert tracker.resync_count('w1') == {
            'requests': 1, 'ok': 0, 'failed': 1}
        # the gap grows past the previously attempted range -> a new
        # logical request is made.
        tracker.observe(make_event(11))
        assert requests.call_count == 2
        assert tracker.gap_count(status='open') == 1

    def test_stale_snapshot_from_restarted_worker_is_rejected(self):
        requests = Mock(return_value={
            'hostname': 'w1',
            'timestamp': 9.0,
            'continuity': {'session': 'OTHER', 'seq': 10},
            'active': [], 'reserved': [], 'completed': [],
        })
        tracker = self.tracker(request_snapshot=requests, gap_tolerance=0)
        tracker.observe(make_event(1))
        tracker.observe(make_event(10))
        assert tracker.gap_count(status='open') == 1
        assert tracker.resync_count('w1')['failed'] == 1

    def test_session_change_is_unrecoverable_for_old_session(self):
        requests = Mock(return_value=None)
        tracker = self.tracker(request_snapshot=requests, gap_tolerance=2)
        tracker.observe(make_event(1))
        tracker.observe(make_event(10))  # open gap in s1, resync fails
        assert tracker.gap_count(status='open') == 1

        delivered = tracker.observe(
            make_event(1, session='s2', timestamp=20.0))
        assert delivered[0]['session'] == 's2'
        assert tracker.gap_count(status='unrecoverable') == 1
        entry = tracker.continuity_state()['w1']
        unrecoverable = [w for w in entry['windows']
                         if w['status'] == 'unrecoverable']
        assert unrecoverable[0]['session'] == 's1'
        # window end is the timestamp of seq 10, the first event past
        # the gap (the new-session event arrives after it).
        assert unrecoverable[0]['window_end'] == 10.0
        assert unrecoverable[0]['window_start'] == 1.0
        # a resync is requested for the new session as well
        assert tracker.resync_count('w1')['requests'] == 2
        assert entry['session'] == 's2'
        assert entry['next_seq'] == 2

    def test_clean_restart_without_gap_only_resyncs_new_session(self):
        requests = Mock(return_value=None)
        tracker = self.tracker(request_snapshot=requests, gap_tolerance=32)
        tracker.observe(make_event(1))
        tracker.observe(make_event(2))
        tracker.observe(make_event(1, session='s2'))
        assert tracker.gap_count() == 0
        assert tracker.resync_count('w1')['requests'] == 1

    def test_queries_and_filters(self):
        requests = Mock(return_value=None)
        tracker = self.tracker(request_snapshot=requests, gap_tolerance=2)
        tracker.observe(make_event(1, hostname='w1'))
        tracker.observe(make_event(10, hostname='w1'))
        tracker.observe(make_event(1, hostname='w2'))
        tracker.observe(make_event(10, hostname='w2'))
        assert tracker.gap_count() == 2
        assert tracker.gap_count('w1') == 1
        assert tracker.resync_count() == {
            'requests': 2, 'ok': 0, 'failed': 2}
        assert tracker.resync_count('w2') == {
            'requests': 1, 'ok': 0, 'failed': 1}
        with pytest.raises(ValueError):
            tracker.gap_count(status='bogus')

    def test_state_ttl_pruning(self):
        clock = [100.0]
        requests = Mock(return_value=None)
        tracker = self.tracker(request_snapshot=requests, gap_tolerance=32,
                               state_ttl=10.0, timefun=lambda: clock[0])
        tracker.observe(make_event(1))
        tracker.observe(make_event(5))
        clock[0] = 101.0
        for seq in range(2, 6):
            tracker.observe(make_event(seq))
        assert tracker.gap_count(status='filled') == 1
        # long after the retention time, idle node and window are pruned
        clock[0] = 200.0
        tracker.observe(make_event(1, hostname='other'))
        assert 'w1' not in tracker.continuity_state()
        assert tracker.gap_count() == 0
        assert 'other' in tracker.continuity_state()


# -- default remote-control requester --------------------------------------

class _FakeInspect:

    calls = 0
    fail_times = 0
    result = None

    def event_snapshot(self, **kwargs):
        type(self).calls += 1
        if type(self).calls <= type(self).fail_times:
            raise OSError('boom')
        return type(self).result


def test_default_requester_retries_then_returns_snapshot():
    _FakeInspect.calls = 0
    _FakeInspect.fail_times = 2
    _FakeInspect.result = {'w1': {'ok': {
        'hostname': 'w1',
        'continuity': {'session': 's1', 'seq': 1}}}}

    class App:
        class control:
            @staticmethod
            def inspect(**kwargs):
                return _FakeInspect()

    requester = make_inspect_requester(App(), 1.0, 2, 1000)
    snapshot = requester('w1', 0)
    assert snapshot['continuity']['seq'] == 1
    assert _FakeInspect.calls == 3


def test_default_requester_gives_up_returns_none():
    _FakeInspect.calls = 0
    _FakeInspect.fail_times = 99
    _FakeInspect.result = None

    class App:
        class control:
            @staticmethod
            def inspect(**kwargs):
                return _FakeInspect()

    requester = make_inspect_requester(App(), 1.0, 1, 1000)
    assert requester('w1', 0) is None
    assert _FakeInspect.calls == 2


# -- receiver integration ---------------------------------------------------

class test_ReceiverContinuity:

    def test_continuity_off_by_default(self):
        r = self.app.events.Receiver(Mock())
        assert r.continuity is None

    def test_legacy_stream_with_tracker_passes_through(self):
        tracker = ContinuityTracker(gap_tolerance=32)
        handler = Mock()
        r = self.app.events.Receiver(
            Mock(), handlers={'*': handler},
            node_id='celery.tests', continuity=tracker)
        r._receive({'type': 'worker-heartbeat', 'hostname': 'w1'}, Mock())
        handler.assert_called_once()
        assert tracker.legacy_events == 1

    def test_reorders_are_delivered_in_sequence_order(self):
        tracker = ContinuityTracker(gap_tolerance=32)
        handler = Mock()
        r = self.app.events.Receiver(
            Mock(), handlers={'*': handler},
            node_id='celery.tests', continuity=tracker)
        r._receive(make_event(1), Mock())
        r._receive(make_event(6), Mock())
        assert handler.call_count == 1  # only seq 1 delivered so far
        for seq in range(2, 6):
            r._receive(make_event(seq), Mock())
        assert [c.args[0]['seq'] for c in handler.call_args_list] == \
            [1, 2, 3, 4, 5, 6]

    def test_multi_batch_preserves_order(self):
        tracker = ContinuityTracker(gap_tolerance=32)
        handler = Mock()
        r = self.app.events.Receiver(
            Mock(), handlers={'*': handler},
            node_id='celery.tests', continuity=tracker)
        r._receive(make_event(1), Mock())
        r._receive(make_event(7), Mock())
        batch = [make_event(seq) for seq in range(2, 7)]
        r._receive(batch, Mock())
        assert [c.args[0]['seq'] for c in handler.call_args_list] == \
            list(range(1, 8))

    def test_invalid_settings_raise_at_construction(self):
        self.app.conf.event_continuity_enabled = True
        self.app.conf.event_continuity_gap_tolerance = -1
        try:
            with pytest.raises(ImproperlyConfigured):
                self.app.events.Receiver(Mock())
        finally:
            self.app.conf.event_continuity_enabled = False
            self.app.conf.event_continuity_gap_tolerance = 32


# -- worker snapshot control command ---------------------------------------

SNAPSHOT_HOST = 'testevent@continuity'


class _Consumer:
    """Minimal consumer stand-in for the pidbox command state."""

    def __init__(self, app):
        self.app = app
        self.event_dispatcher = Mock()

    def tset(self, value):
        return set(value)


class test_EventSnapshotCommand:

    def setup_method(self):
        self.consumer = _Consumer(self.app)

        @self.app.task(name='c.unittest.continuity', shared=False)
        def mytask():
            pass
        self.mytask = mytask

        self.state = AttributeDict(
            app=self.app, hostname=SNAPSHOT_HOST,
            consumer=self.consumer, tset=set)
        self.panel = self.app.control.mailbox.Node(
            hostname=SNAPSHOT_HOST, state=self.state,
            handlers=control.Panel.data)

    def teardown_method(self):
        worker_state.successful_requests.discard('continuity-t-done')

    def test_command_registered(self):
        assert 'event_snapshot' in control.Panel.data

    def test_snapshot_contents(self):
        self.consumer.event_dispatcher.continuity_info.return_value = {
            'enabled': True, 'session': 's1', 'seq': 10,
            'dropped': 0, 'dropped_seqs': []}
        # other tests may leave entries in the global worker state; diff
        # against what was already there before making assertions.
        base_active = {r.id for r in worker_state.active_requests}
        base_reserved = {r.id for r in worker_state.reserved_requests}
        base_completed = set(worker_state.successful_requests)
        active = Request(
            self.TaskMessage(self.mytask.name, id='continuity-t-active'),
            app=self.app)
        reserved = Request(
            self.TaskMessage(self.mytask.name, id='continuity-t-reserved'),
            app=self.app)
        worker_state.active_requests.add(active)
        worker_state.reserved_requests.add(reserved)
        worker_state.successful_requests.add('continuity-t-done')
        try:
            reply = self.panel.handle(
                'event_snapshot', {'since_seq': 9, 'limit': 1000})
        finally:
            worker_state.active_requests.discard(active)
            worker_state.reserved_requests.discard(reserved)

        snapshot = reply['ok']
        assert snapshot['hostname'] == SNAPSHOT_HOST
        assert snapshot['since_seq'] == 9
        assert snapshot['continuity']['seq'] == 10
        active_ids = {t['id'] for t in snapshot['active']} - base_active
        reserved_ids = {t['id'] for t in snapshot['reserved']} - base_reserved
        completed = set(snapshot['completed']) - base_completed
        assert active_ids == {'continuity-t-active'}
        assert reserved_ids == {'continuity-t-reserved'}
        assert completed == {'continuity-t-done'}

    def test_invalid_limit_is_clamped(self):
        # an invalid limit must fall back to the default instead of
        # breaking the command; pre-existing global entries are ignored.
        base_completed = set(worker_state.successful_requests)
        reply = self.panel.handle('event_snapshot', {'limit': 0})
        snapshot = reply['ok']
        assert isinstance(snapshot['active'], list)
        assert isinstance(snapshot['completed'], list)
        assert not (set(snapshot['completed']) - base_completed)

    def test_without_dispatcher(self):
        self.consumer.event_dispatcher = None
        reply = self.panel.handle('event_snapshot', {})
        assert reply['ok']['continuity'] is None
