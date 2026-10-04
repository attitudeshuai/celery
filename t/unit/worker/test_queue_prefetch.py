"""Tests for per-queue prefetch caps (worker_queue_prefetch_limits)."""
import socket
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from kombu import Queue

from celery.exceptions import ImproperlyConfigured
from celery.utils.collections import AttributeDict
from celery.worker.consumer.consumer import Consumer
from celery.worker.consumer.qos import (
    QoS,
    validate_queue_prefetch_limits,
)
from celery.worker.control import Panel
from celery.worker.state import reset_state, scheduled_requests

PREFETCH_COUNT_MAX = 0xFFFF


# ---------------------------------------------------------------------------
# configuration validation (startup errors)
# ---------------------------------------------------------------------------

class test_validate_queue_prefetch_limits:

    @pytest.mark.parametrize('value', [None, {}])
    def test_empty_is_valid(self, value):
        assert validate_queue_prefetch_limits(value) == {}

    def test_accepts_positive_ints(self):
        assert validate_queue_prefetch_limits({
            'fast': 1, 'slow': 100, 'edge': PREFETCH_COUNT_MAX,
        }) == {'fast': 1, 'slow': 100, 'edge': PREFETCH_COUNT_MAX}

    @pytest.mark.parametrize('bad', [
        [('a', 1)],            # not a mapping
        ('a', 1),              # tuple
        42,
        'fast=4',
    ])
    def test_non_mapping_rejected(self, bad):
        with pytest.raises(ImproperlyConfigured):
            validate_queue_prefetch_limits(bad)

    @pytest.mark.parametrize('key', ['', None, 5, b'fast'])
    def test_bad_queue_name_rejected(self, key):
        with pytest.raises(ImproperlyConfigured):
            validate_queue_prefetch_limits({key: 4})

    @pytest.mark.parametrize('value', [
        0, -1, True, False, 1.5, '4', None, PREFETCH_COUNT_MAX + 1,
    ])
    def test_bad_limit_rejected(self, value):
        with pytest.raises(ImproperlyConfigured):
            validate_queue_prefetch_limits({'fast': value})

    def test_consumer_startup_validates_configuration(self, app):
        app.conf.worker_queue_prefetch_limits = {'celery': -3}
        with pytest.raises(ImproperlyConfigured):
            Consumer(
                on_task_request=Mock(), app=app, pool=Mock(),
                timer=Mock(), controller=Mock(),
            )


# ---------------------------------------------------------------------------
# QoS manager
# ---------------------------------------------------------------------------

class PerQueueCase:
    """Build a configured per-queue QoS manager with a broker spy."""

    def get_qos(self, *, auto=16, limits=None, held=None, initial=None):
        self.applied = []

        def apply(queue, count):
            self.applied.append((queue, count))

        qos = QoS(
            lambda prefetch_count=None: None,
            initial if initial is not None else auto,
            queue_limits=dict(limits or {}),
        )
        self.held = held or {}
        qos.configure(
            auto_provider=lambda: auto,
            held_provider=lambda queue: self.held.get(queue, 0),
            apply=apply,
        )
        return qos

    def activate(self, qos, *queues):
        for queue in queues:
            qos.activate_queue(queue)
        return qos


class test_qos_legacy_mode:
    """With no caps the manager must behave like kombu.common.QoS."""

    def test_update_applies_global_integer(self):
        sent = []
        qos = QoS(lambda prefetch_count: sent.append(prefetch_count), 8)
        assert qos.per_queue_enabled is False
        qos.update()
        assert sent == [8]
        qos.decrement_eventually(3)
        qos.update()
        assert sent == [8, 5]

    def test_value_never_below_one(self):
        qos = QoS(lambda **kw: None, 1)
        qos.decrement_eventually()
        qos.decrement_eventually()
        assert qos.value == 1

    def test_info_reports_global_mode(self):
        qos = QoS(lambda **kw: None, 8)
        info = qos.info()
        assert info['mode'] == 'global'
        assert info['prefetch_count'] == 8
        assert info['queues'] == {}


class test_qos_targets(PerQueueCase):

    def test_uncapped_queue_follows_automatic_value(self):
        qos = self.get_qos(auto=12)
        assert qos.activate_queue('a') == 12

    def test_capped_queue_takes_min_of_cap_and_automatic(self):
        qos = self.get_qos(auto=16, limits={'slow': 4})
        assert qos.activate_queue('slow') == 4
        assert qos.activate_queue('fast') == 16

    def test_cap_above_automatic_falls_back_to_automatic(self):
        qos = self.get_qos(auto=8, limits={'slow': 100})
        assert qos.activate_queue('slow') == 8

    def test_automatic_value_never_below_one(self):
        qos = self.get_qos(auto=0, limits={'slow': 4})
        assert qos.activate_queue('slow') == 1


class test_qos_set_limit(PerQueueCase):

    def test_set_limit_applies_target_and_persists(self):
        qos = self.get_qos(auto=16)
        self.activate(qos, 'fast', 'slow')
        qos.set_limit('slow', 4)
        assert self.applied[-1] == ('slow', 4)
        assert qos.queue_limits == {'slow': 4}
        info = qos.info()['queues']['slow']
        assert info['target'] == 4
        assert info['actual'] == 4
        assert info['limit'] == 4
        assert info['auto'] == 16

    def test_illegal_value_rejected_at_runtime(self):
        qos = self.get_qos(auto=16)
        self.activate(qos, 'a')
        for bad in (0, -1, True, 'x', PREFETCH_COUNT_MAX + 1):
            with pytest.raises(ValueError):
                qos.set_limit('a', bad)
        # nothing was applied and no cap recorded
        assert self.applied == []
        assert qos.queue_limits == {}

    def test_unknown_queue_rejected(self):
        qos = self.get_qos(auto=16)
        self.activate(qos, 'a')
        with pytest.raises(ValueError):
            qos.set_limit('nope', 4)
        assert self.applied == []

    def test_clear_limit_restores_automatic_value(self):
        qos = self.get_qos(auto=16, limits={'slow': 4})
        self.activate(qos, 'slow')
        qos.clear_limit('slow')
        assert self.applied[-1] == ('slow', 16)
        assert 'slow' not in qos.queue_limits
        assert qos.info()['queues']['slow']['target'] == 16

    def test_broker_failure_rolls_back_target_and_limit(self):
        qos = self.get_qos(auto=16)
        self.activate(qos, 'a')

        def fail(queue, count):
            raise OSError('broker gone')

        qos._apply = fail
        with pytest.raises(OSError):
            qos.set_limit('a', 4)
        assert qos.queue_limits == {}
        assert qos.info()['queues']['a']['target'] == 16


class test_qos_held_floor(PerQueueCase):

    def test_actual_clamped_to_messages_already_held(self):
        qos = self.get_qos(auto=16, held={'slow': 6})
        self.activate(qos, 'slow')
        report = qos.set_limit('slow', 4)
        # window installed is max(target, held): the six held messages
        # cannot be reclaimed
        assert self.applied[-1] == ('slow', 6)
        assert report['actual'] == 6
        assert report['held'] == 6
        assert report['target'] == 4
        assert report['reclaimable'] == 2

    def test_reclaim_when_held_drains(self):
        held = {'slow': 6}
        qos = self.get_qos(auto=16, held=held)
        self.activate(qos, 'slow')
        qos.set_limit('slow', 4)
        assert self.applied[-1] == ('slow', 6)
        held['slow'] = 2
        qos.set_limit('slow', 4)
        assert self.applied[-1] == ('slow', 4)


class test_qos_recompute(PerQueueCase):

    def get_changing_qos(self, auto_holder, limits=None):
        self.applied = []
        qos = QoS(lambda **kw: None, auto_holder['auto'],
                  queue_limits=dict(limits or {}))
        qos.configure(
            auto_provider=lambda: auto_holder['auto'],
            held_provider=lambda queue: 0,
            apply=lambda q, c: self.applied.append((q, c)),
        )
        return qos

    def test_grow_raises_uncapped_only(self):
        holder = {'auto': 8}
        qos = self.get_changing_qos(holder, limits={'slow': 4})
        self.activate(qos, 'slow', 'fast')
        holder['auto'] = 16
        qos.recompute()
        qos.flush()
        assert dict(self.applied) == {'fast': 16}  # capped slow untouched

    def test_shrink_lowers_uncapped_and_keeps_caps(self):
        holder = {'auto': 16}
        qos = self.get_changing_qos(holder, limits={'slow': 4})
        self.activate(qos, 'slow', 'fast')
        holder['auto'] = 8
        qos.recompute()
        qos.flush()
        assert dict(self.applied) == {'fast': 8}
        assert qos.queue_limits == {'slow': 4}

    def test_cap_below_new_auto_survives_recompute(self):
        holder = {'auto': 8}
        qos = self.get_changing_qos(holder, limits={'slow': 20})
        self.activate(qos, 'slow')  # effective target 8
        holder['auto'] = 16
        qos.recompute()
        qos.flush()
        assert dict(self.applied) == {'slow': 16}
        holder['auto'] = 32
        qos.recompute()
        qos.flush()
        assert ('slow', 20) in self.applied
        assert qos.queue_limits == {'slow': 20}

    def test_failed_flush_keeps_queue_dirty(self):
        qos = self.get_qos(auto=16)
        self.activate(qos, 'slow')
        calls = {'n': 0}

        def flaky(queue, count):
            calls['n'] += 1
            if calls['n'] == 1:
                raise OSError('reset by peer')
            self.applied.append((queue, count))

        qos._apply = flaky
        qos._targets['slow'] = 2
        qos._dirty.add('slow')
        qos.flush()
        assert calls['n'] == 1
        assert 'slow' in qos._dirty
        qos.flush()
        assert self.applied == [('slow', 2)]
        assert 'slow' not in qos._dirty


class test_qos_borrowed(PerQueueCase):

    def _msg(self, queue, tag):
        return SimpleNamespace(delivery_info={
            'consumer_tag': tag, 'routing_key': queue,
        })

    def test_eta_borrow_and_return_per_queue(self):
        qos = self.get_qos(auto=16, limits={'slow': 4})
        self.activate(qos, 'slow')
        msg = self._msg('slow', 'tag-slow')
        qos.bind_message(msg, {'slow': 'tag-slow'})
        qos.increment_eventually()
        qos.note_borrow(msg)
        qos.flush()
        assert self.applied[-1] == ('slow', 5)
        qos.decrement_eventually()
        qos.note_return(msg)
        qos.flush()
        assert self.applied[-1] == ('slow', 4)

    def test_borrow_unknown_queue_is_ignored(self):
        qos = self.get_qos(auto=16)
        self.activate(qos, 'a')
        qos.note_borrow(self._msg('ghost', 't'))
        assert qos._dirty == set()


class test_qos_message_attribution(PerQueueCase):

    def test_consumer_tag_mapping_preferred(self):
        qos = self.get_qos(auto=8)
        msg = SimpleNamespace(delivery_info={
            'consumer_tag': 't2', 'routing_key': 'rk',
        })
        qos.bind_message(msg, {'a': 't1', 'b': 't2'})
        assert qos.queue_of_message(msg) == 'b'

    def test_falls_back_to_routing_key(self):
        qos = self.get_qos(auto=8)
        msg = SimpleNamespace(delivery_info={'routing_key': 'celery'})
        qos.bind_message(msg, None)
        assert qos.queue_of_message(msg) == 'celery'

    def test_mapping_survives_consumer_tag_change(self):
        # the queue is captured when the message arrives, before any
        # runtime adjustment cancels/re-consumes with a fresh tag
        qos = self.get_qos(auto=8)
        msg = SimpleNamespace(delivery_info={
            'consumer_tag': 'old-tag', 'routing_key': 'slow',
        })
        qos.bind_message(msg, {'slow': 'old-tag'})
        assert qos.queue_of_message(msg) == 'slow'


# ---------------------------------------------------------------------------
# Consumer integration
# ---------------------------------------------------------------------------

class ConsumerCase:
    def get_consumer(self, app):
        consumer = Consumer(
            on_task_request=Mock(),
            init_callback=Mock(),
            pool=Mock(),
            app=app,
            timer=Mock(),
            controller=Mock(),
            hub=None,
        )
        consumer.blueprint = Mock(name='blueprint')
        consumer.pool.num_processes = 4
        consumer._restart_state = Mock(name='_restart_state')
        consumer.connection = Mock()
        consumer.connection.connection_errors = (socket.error, OSError)
        consumer.connection.channel_errors = ()
        consumer.connection.transport.driver_type = 'amqp'
        consumer.conninfo = consumer.connection
        consumer.initial_prefetch_count = 16
        consumer.app.amqp.queues.aliases = {}
        return consumer

    def mock_task_consumer(self, *names):
        queues = [Queue(name) for name in names]
        task_consumer = Mock()
        task_consumer.queues = queues
        task_consumer._queues = {q.name: q for q in queues}
        task_consumer._active_tags = {}

        def basic_consume(queue, nowait=True):
            tag = f'tag-{queue.name}'
            task_consumer._active_tags[queue.name] = tag
            return tag

        task_consumer._basic_consume.side_effect = basic_consume
        task_consumer.channel = Mock()
        return task_consumer


class test_consumer_per_queue_switch(ConsumerCase):

    def test_switch_removes_global_window_and_binds_every_queue(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a', 'b')
        c.qos = QoS(lambda **kw: None, 16)

        report = c.set_queue_prefetch_limit('a', 4)

        channel = c.task_consumer.channel
        qos_calls = [x.args for x in channel.basic_qos.call_args_list]
        # legacy channel-global window removed (0 == unlimited)
        assert (0, 0, True) in qos_calls
        # every queue bound with per-consumer QoS
        assert (0, 4, False) in qos_calls
        assert (0, 16, False) in qos_calls
        # each queue consumed exactly once and tags registered
        assert c.task_consumer._basic_consume.call_count == 2
        assert set(c.task_consumer._active_tags) == {'a', 'b'}
        assert report['queue'] == 'a'
        assert report['target'] == 4
        # cap persisted on the consumer (survives reconnects)
        assert c.queue_prefetch_limits == {'a': 4}

    def test_runtime_change_cancels_and_reconsumes_only_that_queue(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a', 'b')
        c.qos = QoS(lambda **kw: None, 16)
        c.set_queue_prefetch_limit('a', 4)

        channel = c.task_consumer.channel
        channel.reset_mock()
        c.task_consumer._basic_consume.reset_mock()

        c.set_queue_prefetch_limit('b', 2)

        channel.basic_cancel.assert_called_once_with('tag-b', nowait=True)
        channel.basic_qos.assert_called_once_with(0, 2, False)
        # only b was re-consumed; a keeps its tag/window
        consumed = [
            one_call.args[0].name
            for one_call in c.task_consumer._basic_consume.call_args_list
        ]
        assert consumed == ['b']
        assert c.task_consumer._active_tags['a'] == 'tag-a'

    def test_illegal_runtime_value_refused_honestly(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a', 'b')
        c.qos = QoS(lambda **kw: None, 16)
        with pytest.raises(ValueError):
            c.set_queue_prefetch_limit('a', -5)
        # still on legacy mode, the broker never received the switch
        assert c.qos.per_queue_enabled is False

    def test_unknown_queue_refused(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 16)
        with pytest.raises(ValueError):
            c.set_queue_prefetch_limit('ghost', 3)

    def test_clear_without_existing_cap_refused(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 16)
        with pytest.raises(ValueError):
            c.set_queue_prefetch_limit('a', None)

    def test_non_amqp_transport_refused(self, app):
        c = self.get_consumer(app)
        c.connection.transport.driver_type = 'redis'
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 16)
        with pytest.raises(ValueError):
            c.set_queue_prefetch_limit('a', 4)
        assert c.qos.per_queue_enabled is False

    def test_held_counts_only_prefetched_not_handed_messages(self, app):
        class _Req:  # weak-referenceable stand-in for a task Request
            def __init__(self, message):
                self.message = message

        reset_state()
        try:
            c = self.get_consumer(app)
            c.task_consumer = self.mock_task_consumer('a', 'b')
            c.qos = QoS(lambda **kw: None, 16, queue_limits={'a': 4})
            c.enable_per_queue_qos()

            msg_a1 = SimpleNamespace(delivery_info={'routing_key': 'a'})
            msg_a2 = SimpleNamespace(delivery_info={'routing_key': 'a'})
            req_a1 = _Req(msg_a1)
            req_a2 = _Req(msg_a2)
            scheduled_requests.add(req_a1)
            bucket = SimpleNamespace(contents=deque([(req_a2, 1)]))
            c.task_buckets = {'t': bucket}

            assert c._held_prefetch_for_queue('a') == 2
            assert c._held_prefetch_for_queue('b') == 0
        finally:
            reset_state()


class test_tasks_startup(ConsumerCase):

    def _mock_startup_consumer(self, app, driver_type='amqp', limits=None):
        c = self.get_consumer(app)
        c.connection.transport.driver_type = driver_type
        c.connection.qos_semantics_matches_spec = False
        c.connection.default_channel = Mock()
        c.update_strategies = Mock()
        c.on_decode_error = Mock()
        c.app.conf.worker_eta_task_limit = None
        c.app.conf.worker_detect_quorum_queues = False
        c.app.conf.worker_disable_prefetch = False
        if limits is not None:
            c.queue_prefetch_limits = validate_queue_prefetch_limits(limits)
        tc = self.mock_task_consumer('a', 'b')
        c.app.amqp.TaskConsumer = Mock(return_value=tc)
        return c, tc

    def test_legacy_startup_unchanged_without_caps(self, app):
        from celery.worker.consumer.tasks import Tasks
        c, tc = self._mock_startup_consumer(app, limits=None)
        Tasks(c).start(c)
        # exactly the legacy single global basic_qos call
        c.connection.default_channel.basic_qos.assert_called_once_with(
            0, 16, True,
        )
        tc.channel.basic_qos.assert_not_called()
        assert c.qos.per_queue_enabled is False

    def test_per_queue_startup_binds_qos_before_consume(self, app):
        from celery.worker.consumer.tasks import Tasks
        c, tc = self._mock_startup_consumer(app, limits={'a': 4})
        Tasks(c).start(c)
        calls = [x.args for x in tc.channel.basic_qos.call_args_list]
        assert calls == [(0, 4, False), (0, 16, False)]
        # both queues consumed, the last one synchronously like kombu
        assert [x.args[0].name
                for x in tc._basic_consume.call_args_list] == ['a', 'b']
        assert tc._basic_consume.call_args_list[1].kwargs == {'nowait': False}
        c.connection.default_channel.basic_qos.assert_not_called()

    def test_non_amqp_startup_with_caps_raises(self, app):
        from celery.worker.consumer.tasks import Tasks
        c, _tc = self._mock_startup_consumer(
            app, driver_type='redis', limits={'a': 4},
        )
        with pytest.raises(ImproperlyConfigured):
            Tasks(c).start(c)

    def test_zero_multiplier_with_caps_raises(self, app):
        from celery.worker.consumer.tasks import Tasks
        c, _tc = self._mock_startup_consumer(app, limits={'a': 4})
        c.initial_prefetch_count = 0
        with pytest.raises(ImproperlyConfigured):
            Tasks(c).start(c)


class test_runtime_cap_reconnect(ConsumerCase):

    def test_reconnect_reduction_keeps_caps(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a', 'b')
        c.qos = QoS(lambda **kw: None, 16)
        c.set_queue_prefetch_limit('a', 4)

        # Emulates the quorum-mode flag that used to force the "skip"
        # branch which resets the prefetch state.
        c.qos_global = False
        c.app.conf.worker_enable_prefetch_count_reduction = True
        c.prefetch_multiplier = 4
        c.pool.num_processes = 4

        class _Active:
            def __len__(self):
                return 2

        with patch(
                'celery.worker.consumer.consumer.active_requests', _Active()):
            c.on_connection_error_after_connected(ConnectionError())
        assert c.queue_prefetch_limits == {'a': 4}
        assert c.initial_prefetch_count > 0


# ---------------------------------------------------------------------------
# remote control commands
# ---------------------------------------------------------------------------

class test_panel_queue_prefetch(ConsumerCase):

    def panel(self, app, consumer):
        panel_state = AttributeDict({
            'app': app, 'hostname': 'test@celery', 'tset': set,
            'consumer': consumer,
        })
        return app.control.mailbox.Node(
            hostname='test@celery', state=panel_state, handlers=Panel.data,
        )

    def test_inspect_reports_targets_and_actuals(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a', 'b')
        c.qos = QoS(lambda **kw: None, 16)
        c.set_queue_prefetch_limit('a', 4)
        reply = self.panel(app, c).handle('queue_prefetch')
        assert reply['mode'] == 'per-queue'
        assert reply['queues']['a']['target'] == 4
        assert reply['queues']['b']['target'] == 16

    def test_set_command_returns_ok_report(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 16)
        reply = self.panel(app, c).handle(
            'set_queue_prefetch', {'queue': 'a', 'limit': 4},
        )
        assert reply['ok']['queue'] == 'a'
        assert reply['ok']['actual'] == 4

    def test_set_command_refuses_bad_value_with_error(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 16)
        reply = self.panel(app, c).handle(
            'set_queue_prefetch', {'queue': 'a', 'limit': -1},
        )
        assert 'error' in reply
        assert c.qos.per_queue_enabled is False

    def test_set_command_requires_queue(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 16)
        reply = self.panel(app, c).handle(
            'set_queue_prefetch', {'queue': None, 'limit': 4},
        )
        assert 'error' in reply

    def test_inspect_legacy_mode(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a')
        c.qos = QoS(lambda **kw: None, 12)
        reply = self.panel(app, c).handle('queue_prefetch')
        assert reply == {
            'mode': 'global', 'prefetch_count': 12, 'queues': {},
        }


# ---------------------------------------------------------------------------
# scaling
# ---------------------------------------------------------------------------

class test_scaling_preserves_caps(ConsumerCase):

    def test_update_prefetch_count_recomputes_targets(self, app):
        c = self.get_consumer(app)
        c.task_consumer = self.mock_task_consumer('a', 'b')
        c.queue_prefetch_limits = validate_queue_prefetch_limits({'a': 4})
        c.qos = QoS(lambda **kw: None, 16,
                    queue_limits=c.queue_prefetch_limits)
        c.enable_per_queue_qos()
        c.qos.activate_queue('a')
        c.qos.activate_queue('b')
        c.prefetch_multiplier = 4
        c.pool.num_processes = 8
        c._update_prefetch_count(4)
        assert c.initial_prefetch_count == 32
        c.qos.flush()
        info = c.qos.info()['queues']
        # capped queue untouched, uncapped one grew to 32
        assert info['a']['target'] == 4
        assert info['b']['target'] == 32
        # caps still live in the persistent mapping
        assert c.queue_prefetch_limits == {'a': 4}
