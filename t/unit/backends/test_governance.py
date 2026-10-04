"""Tests for storage-side result backend capacity governance."""
import os
import tempfile
import threading
from datetime import timedelta

import pytest

import t.skip
from celery import states, uuid
from celery.backends.base import KeyValueStoreBackend
from celery.backends.cache import _DUMMY_CLIENT_CACHE, CacheBackend
from celery.exceptions import ImproperlyConfigured


class GovernedKVBackend(KeyValueStoreBackend):
    """Dict backed KV backend with the governance enumeration primitive."""

    supports_capacity_governance = True

    def __init__(self, app, *args, **kwargs):
        self.db = {}
        self._db_lock = threading.RLock()
        super().__init__(app, *args, **kwargs)

    def get(self, key):
        with self._db_lock:
            return self.db.get(key)

    def set(self, key, value):
        with self._db_lock:
            self.db[key] = value

    def _set_with_state(self, key, value, state):
        with self._db_lock:
            self.db[key] = value

    def mget(self, keys):
        with self._db_lock:
            return [self.db.get(k) for k in keys]

    def delete(self, key):
        with self._db_lock:
            self.db.pop(key, None)

    def _iter_result_keys(self):
        with self._db_lock:
            return sorted(
                k for k in list(self.db) if k.startswith(self.task_keyprefix))


def enable_governance(app, retention=None, max_results=None, max_bytes=None):
    app.conf.result_governance_enabled = True
    app.conf.result_governance_retention = retention
    app.conf.result_governance_max_results = max_results
    app.conf.result_governance_max_bytes = max_bytes


def install_fake_clock(backend):
    """Make app.now() strictly increasing so eviction ordering is stable."""
    now = [backend.app.now()]
    lock = threading.Lock()

    def tick():
        with lock:
            now[0] += timedelta(milliseconds=1)
            return now[0]

    backend.app.now = tick
    return now


def raw_store(backend, task_id, status=states.SUCCESS, age=None,
              result=None, group_id=None, pad=0):
    """Write a result key directly, bypassing the regular write path."""
    meta = {
        'status': status,
        'result': result if result is not None else ('x' * pad),
        'traceback': None,
        'children': [],
        'date_done': None,
        'task_id': task_id,
    }
    if status in states.READY_STATES:
        when = backend.app.now()
        if age is not None:
            when -= timedelta(seconds=age)
        meta['date_done'] = when.isoformat()
    if group_id:
        meta['group_id'] = group_id
    payload = backend.encode(meta)
    backend.set(backend.get_key_for_task(task_id), payload)
    return payload


class test_governance_configuration:

    def test_disabled_by_default(self):
        b = GovernedKVBackend(app=self.app)
        assert b.governance_enabled is False
        assert b.governance_retention == {}
        assert b.governance_max_results is None
        assert b.governance_max_bytes is None

    def test_unsupported_backend_rejected_at_startup(self):
        class PlainKVBackend(KeyValueStoreBackend):
            def get(self, key):
                pass

            def set(self, key, value):
                pass

            def delete(self, key):
                pass

        enable_governance(self.app, max_results=10)
        with pytest.raises(ImproperlyConfigured, match='governance'):
            PlainKVBackend(app=self.app)

    def test_unsupported_backend_without_flag_still_works(self):
        class PlainKVBackend(KeyValueStoreBackend):
            def get(self, key):
                pass

            def set(self, key, value):
                pass

            def delete(self, key):
                pass

        PlainKVBackend(app=self.app)

    @pytest.mark.parametrize('bad', [
        'not-a-mapping', [('SUCCESS', 10)], 60,
    ])
    def test_retention_must_be_mapping(self, bad):
        enable_governance(self.app, retention=bad)
        with pytest.raises(ImproperlyConfigured):
            GovernedKVBackend(app=self.app)

    @pytest.mark.parametrize('state', ['PENDING', 'STARTED', 'NO-SUCH-STATE'])
    def test_retention_only_accepts_final_states(self, state):
        enable_governance(self.app, retention={state: 10})
        with pytest.raises(ImproperlyConfigured, match='state'):
            GovernedKVBackend(app=self.app)

    @pytest.mark.parametrize('value', [-1, -1.5, '60', object()])
    def test_retention_bad_values_rejected(self, value):
        enable_governance(self.app, retention={'SUCCESS': value})
        with pytest.raises(ImproperlyConfigured):
            GovernedKVBackend(app=self.app)

    def test_retention_timedelta_and_seconds_accepted(self):
        enable_governance(
            self.app,
            retention={'SUCCESS': 60.0, 'FAILURE': timedelta(days=1)})
        b = GovernedKVBackend(app=self.app)
        assert b.governance_retention['SUCCESS'] == 60.0
        assert b.governance_retention['FAILURE'] == 86400.0

    @pytest.mark.parametrize('value', [-1, 1.5, True, False, '10'])
    def test_bad_limits_rejected(self, value):
        enable_governance(self.app, max_results=value)
        with pytest.raises(ImproperlyConfigured):
            GovernedKVBackend(app=self.app)
        self.app.conf.result_governance_max_results = None
        self.app.conf.result_governance_max_bytes = value
        with pytest.raises(ImproperlyConfigured):
            GovernedKVBackend(app=self.app)

    def test_zero_and_none_limits_accepted(self):
        enable_governance(self.app, max_results=0, max_bytes=0)
        b = GovernedKVBackend(app=self.app)
        assert b.governance_max_results == 0
        assert b.governance_max_bytes == 0

    def test_memcached_backend_rejects_governance(self):
        enable_governance(self.app, max_results=10)
        with pytest.raises(ImproperlyConfigured, match='memcached'):
            CacheBackend(app=self.app, url='memcached://127.0.0.1:11211')


class test_governance_disabled:

    def setup_method(self):
        self.b = GovernedKVBackend(app=self.app)

    def test_write_path_unchanged(self):
        tid = uuid()
        self.b.mark_as_done(tid, 'hello')
        assert self.b.get_result(tid) == 'hello'
        assert self.b.get_state(tid) == states.SUCCESS

    def test_cleanup_is_legacy_noop(self):
        assert self.b.cleanup() is None

    def test_inspect_requires_enablement(self):
        with pytest.raises(ImproperlyConfigured):
            self.b.inspect_results()

    def test_manual_cleanup_requires_enablement(self):
        with pytest.raises(ImproperlyConfigured):
            self.b.cleanup_results()

    def test_enumeration_not_touched_on_write_path(self):
        def boom():
            raise AssertionError('governance scan while disabled')

        self.b._iter_result_keys = boom
        self.b.mark_as_done(uuid(), 1)
        self.b.mark_as_started(uuid())
        self.b.get_state(uuid())


class test_capacity_enforcement:

    def setup_method(self):
        enable_governance(self.app, max_results=3)
        self.b = GovernedKVBackend(app=self.app)
        install_fake_clock(self.b)

    def test_count_cap_evicts_oldest(self):
        ids = [uuid() for _ in range(6)]
        for tid in ids:
            self.b.mark_as_done(tid, 42)
        inventory = self.b.inspect_results()
        remaining = set(invocation.task_id for invocation in inventory.items)
        assert remaining == set(ids[-3:])
        # Evicted keys read back as PENDING (unfinished state).
        for tid in ids[:-3]:
            assert self.b.get_state(tid) == states.PENDING
            assert self.b.get_result(tid) is None

    def test_capacity_triggers_with_single_limit(self):
        # Only max_results configured: bytes dimension unbounded.
        for i in range(5):
            self.b.mark_as_done(uuid(), 'v' * 500)
        inventory = self.b.inspect_results()
        assert inventory.total_count == 3

    def test_zero_cap_stops_new_results(self):
        self.app.conf.result_governance_max_results = 0
        b = GovernedKVBackend(app=self.app)
        tid = uuid()
        b.mark_as_done(tid, 'never stored')
        assert b.get_state(tid) == states.PENDING
        assert b.task_result_exists(tid) is False

    def test_zero_cap_allows_updates_of_existing_keys(self):
        self.app.conf.result_governance_max_results = 0
        b = GovernedKVBackend(app=self.app)
        tid = uuid()
        raw_store(b, tid, status=states.STARTED)
        b.mark_as_done(tid, 'now finished')
        assert b.get_state(tid) == states.SUCCESS
        assert b.get_result(tid) == 'now finished'

    def test_zero_byte_cap_stops_new_results(self):
        self.app.conf.result_governance_max_results = None
        self.app.conf.result_governance_max_bytes = 0
        b = GovernedKVBackend(app=self.app)
        tid = uuid()
        b.mark_as_done(tid, 'x')
        assert b.get_state(tid) == states.PENDING

    def test_bytes_cap_evicts_oldest(self):
        self.app.conf.result_governance_max_results = None
        # Limit chosen so exactly the two newest (largest) values fit.
        sizes = [10, 20, 30, 40, 50]
        payloads = [raw_store(self.b, uuid(), pad=size)
                    for size in sizes]
        cap = len(payloads[-1]) + len(payloads[-2])
        self.b.governance_max_bytes = cap
        self.b.governance_max_results = None
        report = self.b.enforce_result_capacity()
        assert report.deleted_count == 3
        inventory = self.b.inspect_results()
        assert inventory.total_count == 2
        assert inventory.total_bytes <= cap

    def test_repeated_enforcement_is_idempotent(self):
        ids = [uuid() for _ in range(6)]
        for tid in ids:
            raw_store(self.b, tid, states.SUCCESS)
        first = self.b.enforce_result_capacity()
        second = self.b.enforce_result_capacity()
        assert first.deleted_count == 3
        assert second.deleted_count == 0
        assert second.freed_bytes == 0
        assert self.b.inspect_results().total_count == 3


class test_retention_tiers:

    def setup_method(self):
        enable_governance(self.app, retention={
            states.SUCCESS: 60.0,
            states.FAILURE: 3600.0,
        })
        self.b = GovernedKVBackend(app=self.app)

    def test_state_specific_retention(self):
        old_success = uuid()
        fresh_success = uuid()
        old_failure = uuid()
        raw_store(self.b, old_success, states.SUCCESS, age=120)
        raw_store(self.b, fresh_success, states.SUCCESS, age=10)
        raw_store(self.b, old_failure, states.FAILURE, age=120)

        report = self.b.cleanup()
        assert report.retention_deleted == 1
        assert self.b.get_state(old_success) == states.PENDING
        assert self.b.get_state(fresh_success) == states.SUCCESS
        assert self.b.get_state(old_failure) == states.FAILURE

    def test_retention_with_timedelta_policy(self):
        self.app.conf.result_governance_retention = {
            states.REVOKED: timedelta(seconds=30)}
        b = GovernedKVBackend(app=self.app)
        old_revoked, fresh_revoked = uuid(), uuid()
        raw_store(b, old_revoked, states.REVOKED, age=60)
        raw_store(b, fresh_revoked, states.REVOKED, age=1)
        b.enforce_result_retention()
        assert b.get_state(old_revoked) == states.PENDING
        assert b.get_state(fresh_revoked) == states.REVOKED

    def test_retention_and_capacity_pick_key_at_most_once(self):
        self.app.conf.result_governance_max_results = 2
        b = GovernedKVBackend(app=self.app)
        ids = [uuid() for _ in range(5)]
        for tid in ids:
            raw_store(b, tid, states.SUCCESS, age=600)
        report = b.cleanup_results()
        # Retention expiry runs first and removes every expired key; the
        # capacity pass never re-selects (or double counts) a deleted key.
        assert report.deleted_count == 5
        assert report.retention_deleted == 5
        assert report.capacity_deleted == 0
        assert len(set(report.keys)) == len(report.keys)
        assert b.inspect_results().total_count == 0

    def test_retention_then_capacity_evict_distinct_keys(self):
        self.app.conf.result_governance_max_results = 1
        b = GovernedKVBackend(app=self.app)
        old_ids = [uuid(), uuid()]
        fresh_ids = [uuid(), uuid()]
        for tid in old_ids:
            raw_store(b, tid, states.SUCCESS, age=600)
        for tid in fresh_ids:
            raw_store(b, tid, states.SUCCESS, age=1)
        report = b.cleanup_results()
        # Two old keys removed by retention; capacity then trims the two
        # fresh survivors down to the single newest one.
        assert report.retention_deleted == 2
        assert report.capacity_deleted == 1
        assert report.deleted_count == 3
        assert b.inspect_results().total_count == 1
        assert b.get_state(fresh_ids[-1]) == states.SUCCESS


class test_protection:

    def setup_method(self):
        enable_governance(
            self.app,
            retention={states.SUCCESS: 1.0},
            max_results=1)
        self.b = GovernedKVBackend(app=self.app)

    def test_unfinished_results_are_protected(self):
        started = uuid()
        raw_store(self.b, started, states.STARTED, age=100)
        report = self.b.cleanup_results()
        assert report.deleted_count == 0
        assert self.b.get_state(started) == states.STARTED
        inventory = self.b.inspect_results(protected=True)
        assert inventory.items[0].protection_reason == 'unfinished'

    def test_group_members_protected_while_group_metadata_exists(self):
        gid = uuid()
        member = uuid()
        outsider = uuid()
        self.app.GroupResult(id=gid, results=[]).save(backend=self.b)
        raw_store(self.b, member, states.SUCCESS, age=100, group_id=gid)
        raw_store(self.b, outsider, states.SUCCESS, age=100)

        report = self.b.cleanup_results()
        assert report.deleted_count == 1
        assert self.b.get_state(member) == states.SUCCESS
        assert self.b.get_state(outsider) == states.PENDING

        inventory = self.b.inspect_results()
        member_item = next(
            item for item in inventory.items if item.task_id == member)
        assert member_item.protected is True
        assert member_item.protection_reason == 'group'

        # Once the group metadata is gone from the store, the member is
        # governed like any other result.
        self.b.delete_group(gid)
        self.b.cleanup_results()
        assert self.b.get_state(member) == states.PENDING

    def test_chord_and_group_keys_are_never_scanned(self):
        # incr style chord counter, redis style sub-keys, group metadata
        self.b.set(self.b.get_key_for_chord('g1'), b'1')
        self.b.set(self.b.get_key_for_group('g2'), b'group-meta')
        self.b.set(self.b.get_key_for_group('g3', '.t'), b'1')
        self.b.set(self.b.get_key_for_group('g3', '.j'), b'1')
        self.b.set(self.b.get_key_for_group('g3', '.s'), b'1')
        raw_store(self.b, uuid(), states.SUCCESS, age=100)

        self.b.cleanup_results()
        assert self.b.get(self.b.get_key_for_chord('g1')) == b'1'
        assert self.b.get(self.b.get_key_for_group('g2')) == b'group-meta'
        assert self.b.get(self.b.get_key_for_group('g3', '.t')) == b'1'
        assert self.b.get(self.b.get_key_for_group('g3', '.j')) == b'1'
        assert self.b.get(self.b.get_key_for_group('g3', '.s')) == b'1'

    def test_protection_ignores_memory_markers(self):
        # A group id on the meta means nothing unless the group metadata
        # key is actually present in the store.
        orphan = uuid()
        raw_store(self.b, orphan, states.SUCCESS, age=100, group_id='ghost')
        self.b.cleanup_results()
        assert self.b.get_state(orphan) == states.PENDING


class test_inventory:

    def setup_method(self):
        enable_governance(self.app, retention={
            states.SUCCESS: 600.0, states.FAILURE: 3600.0})
        self.b = GovernedKVBackend(app=self.app)
        self.ids = {
            'old_success': uuid(),
            'fresh_success': uuid(),
            'failure': uuid(),
        }
        raw_store(self.b, self.ids['old_success'], states.SUCCESS, age=300)
        raw_store(self.b, self.ids['fresh_success'], states.SUCCESS, age=10)
        raw_store(self.b, self.ids['failure'], states.FAILURE, age=300)

    def test_counts_and_bytes_per_state(self):
        inventory = self.b.inspect_results()
        assert inventory.total_count == 3
        assert inventory.by_state[states.SUCCESS].count == 2
        assert inventory.by_state[states.FAILURE].count == 1
        assert inventory.by_state[states.FAILURE].bytes > 0
        assert sum(u.bytes for u in inventory.by_state.values()) \
            == inventory.total_bytes

    def test_filter_by_state(self):
        inventory = self.b.inspect_results(states=states.FAILURE)
        assert inventory.total_count == 1
        assert inventory.items[0].task_id == self.ids['failure']

    def test_filter_by_task_ids(self):
        target = self.ids['fresh_success']
        inventory = self.b.inspect_results(task_ids=[target, 'missing'])
        assert [item.task_id for item in inventory.items] == [target]

    def test_filter_by_time_window(self):
        now = self.b.app.now()
        inventory = self.b.inspect_results(
            since=now - timedelta(seconds=60))
        assert {item.task_id for item in inventory.items} == {
            self.ids['fresh_success']}
        inventory = self.b.inspect_results(
            until=now - timedelta(seconds=60))
        assert {item.task_id for item in inventory.items} == {
            self.ids['old_success'], self.ids['failure']}
        # epoch numbers and ISO strings are accepted too
        epoch = (now - timedelta(seconds=60)).timestamp()
        assert self.b.inspect_results(since=epoch).total_count == 1
        iso = (now - timedelta(seconds=60)).isoformat()
        assert self.b.inspect_results(since=iso).total_count == 1

    def test_filter_by_tier(self):
        inventory = self.b.inspect_results(tier=states.SUCCESS)
        assert inventory.total_count == 2
        for item in inventory.items:
            assert item.status == states.SUCCESS
            assert item.retention_seconds == 600.0
            assert item.expires_at is not None
        with pytest.raises(ValueError):
            self.b.inspect_results(tier=states.REVOKED)

    def test_pagination_oldest_first(self):
        inventory = self.b.inspect_results(limit=1, offset=1)
        assert len(inventory.items) == 1
        assert inventory.total_count == 3  # summary is not paginated
        full = self.b.inspect_results()
        assert inventory.items[0].task_id == full.items[1].task_id
        assert full.items[0].task_id == self.ids['old_success']

    def test_inventory_is_read_only(self):
        before = dict(self.b.db)

        # No write primitives may be used at all (no transient lock keys).
        def refuse_write(*args, **kwargs):
            raise AssertionError('inspect_results issued a write operation')

        self.b.set = refuse_write
        self.b._set_with_state = refuse_write
        self.b.delete = refuse_write
        self.b.inspect_results()
        assert self.b.db == before
        # The local result cache is not used or populated.
        self.b._cache.clear()
        self.b.inspect_results()
        assert not self.b.is_cached(self.ids['failure'])


class test_manual_cleanup:

    def setup_method(self):
        enable_governance(self.app, retention={states.SUCCESS: 60.0})
        self.b = GovernedKVBackend(app=self.app)

    def test_dry_run_reports_but_does_not_delete(self):
        ids = [uuid() for _ in range(3)]
        for tid in ids:
            raw_store(self.b, tid, states.SUCCESS, age=120)
        snapshot = dict(self.b.db)
        report = self.b.cleanup_results(dry_run=True)
        assert report.deleted_count == 0
        assert len(report.keys) == 3
        assert self.b.db == snapshot

    def test_filter_deletion_by_task_id(self):
        keep, drop = uuid(), uuid()
        raw_store(self.b, keep, states.SUCCESS, age=120)
        raw_store(self.b, drop, states.SUCCESS, age=120)
        report = self.b.cleanup_results(task_ids=[drop])
        assert report.keys == [drop]
        assert self.b.get_state(keep) == states.SUCCESS
        assert self.b.get_state(drop) == states.PENDING

    def test_filter_deletion_by_state(self):
        success, failure = uuid(), uuid()
        raw_store(self.b, success, states.SUCCESS, age=120)
        raw_store(self.b, failure, states.FAILURE, age=120)
        report = self.b.cleanup_results(states=states.FAILURE)
        # FAILURE has no tier configured and no capacity ceiling, so the
        # filtered run removes nothing.
        assert report.deleted_count == 0
        report = self.b.cleanup_results(states=states.SUCCESS)
        assert report.deleted_count == 1

    def test_invalid_time_window_rejected(self):
        with pytest.raises(ValueError):
            self.b.inspect_results(
                since=self.b.app.now(),
                until=self.b.app.now() - timedelta(seconds=1))


class test_cache_invalidation:

    def test_stale_cached_value_removed_on_eviction(self):
        self.app.conf.result_cache_max = 100
        enable_governance(self.app, max_results=1)
        b = GovernedKVBackend(app=self.app)
        install_fake_clock(b)
        first, second = uuid(), uuid()
        b.mark_as_done(first, 1)
        assert b.get_state(first) == states.SUCCESS
        assert b.is_cached(first)
        b.mark_as_done(second, 2)
        assert not b.is_cached(first)
        # Even though SUCCESS was cached, the evicted key now reads PENDING.
        assert b.get_state(first) == states.PENDING
        assert b.get_state(second) == states.SUCCESS


class test_concurrency:

    def test_concurrent_writers_keep_capacity(self):
        enable_governance(self.app, max_results=40)
        b = GovernedKVBackend(app=self.app)
        errors = []

        def worker(prefix):
            try:
                for i in range(25):
                    b.mark_as_done(f'{prefix}-{i}-{uuid()}', i)
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(p,))
                   for p in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert not errors
        report = b.cleanup_results()
        assert report.inspected_count <= 40

    def test_cleanup_concurrent_with_group_completion(self):
        enable_governance(self.app, max_results=10)
        b = GovernedKVBackend(app=self.app)
        errors = []
        stop = threading.Event()

        def cleanup_loop():
            try:
                while not stop.is_set():
                    b.cleanup_results()
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        def group_loop():
            try:
                for i in range(50):
                    gid = f'group-{i}'
                    b.save_group(gid, b.app.GroupResult(
                        id=gid, results=[]))
                    raw_store(b, f'member-{i}', states.SUCCESS,
                              group_id=gid)
                    b.delete_group(gid)
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        threads = [
            threading.Thread(target=cleanup_loop),
            threading.Thread(target=group_loop),
        ]
        for thread in threads:
            thread.start()
        threads[1].join()
        stop.set()
        threads[0].join()
        assert not errors


class FakeRedisPipeline:
    def __init__(self, client):
        self.client = client
        self.steps = []

    def __getattr__(self, name):
        def add_step(*args, **kwargs):
            self.steps.append((name, args, kwargs))
            return self
        return add_step

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def execute(self):
        return [
            getattr(self.client, name)(*args, **kwargs)
            for name, args, kwargs in self.steps
        ]


class FakeRedis:
    def __init__(self, **kwargs):
        self.data = {}
        self._lock = threading.RLock()

    def get(self, key):
        with self._lock:
            return self.data.get(key)

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def keys(self, pattern):
        prefix = pattern[:-1]
        with self._lock:
            return [k for k in list(self.data) if k.startswith(prefix)]

    def scan_iter(self, match=None, count=None):
        for key in self.keys(match):
            yield key

    def set(self, key, value, nx=False, px=None, ex=None):
        with self._lock:
            if nx and key in self.data:
                return None
            self.data[key] = value
            return True

    def setex(self, key, ttl, value):
        return self.set(key, value)

    def delete(self, *keys):
        removed = 0
        with self._lock:
            for key in keys:
                if self.data.pop(key, None) is not None:
                    removed += 1
        return removed

    def pipeline(self):
        return FakeRedisPipeline(self)

    def publish(self, *args, **kwargs):
        return 0

    def eval(self, script, numkeys, *args):
        key, token = args[0], args[1]
        with self._lock:
            if self.data.get(key) == token:
                self.data.pop(key, None)
                return 1
            return 0

    def incr(self, key, delta=1):
        with self._lock:
            value = int(self.data.get(key, 0)) + delta
            self.data[key] = value
            return value

    def expire(self, key, ttl):
        return True


class FakeRedisModule:
    StrictRedis = FakeRedis

    class ConnectionPool:
        def __init__(self, **kwargs):
            pass

    class UnixDomainSocketConnection:
        def __init__(self, **kwargs):
            pass

    SSLConnection = None

    class CredentialProvider:
        pass

    sentinel = None


class test_RedisGovernance:

    def _backend(self, **governance):
        from celery.backends.redis import RedisBackend

        class _RedisBackend(RedisBackend):
            redis = FakeRedisModule

        enable_governance(self.app, **governance)
        return _RedisBackend(app=self.app)

    def test_count_cap_end_to_end(self):
        b = self._backend(max_results=2)
        install_fake_clock(b)
        ids = [uuid() for _ in range(5)]
        for tid in ids:
            b.mark_as_done(tid, 'result')
        remaining = {item.task_id for item in b.inspect_results().items}
        assert remaining == set(ids[-2:])
        assert b.get_state(ids[0]) == states.PENDING

    def test_zero_cap_drops_writes(self):
        b = self._backend(max_results=0)
        tid = uuid()
        b.mark_as_done(tid, 1)
        assert b.get_state(tid) == states.PENDING

    def test_inventory_writes_no_lock_key(self):
        b = self._backend(max_results=2)
        b.mark_as_done(uuid(), 1)
        before = dict(b.client.data)
        b.inspect_results()
        assert b.client.data == before
        lock_key = b._get_key_for(
            b.group_keyprefix, b.governance_lock_suffix)
        assert lock_key not in b.client.data

    def test_storage_lock_released(self):
        b = self._backend(max_results=1)
        install_fake_clock(b)
        for i in range(3):
            b.mark_as_done(uuid(), i)
        b.cleanup_results()
        lock_key = b._get_key_for(
            b.group_keyprefix, b.governance_lock_suffix)
        assert lock_key not in b.client.data

    def test_group_protection_and_chord_keys(self):
        b = self._backend(retention={states.SUCCESS: 1.0}, max_results=1)
        gid = uuid()
        member, outsider = uuid(), uuid()
        b.app.GroupResult(id=gid, results=[]).save(backend=b)
        raw_store(b, member, states.SUCCESS, age=100, group_id=gid)
        raw_store(b, outsider, states.SUCCESS, age=100)
        b.set(b.get_key_for_group(gid, '.t'), b'1')
        b.set(b.get_key_for_group(gid, '.j'), b'1')
        b.cleanup_results()
        assert b.get_state(member) == states.SUCCESS
        assert b.get_state(outsider) == states.PENDING
        assert b.get(b.get_key_for_group(gid, '.t')) == b'1'
        assert b.get(b.get_key_for_group(gid, '.j')) == b'1'
        b.delete_group(gid)
        b.cleanup_results()
        assert b.get_state(member) == states.PENDING


class test_MemoryCacheGovernance:

    def setup_method(self):
        _DUMMY_CLIENT_CACHE.clear()

    def _backend(self, **governance):
        enable_governance(self.app, **governance)
        return CacheBackend(app=self.app, url='memory://')

    def test_capacity_enforced(self):
        b = self._backend(max_results=2)
        ids = [uuid() for _ in range(4)]
        for tid in ids:
            b.mark_as_done(tid, 1)
        assert b.inspect_results().total_count == 2

    def test_zero_cap(self):
        b = self._backend(max_results=0)
        tid = uuid()
        b.mark_as_done(tid, 1)
        assert b.get_state(tid) == states.PENDING

    def test_inventory_stats(self):
        b = self._backend(retention={states.SUCCESS: 60})
        raw_store(b, uuid(), states.SUCCESS, age=100)
        raw_store(b, uuid(), states.FAILURE, age=100)
        inventory = b.inspect_results()
        assert inventory.total_count == 2
        assert set(inventory.by_state) == {states.SUCCESS, states.FAILURE}


@t.skip.if_win32
class test_FilesystemGovernance:

    def setup_method(self):
        self.directory = tempfile.mkdtemp()
        self.url = 'file://' + self.directory

    def _backend(self, **governance):
        enable_governance(self.app, **governance)
        from celery.backends.filesystem import FilesystemBackend
        return FilesystemBackend(app=self.app, url=self.url)

    def test_capacity_enforced(self):
        b = self._backend(max_results=2)
        install_fake_clock(b)
        ids = [uuid() for _ in range(4)]
        for tid in ids:
            b.mark_as_done(tid, 1)
        assert b.inspect_results().total_count == 2
        assert b.get_state(ids[0]) == states.PENDING

    def test_retention_cleanup(self):
        b = self._backend(retention={states.SUCCESS: 1.0})
        old, fresh = uuid(), uuid()
        raw_store(b, old, states.SUCCESS, age=10)
        raw_store(b, fresh, states.SUCCESS, age=0)
        b.cleanup()
        assert b.get_state(old) == states.PENDING
        assert b.get_state(fresh) == states.SUCCESS

    def test_lock_file_removed(self):
        b = self._backend(max_results=1)
        b.mark_as_done(uuid(), 1)
        b.cleanup_results()
        assert b.governance_lock_name.decode() not in \
            os.listdir(self.directory)
