"""Tests for the Redis native group roster and chord aggregation.

A small in-memory fake of the redis client implements just the commands the
roster code paths use (strings, hashes, sorted sets and pipelines). The
pipeline executes commands sequentially -- every test here is single
threaded, and the production code relies on redis MULTI/EXEC for the atomic
guarantees.
"""
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from celery import states
from celery.backends.redis import RedisBackend
from celery.result import ROSTER_PENDING, ROSTER_RECOVERING, ROSTER_SENT


class FakePipeline:
    def __init__(self, client):
        self.client = client
        self.steps = []

    def __getattr__(self, attr):
        def add_step(*args, **kwargs):
            self.steps.append((attr, args, kwargs))
            return self

        return add_step

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self):
        return [
            getattr(self.client, name)(*args, **kwargs)
            for name, args, kwargs in self.steps
        ]


class FakeRedis:
    def __init__(self, *args, **kwargs):
        self.strings = {}
        self.hashes = {}
        self.zsets = {}
        self.expiry = {}

    def pipeline(self, transaction=True):
        return FakePipeline(self)

    # strings
    def get(self, key):
        return self.strings.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        if ex is not None:
            self.expiry[key] = ex
        return True

    def setex(self, key, ex, value):
        self.strings[key] = value
        self.expiry[key] = ex
        return True

    def delete(self, key):
        found = key in self.strings or key in self.hashes or key in self.zsets
        self.strings.pop(key, None)
        self.hashes.pop(key, None)
        self.zsets.pop(key, None)
        self.expiry.pop(key, None)
        return int(found)

    def exists(self, key):
        return int(key in self.strings or key in self.hashes
                   or key in self.zsets)

    def expire(self, key, ex):
        if self.exists(key):
            self.expiry[key] = ex
        return True

    def publish(self, key, value):
        return 0

    # hashes
    def hset(self, name, key=None, value=None, mapping=None):
        table = self.hashes.setdefault(name, {})
        added = 0
        if mapping:
            for field, val in mapping.items():
                if field not in table:
                    added += 1
                table[field] = val
        if key is not None:
            if key not in table:
                added += 1
            table[key] = value
        return added

    def hsetnx(self, name, key, value):
        table = self.hashes.setdefault(name, {})
        if key in table:
            return 0
        table[key] = value
        return 1

    def hget(self, name, key):
        return self.hashes.get(name, {}).get(key)

    def hgetall(self, name):
        return dict(self.hashes.get(name, {}))

    def hmget(self, name, keys):
        table = self.hashes.get(name, {})
        return [table.get(k) for k in keys]

    def hexists(self, name, key):
        return int(key in self.hashes.get(name, {}))

    # sorted sets
    def zadd(self, name, mapping, nx=False):
        table = self.zsets.setdefault(name, {})
        added = 0
        for member, score in mapping.items():
            if nx:
                if member in table:
                    continue
            if member not in table:
                added += 1
            table[member] = score
        return added

    def zcount(self, name, min_, max_):
        return len(self.zsets.get(name, {}))

    def zrange(self, name, start, end):
        ordered = sorted(self.zsets.get(name, {}).items(),
                         key=lambda item: item[1])
        members = [member for member, _ in ordered]
        end = end + 1 if end != -1 else None
        return members[start:end]


class FakeConnectionPool:
    def __init__(self, *args, **kwargs):
        pass


class FakeUnixConnection:
    def __init__(self, *args, **kwargs):
        pass


fake_redis_module = SimpleNamespace(
    StrictRedis=FakeRedis,
    ConnectionPool=FakeConnectionPool,
    UnixDomainSocketConnection=FakeUnixConnection,
    SSLConnection=object,
    CredentialProvider=object,
)


class _RosterRedisBackend(RedisBackend):
    redis = fake_redis_module


@pytest.fixture
def backend(app):
    app.conf.result_group_roster = True
    b = _RosterRedisBackend(app=app)
    yield b
    app.conf.result_group_roster = False


def _entry(b, tid, index, status=ROSTER_PENDING, sent_at=None, attempts=0):
    return {
        'id': tid,
        'index': index,
        'task': 'tasks.add',
        'signature': {
            'task': 'tasks.add', 'args': [index], 'kwargs': {},
            'options': {'task_id': tid}, 'subtask_type': None,
            'immutable': False,
        },
        'status': status,
        'attempts': attempts,
        'sent_at': sent_at,
    }


class test_redis_roster_storage:
    def test_save_restore_order_and_statuses(self, backend):
        gid = 'g1'
        backend.save_group_roster(
            gid, [_entry(backend, 'c', 2), _entry(backend, 'a', 0),
                  _entry(backend, 'b', 1)])
        data = backend.restore_group_roster(gid)
        assert [m['id'] for m in data['members']] == ['a', 'b', 'c']
        assert [m['index'] for m in data['members']] == [0, 1, 2]
        assert data['callback'] is False

    def test_mark_sent_and_done(self, backend):
        gid = 'g2'
        backend.save_group_roster(gid, [_entry(backend, 'a', 0)])
        backend.mark_group_member_sent(gid, 'a', attempts=1)
        member = backend.restore_group_roster(gid)['members'][0]
        assert member['status'] == ROSTER_SENT
        assert member['attempts'] == 1
        assert member['sent_at']

        assert backend.mark_group_member_done(gid, 'a', states.SUCCESS) is \
            True
        assert backend.mark_group_member_done(gid, 'a', states.SUCCESS) is \
            False
        assert backend.claim_group_member_recovery(gid, 'a') is False

    def test_claim_lifecycle_and_callback_fence(self, backend):
        gid = 'g3'
        entry = _entry(backend, 'a', 0, status=ROSTER_SENT,
                       sent_at=time.time())
        backend.save_group_roster(gid, [entry])
        assert backend.claim_group_member_recovery(
            gid, 'a', reclaim_after=10) is True
        assert backend.claim_group_member_recovery(
            gid, 'a', reclaim_after=10) is False
        backend.release_group_member_recovery(gid, 'a')
        assert backend.restore_group_roster(gid)['members'][0]['status'] == \
            ROSTER_SENT

        assert backend.mark_group_callback_started(gid) is True
        assert backend.mark_group_callback_started(gid) is False
        assert backend.claim_group_member_recovery(gid, 'a') is False

    def test_stale_recovering_claim_can_be_taken_over(self, backend):
        gid = 'g4'
        entry = _entry(backend, 'a', 0, status=ROSTER_RECOVERING,
                       sent_at=time.time())
        entry['recovering_at'] = time.time() - 100
        backend.save_group_roster(gid, [entry])
        assert backend.claim_group_member_recovery(
            gid, 'a', reclaim_after=10) is True

    def test_delete_removes_keys(self, backend):
        gid = 'g5'
        backend.save_group_roster(gid, [_entry(backend, 'a', 0)])
        backend.mark_group_callback_started(gid)
        backend.mark_group_member_done(gid, 'a', states.SUCCESS)
        assert backend.restore_group_roster(gid) is not None
        backend.delete_group_roster(gid)
        assert backend.restore_group_roster(gid) is None


def _request(tid, gid, index):
    request = Mock(name=f'request-{tid}')
    request.id = tid
    request.group = gid
    request.group_index = index
    request.chord = {'task': 'tasks.collect'}
    return request


class test_redis_roster_chord:
    def _chord(self, backend, size=3, group_id='gid-chord'):
        backend.set_chord_size(group_id, size)
        entries = [_entry(backend, f't{i}', i, status=ROSTER_SENT,
                          sent_at=time.time()) for i in range(size)]
        backend.save_group_roster(group_id, entries)
        return [_request(f't{i}', group_id, i) for i in range(size)]

    def test_callback_fires_once_with_ordered_results(self, backend):
        with patch('celery.backends.redis.GroupResult') as GR, \
                patch('celery.backends.redis.maybe_signature') as ms:
            GR.restore.return_value = None
            callback = ms.return_value = Mock(name='callback')
            requests = self._chord(backend)
            for request, value in zip(requests, (0, 1, 4)):
                backend.on_chord_part_return(
                    request, states.SUCCESS, value)
            callback.delay.assert_called_once_with([0, 1, 4])
            client = backend.client
            for suffix in ('.j', '.t', '.s', '.r', '.st', '.d', '.c'):
                assert not client.exists(
                    backend.get_key_for_group('gid-chord', suffix))

    def test_duplicate_reports_do_not_fire_or_overcount(self, backend):
        with patch('celery.backends.redis.GroupResult') as GR, \
                patch('celery.backends.redis.maybe_signature') as ms:
            GR.restore.return_value = None
            callback = ms.return_value = Mock(name='callback')
            requests = self._chord(backend)
            # member 0 returns twice (e.g. after a selective redelivery)
            backend.on_chord_part_return(requests[0], states.SUCCESS, 0)
            backend.on_chord_part_return(requests[0], states.SUCCESS, 0)
            assert callback.delay.call_count == 0
            backend.on_chord_part_return(requests[1], states.SUCCESS, 1)
            assert callback.delay.call_count == 0
            # out-of-order completion of the final unique member still fires
            backend.on_chord_part_return(requests[2], states.SUCCESS, 4)
            callback.delay.assert_called_once_with([0, 1, 4])
            # a very late duplicate after cleanup cannot fire again
            backend.on_chord_part_return(requests[0], states.SUCCESS, 0)
            assert callback.delay.call_count == 1

    def test_failure_member_is_counted_and_propagated(self, backend):
        from celery.exceptions import ChordError

        with patch('celery.backends.redis.GroupResult') as GR, \
                patch('celery.backends.redis.maybe_signature') as ms, \
                patch.object(backend, 'chord_error_from_stack') as err:
            GR.restore.return_value = None
            ms.return_value = Mock(name='callback')
            requests = self._chord(backend, size=1)
            backend.on_chord_part_return(
                requests[0], states.FAILURE, RuntimeError('boom'))
            assert err.call_count == 1
            assert isinstance(err.call_args.args[1], ChordError)
            # finished (failed) group releases the roster; aggregation keys
            # follow the native path's own lifecycle
            client = backend.client
            assert not client.exists(
                backend.get_key_for_group('gid-chord', '.r'))
            assert not client.exists(
                backend.get_key_for_group('gid-chord', '.c'))
