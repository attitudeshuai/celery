"""Tests for the group member roster and selective member recovery.

These tests exercise the generic key/value implementation through the
in-memory cache backend, the canvas freeze/deliver hooks, the incrementing
chord counter path and the polling ``celery.chord_unlock`` task.
"""
import threading
import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest

import celery.contrib.testing.worker as contrib_embed_worker
from celery import chord, group, states
from celery.app.task import Context
from celery.exceptions import ChordCallbackStarted, GroupRosterMissing
from celery.result import MEMBER_EXPIRED, MEMBER_NEVER_DELIVERED, ROSTER_PENDING, ROSTER_SENT, GroupResult
from celery.utils.nodenames import anon_nodename


@contextmanager
def embed_worker(app, concurrency=2, pool='threads', **kwargs):
    app.finalize()
    app.set_current()
    worker = contrib_embed_worker.TestWorkController(
        app=app, concurrency=concurrency, hostname=anon_nodename(),
        pool=pool, ready_callback=None, without_heartbeat=True,
        without_mingle=True, without_gossip=True, **kwargs)
    thread = threading.Thread(target=worker.start, daemon=True)
    thread.start()
    worker.ensure_started()
    try:
        yield worker
    finally:
        worker.stop()
        thread.join(10.0)
        if thread.is_alive():
            raise RuntimeError('embedded worker failed to stop')


@pytest.fixture
def roster_on(app):
    app.conf.result_group_roster = True
    try:
        yield app
    finally:
        app.conf.result_group_roster = False


@pytest.fixture
def collect(app):
    @app.task(shared=False)
    def collect(items):
        return items

    return collect


class test_roster_canvas:
    def test_roster_settled_in_submission_order_and_marked_sent(
            self, app, roster_on):
        b = app.backend

        def build():
            return group([self.add.s(1, 1), self.add.s(2, 2),
                          self.add.s(3, 3)])

        res = build().apply_async()
        data = b.restore_group_roster(res.id)
        assert data is not None
        assert data['callback'] is False
        assert [m['index'] for m in data['members']] == [0, 1, 2]
        assert [m['id'] for m in data['members']] == [r.id for r in res.results]
        assert all(m['task'] for m in data['members'])
        assert all(m['status'] == ROSTER_SENT for m in data['members'])
        assert all(m['sent_at'] for m in data['members'])
        assert all(m['attempts'] == 0 for m in data['members'])
        # signature kept for redelivery with the original task id
        signatures = [m['signature'] for m in data['members']]
        assert all(sig['options']['task_id'] for sig in signatures)
        assert all(sig['options']['group_id'] == res.id for sig in signatures)

        roster = res.roster()
        assert [m.id for m in roster] == [r.id for r in res.results]
        assert [m.index for m in roster] == [0, 1, 2]
        assert all(m.status == ROSTER_SENT for m in roster)

    def test_disabled_by_default_writes_nothing(self, app):
        assert app.conf.result_group_roster is False
        b = app.backend
        with patch.object(type(b), 'save_group_roster') as save, \
                patch.object(type(b), 'mark_group_member_sent') as sent:
            res = group([self.add.s(1, 1), self.add.s(2, 2)]).apply_async()
            save.assert_not_called()
            sent.assert_not_called()
        assert b.restore_group_roster(res.id) is None

    def test_delete_group_also_removes_roster(self, app, roster_on):
        res = group([self.add.s(1, 1)]).apply_async()
        assert app.backend.restore_group_roster(res.id) is not None
        res.delete()
        assert app.backend.restore_group_roster(res.id) is None

    def test_empty_group_has_no_roster(self, app, roster_on):
        res = group(app=app).apply_async()
        assert app.backend.restore_group_roster(res.id) is None


class test_roster_backend_primitives:
    def _entry(self, app, gid, index, status=ROSTER_PENDING,
               sent_at=None, attempts=0, task=None):
        task = task or self.add
        sig = task.s(index)
        result = sig.freeze(group_id=gid, group_index=index)
        return {
            'id': result.id,
            'index': index,
            'task': sig.task,
            'signature': dict(sig),
            'status': status,
            'attempts': attempts,
            'sent_at': sent_at,
        }

    def test_done_is_idempotent_and_claim_blocks(self, app, roster_on):
        b = app.backend
        gid = 'gid-primitives'
        entry = self._entry(app, gid, 0)
        tid = entry['id']
        b.save_group_roster(gid, [entry])

        assert b.mark_group_member_done(gid, tid, states.SUCCESS) is True
        # a second (duplicate) terminal report must not count again
        assert b.mark_group_member_done(gid, tid, states.SUCCESS) is False
        # terminal members can never be claimed for recovery
        assert b.claim_group_member_recovery(gid, tid, reclaim_after=1) is \
            False

    def test_claim_release_and_stale_takeover(self, app, roster_on):
        b = app.backend
        gid = 'gid-claims'
        entry = self._entry(app, gid, 0, status=ROSTER_SENT,
                            sent_at=time.time())
        tid = entry['id']
        b.save_group_roster(gid, [entry])

        assert b.claim_group_member_recovery(gid, tid, reclaim_after=10) is \
            True
        # concurrent recovery run must not claim the same member
        assert b.claim_group_member_recovery(gid, tid, reclaim_after=10) is \
            False
        b.release_group_member_recovery(gid, tid)
        assert b.restore_group_roster(gid)['members'][0]['status'] == \
            ROSTER_SENT
        assert b.claim_group_member_recovery(gid, tid, reclaim_after=10) is \
            True
        # a stale RECOVERING claim (dead recovery worker) can be taken over
        stale = b.restore_group_roster(gid)['members'][0]
        stale['recovering_at'] = time.time() - 100
        b.save_group_roster(gid, [stale])
        assert b.claim_group_member_recovery(gid, tid, reclaim_after=10) is \
            True

    def test_callback_fence(self, app, roster_on):
        b = app.backend
        gid = 'gid-fence'
        b.save_group_roster(gid, [self._entry(app, gid, 0)])
        assert b.mark_group_callback_started(gid) is True
        assert b.mark_group_callback_started(gid) is False


class test_recover_lost:
    def _entry(self, app, gid, index, status=ROSTER_SENT, sent_at=None,
               attempts=0, task=None, store_state=None, value=None):
        task = task or self.add
        sig = task.s(index)
        result = sig.freeze(group_id=gid, group_index=index)
        if store_state is not None:
            app.backend.store_result(result.id, value, store_state)
        return {
            'id': result.id,
            'index': index,
            'task': sig.task,
            'signature': dict(sig),
            'status': status,
            'attempts': attempts,
            'sent_at': sent_at,
        }

    def test_missing_roster_raises(self, app, roster_on):
        with pytest.raises(GroupRosterMissing):
            GroupResult('no-such-group', [], app=app).roster()
        with pytest.raises(GroupRosterMissing):
            GroupResult('no-such-group', [], app=app).recover_lost()

    def test_callback_started_refuses_recovery(self, app, roster_on):
        gid = 'gid-callback-started'
        app.backend.save_group_roster(
            gid, [self._entry(app, gid, 0, status=ROSTER_PENDING)])
        app.backend.mark_group_callback_started(gid)
        with pytest.raises(ChordCallbackStarted):
            GroupResult(gid, [], app=app).recover_lost(timeout=0)

    def test_only_lost_members_are_redelivered(self, app, roster_on):
        b = app.backend
        gid = 'gid-recover'
        now = time.time()
        old = now - 100
        entries = [
            # 0: succeeded already -> must never run again
            self._entry(app, gid, 0, store_state=states.SUCCESS, value=0),
            # 1: failed -> known failure, not lost
            self._entry(app, gid, 1, store_state=states.FAILURE,
                        value=RuntimeError('boom')),
            # 2: retrying -> in flight, not lost
            self._entry(app, gid, 2, store_state=states.RETRY,
                        value=RuntimeError('retry')),
            # 3: started -> running, not lost
            self._entry(app, gid, 3, store_state=states.STARTED),
            # 4: never delivered, past the grace window -> lost
            self._entry(app, gid, 4, status=ROSTER_PENDING, sent_at=None),
            # 5: delivered but result expired -> lost
            self._entry(app, gid, 5, status=ROSTER_SENT, sent_at=old),
            # 6: delivered recently, no result yet -> inside grace window
            self._entry(app, gid, 6, status=ROSTER_SENT, sent_at=now),
            # 7: never delivered but redelivery quota exhausted
            self._entry(app, gid, 7, status=ROSTER_SENT, sent_at=old,
                        attempts=1),
        ]
        # add an 8th member whose task name is no longer registered
        missing = self._entry(app, gid, 8, status=ROSTER_SENT, sent_at=old)
        missing_tid = missing['id']
        missing['task'] = 'no.longer.registered'
        missing['signature'] = {
            'task': 'no.longer.registered', 'args': (), 'kwargs': {},
            'options': {'task_id': missing_tid, 'group_id': gid},
            'subtask_type': None, 'immutable': False,
        }
        entries.append(missing)

        b.save_group_roster(gid, entries, created_at=old)

        with patch('celery.app.task.Task.apply_async') as publish:
            report = GroupResult(gid, [], app=app).recover_lost(
                timeout=10, limit=1)

        reasons = {m.id: m.reason for m in report.skipped}
        assert len(report.recovered) == 2
        recovered = {m.id: m for m in report.recovered}
        assert recovered[entries[4]['id']].reason == MEMBER_NEVER_DELIVERED
        assert recovered[entries[5]['id']].reason == MEMBER_EXPIRED
        assert recovered[entries[4]['id']].attempts == 1
        assert recovered[entries[5]['id']].attempts == 1
        # both redelivered with their original task id
        published_ids = {
            c.kwargs.get('task_id') for c in publish.call_args_list}
        assert published_ids == {entries[4]['id'], entries[5]['id']}
        assert publish.call_count == 2

        assert reasons[entries[0]['id']] == 'completed'
        assert reasons[entries[1]['id']] == 'failed'
        assert reasons[entries[2]['id']] == 'retrying'
        assert reasons[entries[3]['id']] == 'running'
        assert reasons[entries[6]['id']] == 'pending_grace'
        assert reasons[entries[7]['id']] == 'quota_exceeded'
        assert reasons[entries[8]['id']] == 'task_not_registered'

        # roster reflects the two redeliveries and the unregistered member
        # claim was released
        members = {m['id']: m
                   for m in b.restore_group_roster(gid)['members']}
        assert members[entries[4]['id']]['status'] == ROSTER_SENT
        assert members[entries[4]['id']]['attempts'] == 1
        assert members[entries[5]['id']]['attempts'] == 1
        assert members[entries[8]['id']]['status'] == ROSTER_SENT

    def test_completed_member_redelivered_late_is_not_rerun(
            self, app, roster_on):
        b = app.backend
        gid = 'gid-already-successful'
        old = time.time() - 100
        entry = self._entry(app, gid, 0, status=ROSTER_SENT, sent_at=old,
                            store_state=states.SUCCESS, value=42)
        b.save_group_roster(gid, [entry])
        with patch('celery.app.task.Task.apply_async') as publish:
            report = GroupResult(gid, [], app=app).recover_lost(timeout=10)
        publish.assert_not_called()
        assert [m.reason for m in report.skipped] == ['completed']
        assert report.recovered == []


class test_chord_counter_with_roster:
    def _request(self, tid, gid, body, group_index):
        ctx = Context()
        ctx.id = tid
        ctx.group = gid
        ctx.group_index = group_index
        ctx.chord = dict(body)
        return ctx

    def test_callback_once_with_ordered_results_and_roster_cleanup(
            self, app, roster_on, collect):
        b = app.backend
        body = collect.s()
        body.freeze('body-id')
        body_result = chord(
            [self.add.s(0, 0), self.add.s(1, 1), self.add.s(2, 2)],
            body,
        ).apply_async()
        gid = body_result.parent.id
        ids = [r.id for r in body_result.parent.results]

        with patch('celery.canvas.Signature.apply_async') as dispatch:
            for index, tid in enumerate(ids):
                b.mark_as_done(
                    tid, index * 2,
                    request=self._request(tid, gid, body, index))

        dispatch.assert_called_once()
        assert dispatch.call_args.args[0] == ([0, 2, 4],)
        # group finished: roster and counters are gone
        assert b.restore_group_roster(gid) is None
        assert b.get(b.get_key_for_chord(gid)) is None

    def test_duplicate_part_return_does_not_overcount(self, app, roster_on,
                                                      collect):
        b = app.backend
        body = collect.s()
        body.freeze('body-id-2')
        body_result = chord(
            [self.add.s(1, 1), self.add.s(2, 2)], body).apply_async()
        gid = body_result.parent.id
        ids = [r.id for r in body_result.parent.results]

        with patch('celery.canvas.Signature.apply_async') as dispatch:
            # first member reports ...
            b.mark_as_done(ids[0], 2,
                           request=self._request(ids[0], gid, body, 0))
            # ... and reports again (duplicate execution after recovery):
            # must not bump the counter a second time
            b.mark_as_done(ids[0], 2,
                           request=self._request(ids[0], gid, body, 0))
            assert dispatch.call_count == 0
            b.mark_as_done(ids[1], 4,
                           request=self._request(ids[1], gid, body, 1))
            dispatch.assert_called_once()
            assert dispatch.call_args.args[0] == ([2, 4],)

        assert b.restore_group_roster(gid) is None

    def test_late_duplicate_after_completion_does_not_refire(
            self, app, roster_on, collect):
        b = app.backend
        body = collect.s()
        body.freeze('body-id-3')
        body_result = chord([self.add.s(1, 1)], body).apply_async()
        gid = body_result.parent.id
        ids = [r.id for r in body_result.parent.results]

        with patch('celery.canvas.Signature.apply_async') as dispatch:
            b.mark_as_done(ids[0], 2,
                           request=self._request(ids[0], gid, body, 0))
            dispatch.assert_called_once()
            # roster already cleaned up; a very late duplicate must not refire
            b.on_chord_part_return(
                self._request(ids[0], gid, body, 0), states.SUCCESS, 2)
            assert dispatch.call_count == 1


class test_unlock_chord_with_roster:
    def _run_unlock(self, app, collect, gid, pre_mark=False):
        b = app.backend
        tids = ['t1', 't2']
        for tid, value in zip(tids, (11, 22)):
            b.store_result(tid, value, states.SUCCESS)
        result = [((tid, None), None) for tid in tids]
        if pre_mark:
            b.mark_group_callback_started(gid)
        unlock = app.tasks['celery.chord_unlock']
        with patch('celery.canvas.Signature.apply_async') as dispatch:
            unlock.apply(args=(gid, dict(collect.s())),
                         kwargs={'result': result})
        return dispatch

    def test_unlock_fences_and_cleans_roster(self, app, roster_on, collect):
        b = app.backend
        gid = 'gid-unlock'
        b.save_group_roster(gid, [])
        dispatch = self._run_unlock(app, collect, gid)
        dispatch.assert_called_once()
        assert dispatch.call_args.args[0] == ([11, 22],)
        assert b.restore_group_roster(gid) is None

    def test_unlock_refuses_when_callback_already_started(
            self, app, roster_on, collect):
        b = app.backend
        gid = 'gid-unlock-fence'
        b.save_group_roster(gid, [])
        dispatch = self._run_unlock(app, collect, gid, pre_mark=True)
        dispatch.assert_not_called()
        # nothing deleted: the original completion path owns the cleanup
        assert b.restore_group_roster(gid) is not None

    def test_unlock_no_roster_roundtrips_when_disabled(
            self, app, collect):
        assert app.conf.result_group_roster is False
        b = app.backend
        with patch.object(type(b), 'mark_group_callback_started') as mark, \
                patch.object(type(b), 'delete_group_roster') as delete:
            dispatch = self._run_unlock(app, collect, 'gid-unlock-off')
        dispatch.assert_called_once()
        mark.assert_not_called()
        delete.assert_not_called()


def _wait_states(app, tids, expected, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(app.backend.get_state(tid) == expected for tid in tids):
            return
        time.sleep(0.05)
    states_seen = [app.backend.get_state(tid) for tid in tids]
    raise AssertionError(f'timed out waiting for {expected}: {states_seen}')


class test_roster_with_embedded_worker:
    def test_only_lost_member_reruns_through_real_worker(
            self, app, roster_on):
        @app.task(shared=False)
        def rl_add(x, y):
            return x + y

        b = app.backend
        gid = 'gid-worker-recover'
        sigs = [rl_add.s(i, 10) for i in range(4)]
        entries = []
        for i, sig in enumerate(sigs):
            result = sig.freeze(group_id=gid, group_index=i)
            entries.append({
                'id': result.id, 'index': i, 'task': sig.task,
                'signature': dict(sig),
                'status': ROSTER_PENDING, 'attempts': 0, 'sent_at': None,
            })
        b.save_group_roster(gid, entries, created_at=time.time() - 100)

        with embed_worker(app):
            # members 0,1,3 are delivered and finish; 2 never is
            for i in (0, 1, 3):
                sigs[i].apply_async(add_to_parent=False)
            _wait_states(app, [entries[i]['id'] for i in (0, 1, 3)],
                         states.SUCCESS)

            report = GroupResult(gid, [], app=app).recover_lost(timeout=10)
            assert [m.index for m in report.recovered] == [2]
            assert report.recovered[0].reason == MEMBER_NEVER_DELIVERED
            assert [m.reason for m in report.skipped] == [
                'completed', 'completed', 'completed']
            _wait_states(app, [entries[2]['id']], states.SUCCESS)

        # every member finished exactly once with the expected value
        for i in range(4):
            assert b.get_result(entries[i]['id']) == i + 10
        roster = b.restore_group_roster(gid)
        statuses = {m['id']: m['status'] for m in roster['members']}
        assert all(s == states.SUCCESS for s in statuses.values())

    def test_recovered_chord_members_fire_body_exactly_once(
            self, app, roster_on):
        @app.task(shared=False)
        def rc_add(x, y):
            return x + y

        @app.task(shared=False)
        def rc_total(items):
            return sum(items)

        b = app.backend
        gid = 'gid-worker-chord'
        body = rc_total.s()
        body_id = body.freeze('rc-body-id').id

        # freeze the header group with the chord body attached, like
        # chord.run would, but do not deliver anything yet
        header = group([rc_add.s(i, 1) for i in range(3)],
                       app=app, task_id=gid)
        header._freeze_group_tasks(group_id=gid, chord=body)
        entries = []
        for i, sig in enumerate(header.tasks):
            entries.append({
                'id': sig.options['task_id'], 'index': i,
                'task': sig.task, 'signature': dict(sig),
                'status': ROSTER_PENDING, 'attempts': 0, 'sent_at': None,
            })
        b.save_group_roster(gid, entries, created_at=time.time() - 100)
        # emulate the apply_chord() counter/group setup (cache incr backend)
        b._apply_chord_incr(
            (gid, [app.AsyncResult(e['id']) for e in entries]), body)

        with embed_worker(app):
            # no header message exists anywhere: recovery delivers all three,
            # the worker runs them and the chord counter fires the body
            report = GroupResult(gid, [], app=app).recover_lost(timeout=10)
            assert len(report.recovered) == 3
            body_result = app.AsyncResult(body_id)
            assert body_result.get(timeout=20) == 0 + 1 + 2 + 3

        # body fired and group ended: roster and counter are gone
        assert b.restore_group_roster(gid) is None
        assert b.get(b.get_key_for_chord(gid)) is None
