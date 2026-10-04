from kombu import Exchange, Queue

from celery.app import route_checks
from celery.app.amqp import Queues
from celery.app.route_checks import (
    DEFAULT_QUEUE_NOT_FOUND, INVALID_QUEUE_TYPE, INVALID_ROUTE_VALUE,
    QUEUE_NOT_FOUND, inspect_routing, rule_source,
)
from celery.app.routes import prepare


def make_queues(*queue_args, create_missing=True,
                create_missing_queue_type='classic',
                create_missing_queue_exchange_type=None,
                default_exchange=None, default_routing_key=None):
    return Queues(
        tuple(queue_args) if queue_args else None,
        create_missing=create_missing,
        create_missing_queue_type=create_missing_queue_type,
        create_missing_queue_exchange_type=create_missing_queue_exchange_type,
        default_exchange=default_exchange,
        default_routing_key=default_routing_key,
    )


def run(routes, queues=None, task_names=(), task_options=None,
        default_queue='celery', implicit_default=None, **queues_kwargs):
    if queues is None:
        queues = make_queues(**queues_kwargs)
    return inspect_routing(
        version=1,
        prepared_routes=prepare(routes),
        queues=queues,
        missing_factory=queues.new_missing,
        consume_from=set(queues.consume_from),
        task_names=tuple(task_names),
        task_options=task_options or {},
        create_missing=queues.create_missing,
        create_missing_queue_type=queues.create_missing_queue_type,
        create_missing_queue_exchange_type=(
            queues.create_missing_queue_exchange_type),
        default_queue_name=default_queue,
        implicit_default_queue=implicit_default,
    )


def winner_of(report, task):
    resolution = next(r for r in report.resolutions if r.task == task)
    return resolution


class test_inspect_routing:

    def test_resolves_to_declared_queue_and_subscription_matches(self):
        topic = Exchange('tasks', 'topic')
        queues = make_queues(
            Queue('cpu', topic, routing_key='cpu'),
            create_missing=False)
        report = run([{'proj.add': {'queue': 'cpu'}}], queues,
                     task_names=['proj.add'], default_queue='cpu')
        assert not report.errors
        resolution = winner_of(report, 'proj.add')
        assert resolution.winner.router_index == 0
        assert resolution.target.queue.name == 'cpu'
        assert report.reachable == ('cpu',)
        assert report.subscription_diff.reachable_only == ()
        assert report.subscription_diff.subscribed_only == ()

    def test_missing_queue_is_fatal_when_create_missing_disabled(self):
        queues = make_queues(create_missing=False)
        report = run([{'proj.add': 'ghost'}], queues,
                     task_names=['proj.add'], default_queue='celery')
        codes = {e.code for e in report.errors}
        assert QUEUE_NOT_FOUND in codes
        assert DEFAULT_QUEUE_NOT_FOUND in codes
        resolution = winner_of(report, 'proj.add')
        assert resolution.target is None

    def test_missing_queue_is_reported_as_auto_created(self):
        queues = make_queues(create_missing=True)
        report = run([{'proj.add': 'ghost'}], queues,
                     task_names=['proj.add'], default_queue='celery')
        assert not report.errors
        auto = {q.name: q for q in report.auto_queues}
        assert set(auto) == {'ghost', 'celery'}
        assert auto['ghost'].queue_type == 'classic'
        assert auto['ghost'].exchange_name == 'ghost'
        assert auto['ghost'].routing_key == 'ghost'
        # The analysis must not register the missing queue in live state.
        assert 'ghost' not in queues

    def test_auto_created_quorum_queue_parameters(self):
        queues = make_queues(
            create_missing=True,
            create_missing_queue_type='quorum',
            create_missing_queue_exchange_type='topic')
        report = run([{'proj.add': 'ghost'}], queues,
                     task_names=['proj.add'])
        auto = {q.name: q for q in report.auto_queues}
        assert auto['ghost'].queue_type == 'quorum'
        assert auto['ghost'].exchange_type == 'topic'
        assert auto['ghost'].queue_arguments == {'x-queue-type': 'quorum'}

    def test_invalid_create_missing_queue_type_is_fatal(self):
        queues = make_queues(
            create_missing=True, create_missing_queue_type='stream')
        report = run([{'proj.add': 'ghost'}], queues,
                     task_names=['proj.add'])
        assert any(e.code == INVALID_QUEUE_TYPE for e in report.errors)

    def test_rule_supplied_queue_instance_is_not_declared(self):
        queues = make_queues(create_missing=False)
        q = Queue('ruleq', Exchange('rulex', 'topic'), routing_key='ruleq')
        report = run([{'proj.add': {'queue': q}}], queues,
                     task_names=['proj.add'], default_queue='celery')
        assert any(e.code == QUEUE_NOT_FOUND for e in report.errors)

    def test_shadowed_rule_is_reported_with_winner(self):
        queues = make_queues(
            Queue('a'), Queue('b'), create_missing=False,
            default_exchange=Exchange('celery', 'direct'))
        queues.select(['a'])
        report = run(
            [{'proj.add': 'a'}, {'proj.add': 'b'}],
            queues, task_names=['proj.add'], default_queue='celery')
        shadowed = report.shadowed
        assert len(shadowed) == 1
        assert shadowed[0].tasks == ('proj.add',)
        assert shadowed[0].shadowed_by[0].router_index == 0
        resolution = winner_of(report, 'proj.add')
        assert len(resolution.losers) == 1
        assert 'takes precedence' in resolution.losers[0].reason

    def test_partial_shadow_is_not_listed_as_fully_shadowed(self):
        queues = make_queues(
            Queue('a'), Queue('b'), create_missing=False,
            default_exchange=Exchange('celery', 'direct'))
        queues.select(['a', 'b'])
        report = run(
            [{'proj.add': 'a'}, {'proj.*': 'b'}],
            queues, task_names=['proj.add', 'proj.mul'],
            default_queue='celery')
        assert not report.shadowed
        assert winner_of(report, 'proj.add').target.queue.name == 'a'
        assert winner_of(report, 'proj.mul').target.queue.name == 'b'

    def test_unmatched_rule_is_hint(self):
        queues = make_queues(Queue('a'), default_routing_key='a')
        report = run([{'proj.unknown': 'a'}], queues,
                     task_names=['proj.add'])
        assert len(report.unmatched) == 1
        assert report.unmatched[0].rule.matcher == 'proj.unknown'

    def test_unmatched_rule_target_is_still_validated(self):
        # send_task() accepts unknown names, so every rule target is
        # validated even when no registered task matches the rule.
        queues = make_queues(create_missing=False)
        report = run([{'proj.unknown': 'ghost'}], queues,
                     task_names=['proj.add'], default_queue='celery')
        assert any(e.code == QUEUE_NOT_FOUND for e in report.errors)

    def test_unmatched_auto_queue_is_still_reachable(self):
        queues = make_queues(create_missing=True)
        report = run([{'proj.unknown': 'ghost'}], queues,
                     task_names=['proj.add'])
        assert 'ghost' in report.reachable
        assert 'ghost' in {q.name for q in report.auto_queues}

    def test_pattern_rules_match_in_order(self):
        queues = make_queues(
            Queue('media'), Queue('other'),
            default_exchange=Exchange('celery', 'direct'))
        report = run(
            [{'video.*': 'media', '*': 'other'}],
            queues, task_names=['video.encode', 'text.render'],
            default_queue='celery')
        assert winner_of(report, 'video.encode').target.queue.name == 'media'
        assert winner_of(report, 'text.render').target.queue.name == 'other'

    def test_callable_and_named_routers_are_undecidable(self):
        queues = make_queues(Queue('a'))
        report = run(
            ['celery.tests.unit.app.test_route_checks:_named_router',
             lambda task, args, kwargs, options, task_type=None: None],
            queues, task_names=['proj.add'])
        assert {item.kind for item in report.undecidable} == {
            'named', 'callable'}
        assert winner_of(report, 'proj.add').note is not None

    def test_compat_router_class_is_undecidable(self):
        queues = make_queues(Queue('a'))

        class OldRouter:
            def route_for_task(self, task, args, kwargs):
                return None

        report = run([OldRouter()], queues, task_names=['proj.add'])
        assert report.undecidable[0].kind == 'router-class'

    def test_routing_key_mismatch_on_topic_exchange(self):
        topic = Exchange('tasks', 'topic')
        queues = make_queues(
            Queue('cpu', topic, routing_key='cpu'),
            create_missing=False)
        report = run(
            [{'proj.add': {'queue': 'cpu', 'routing_key': 'gpu'}}],
            queues, task_names=['proj.add'], default_queue='celery')
        assert len(report.mismatches) == 1
        mismatch = report.mismatches[0]
        assert mismatch.route_routing_key == 'gpu'
        assert mismatch.queue_routing_key == 'cpu'
        assert mismatch.rewritten_at_publish is False

    def test_routing_key_mismatch_on_direct_is_marked_rewritten(self):
        direct = Exchange('cpu', 'direct')
        queues = make_queues(
            Queue('cpu', direct, routing_key='cpu'),
            create_missing=False)
        report = run(
            [{'proj.add': {'queue': 'cpu', 'routing_key': 'gpu'}}],
            queues, task_names=['proj.add'], default_queue='celery')
        assert report.mismatches[0].rewritten_at_publish is True

    def test_fanout_ignores_routing_key_mismatch(self):
        fanout = Exchange('bcast', 'fanout')
        queues = make_queues(
            Queue('bcast', fanout, routing_key='ignored'),
            create_missing=False)
        report = run(
            [{'proj.add': {'queue': 'bcast', 'routing_key': 'whatever'}}],
            queues, task_names=['proj.add'], default_queue='celery')
        assert not report.mismatches

    def test_exchange_mismatch(self):
        queues = make_queues(
            Queue('cpu', Exchange('tasks', 'topic'), routing_key='cpu'),
            create_missing=False)
        report = run(
            [{'proj.add': {'queue': 'cpu', 'exchange': 'other'}}],
            queues, task_names=['proj.add'], default_queue='celery')
        assert len(report.mismatches) == 1
        assert report.mismatches[0].route_exchange == 'other'

    def test_subscription_diff_both_directions(self):
        topic = Exchange('tasks', 'topic')
        queues = make_queues(
            Queue('cpu', topic, routing_key='cpu'),
            Queue('idle', topic, routing_key='idle'),
            Queue('celery', topic, routing_key='celery'),
            create_missing=False)
        queues.select(['cpu', 'idle'])
        report = run([{'proj.add': {'queue': 'cpu'}}], queues,
                     task_names=['proj.add'], default_queue='celery')
        diff = report.subscription_diff
        assert 'idle' in diff.subscribed_only
        assert 'celery' in diff.reachable_only

    def test_task_execution_options_are_the_baseline(self):
        queues = make_queues(Queue('cpu'), default_routing_key='cpu')
        report = run(None, queues, task_names=['proj.add'],
                     task_options={'proj.add': {'queue': 'cpu'}})
        resolution = winner_of(report, 'proj.add')
        assert resolution.winner is None
        assert resolution.target.queue.name == 'cpu'

    def test_routing_key_only_rule_targets_default_queue_exchange(self):
        queues = make_queues(
            Queue('celery', Exchange('celery', 'direct'),
                  routing_key='celery'),
            create_missing=False)
        report = run(
            [{'proj.add': {'routing_key': 'custom'}}],
            queues, task_names=['proj.add'], default_queue='celery')
        resolution = winner_of(report, 'proj.add')
        assert resolution.target.queue.name == 'celery'
        assert report.mismatches[0].rewritten_at_publish is True

    def test_exchange_only_rule_does_not_add_queue_reachable(self):
        queues = make_queues(
            Queue('celery', Exchange('celery', 'direct'),
                  routing_key='celery'),
            create_missing=False)
        report = run(
            [{'proj.add': {'exchange': 'events', 'routing_key': '#'}}],
            queues, task_names=['proj.add'], default_queue='celery')
        resolution = winner_of(report, 'proj.add')
        assert resolution.target.queue is None
        assert 'events' not in report.reachable

    def test_invalid_route_values(self):
        queues = make_queues(Queue('a'))
        report = run(
            [{'proj.add': 3}, {'proj.mul': {'queue': 7}},
             {'proj.div': None}],
            queues, task_names=['proj.add', 'proj.mul', 'proj.div'])
        invalid = [e for e in report.errors
                   if e.code == INVALID_ROUTE_VALUE]
        assert len(invalid) == 3

    def test_ineffective_empty_mapping_rule(self):
        queues = make_queues(
            Queue('celery', Exchange('celery', 'direct'),
                  routing_key='celery'))
        report = run(
            [{'proj.add': {}}],
            queues, task_names=['proj.add'], default_queue='celery')
        assert len(report.ineffective) == 1
        # An ineffective rule does not win; the default fallback does.
        assert winner_of(report, 'proj.add').winner is None
        assert not report.shadowed

    def test_implicit_default_queue_type_quorum(self):
        # Reproduces AMQP.Queues(()), where the default queue is built
        # from task_default_queue_type.
        default_queue = Queue(
            'celery', Exchange('celery', 'direct'), routing_key='celery',
            queue_arguments={'x-queue-type': 'quorum'})
        queues = make_queues(create_missing=True)
        report = run(None, queues, task_names=['proj.add'],
                     implicit_default=default_queue)
        auto = {q.name: q for q in report.auto_queues}
        assert auto['celery'].queue_type == 'quorum'

    def test_errors_are_deduplicated(self):
        queues = make_queues(create_missing=False)
        # The same broken pattern matches two tasks: one finding, not two.
        report = run([{'proj.*': 'ghost'}],
                     queues, task_names=['proj.add', 'proj.mul'],
                     default_queue='celery')
        missing = [e for e in report.errors if e.code == QUEUE_NOT_FOUND]
        assert len(missing) == 1
        default_missing = [
            e for e in report.errors
            if e.code == DEFAULT_QUEUE_NOT_FOUND]
        assert len(default_missing) == 1

    def test_formatting_contains_all_sections(self):
        queues = make_queues(
            Queue('cpu', Exchange('tasks', 'topic'), routing_key='cpu'),
            create_missing=True)
        report = run(
            [{'proj.add': {'queue': 'cpu', 'routing_key': 'x'}},
             {'proj.add': 'cpu'}],
            queues, task_names=['proj.add'])
        text = route_checks.format_report(report)
        assert 'Per-task resolution' in text
        assert 'auto-created' in text
        assert 'disagreements with queue bindings' in text
        assert 'Subscription set' in text
        assert 'Fully shadowed' in text
        assert 'cannot be decided statically' in text
        errors_text = route_checks.format_errors(report)
        assert errors_text  # always renders a header, even with no errors

    def test_rule_source_labels(self):
        assert rule_source(None).endswith('fallback')
        assert rule_source(
            route_checks.RuleRef(2, 'exact', 't')) == (
            "task_routes[2] (exact name 't')")
        assert 'pattern' in rule_source(
            route_checks.RuleRef(0, 'pattern', '.*'))


def _named_router(task, args, kwargs, options, task_type=None):
    return None
