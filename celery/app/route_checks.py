"""Static, broker-free validation of the task routing configuration.

When :setting:`task_routes_validate` is enabled the application evaluates
the whole routing set once -- at worker startup and again after every
routing-relevant configuration change -- *before* any task message is
published.

The analysis performed in this module is purely introspective: it only
reads the prepared routing table (:mod:`celery.app.routes`), the declared
:class:`~celery.app.amqp.Queues`, the registered task names and a handful
of configuration values.  It never connects to a broker, never declares
queues and never publishes messages.

For every known task name the analyzer reports which rule wins, where the
message would end up (queue/exchange/routing key) and why every other
matching rule loses, alongside three static conclusions:

* queues that would be created implicitly at first publish, with their
  queue type and exchange parameters;
* routing keys/exchanges carried by rules that disagree with the target
  queue's binding;
* the difference between the subscribed queue set (``worker -Q``) and the
  queues statically reachable through routing.

Rules that are fully shadowed by earlier rules are reported as hints.
Callable routers and routers configured by import name decide at runtime
from the task arguments, so they are marked as undecidable instead of
being guessed.
"""
from collections import OrderedDict, namedtuple
from collections.abc import Mapping

from kombu import Exchange, Queue
from kombu.utils.functional import lazy

from .routes import MapRoute

__all__ = (
    'RuleRef', 'Target', 'RuleLoss', 'TaskResolution', 'AutoQueue',
    'BindingMismatch', 'ShadowedRule', 'UnmatchedRule', 'IneffectiveRule',
    'UndecidableRule', 'StaticError', 'SubscriptionDiff', 'RouteReport',
    'inspect_routing', 'rule_source', 'format_report', 'format_errors',
)

#: Fatal finding codes.
INVALID_QUEUE_PART = 'invalid-queue-part'
QUEUE_NOT_FOUND = 'queue-not-found'
DEFAULT_QUEUE_NOT_FOUND = 'default-queue-not-found'
INVALID_QUEUE_TYPE = 'invalid-queue-type'
INVALID_ROUTE_VALUE = 'invalid-route-value'

DEFAULT_SOURCE = 'task_default_queue fallback'
EXCHANGE_ONLY = 'exchange-only publish (no queue declared in the rule)'
NOOP_REASON = (
    'rule carries no queue, exchange or routing key; routing falls '
    'through as if it did not match')
UNDECIDABLE_NOTE = (
    '{n} runtime router(s) in task_routes can override the static '
    'result depending on task arguments')


def rule_source(ref):
    """Human-readable location of *ref* inside the ``task_routes`` list."""
    if ref is None:
        return DEFAULT_SOURCE
    kind = 'exact name' if ref.kind == 'exact' else 'pattern'
    return f'task_routes[{ref.router_index}] ({kind} {ref.matcher!r})'


# --- static findings -------------------------------------------------------

RuleRef = namedtuple('RuleRef', ('router_index', 'kind', 'matcher'))
#: Resolved destination. ``queue`` is ``None`` for exchange-only publishes.
Target = namedtuple('Target', ('queue', 'auto_created', 'origin'))
RuleLoss = namedtuple('RuleLoss', ('rule', 'reason'))
TaskResolution = namedtuple('TaskResolution', (
    'task', 'winner', 'target', 'explicit_exchange', 'explicit_routing_key',
    'losers', 'note',
))
AutoQueue = namedtuple('AutoQueue', (
    'name', 'queue_type', 'exchange_name', 'exchange_type',
    'routing_key', 'queue_arguments',
))
BindingMismatch = namedtuple('BindingMismatch', (
    'rule', 'task', 'queue_name', 'route_exchange', 'route_routing_key',
    'queue_exchange', 'queue_routing_key', 'rewritten_at_publish',
))
ShadowedRule = namedtuple('ShadowedRule', ('rule', 'tasks', 'shadowed_by'))
UnmatchedRule = namedtuple('UnmatchedRule', ('rule',))
IneffectiveRule = namedtuple('IneffectiveRule', ('rule', 'tasks', 'reason'))
UndecidableRule = namedtuple(
    'UndecidableRule', ('router_index', 'router', 'kind', 'reason'))
StaticError = namedtuple('StaticError', ('rule', 'task', 'code', 'message'))
SubscriptionDiff = namedtuple('SubscriptionDiff', (
    'reachable_only', 'subscribed_only'))
RouteReport = namedtuple('RouteReport', (
    'version', 'resolutions', 'errors', 'auto_queues', 'mismatches',
    'shadowed', 'unmatched', 'ineffective', 'undecidable',
    'subscription_diff', 'default_target', 'subscribed', 'reachable',
))

#: Sentinel outcome.queue_part meaning "publish through the default queue's
#: exchange" (rule/task option only overrides the routing key).
_DEFAULT_PART = '__default_queue__'


class _Outcome:
    """What one rule/task-option contributes to the destination."""

    __slots__ = ('queue_part', 'exchange', 'routing_key', 'noop')

    def __init__(self, queue_part=None, exchange=None, routing_key=None,
                 noop=False):
        self.queue_part = queue_part
        self.exchange = exchange
        self.routing_key = routing_key
        self.noop = noop


def _normalize_value(value):
    """Mirror :meth:`Router.expand_destination` without mutating state.

    Returns ``(_Outcome, error_message)`` with exactly one item set.
    """
    if isinstance(value, str):
        return _Outcome(queue_part=value), None
    if isinstance(value, Queue):
        return _Outcome(queue_part=value), None
    if isinstance(value, Mapping):
        data = dict(value)
        queue_part = data.get('queue')
        exchange = data.get('exchange')
        routing_key = data.get('routing_key')
        if queue_part is not None and not isinstance(queue_part, (str, Queue)):
            return None, (
                f"'queue' must be a queue name or a kombu.Queue instance, "
                f"not {type(queue_part).__name__}: {queue_part!r}")
        if exchange is not None and not isinstance(exchange, (str, Exchange)):
            return None, (
                f"'exchange' must be an exchange name or a kombu.Exchange "
                f"instance, not {type(exchange).__name__}: {exchange!r}")
        if routing_key is not None and not isinstance(routing_key, str):
            return None, (
                f"'routing_key' must be a string, not "
                f"{type(routing_key).__name__}: {routing_key!r}")
        if queue_part is None and exchange is None:
            if routing_key is None:
                # expand_destination() returns an empty dict, which lookup()
                # treats as "no match" and falls through -- the rule never
                # wins any task.
                return _Outcome(noop=True), None
            return _Outcome(queue_part=_DEFAULT_PART,
                            routing_key=routing_key), None
        return _Outcome(queue_part=queue_part, exchange=exchange,
                        routing_key=routing_key), None
    if value is None:
        return None, ('route value is None (rules must resolve to a queue '
                      'name, a kombu.Queue or a mapping)')
    return None, (
        f'route value must be a queue name, a kombu.Queue instance or a '
        f'mapping, not {type(value).__name__}: {value!r}')


def _exchange_name(exchange):
    if exchange is None:
        return None
    if isinstance(exchange, Exchange):
        return exchange.name
    return exchange


def _queue_type(queue):
    args = getattr(queue, 'queue_arguments', None) or {}
    return args.get('x-queue-type', 'classic')


def _describe_auto_queue(name, queue):
    exchange = getattr(queue, 'exchange', None)
    return AutoQueue(
        name=name,
        queue_type=_queue_type(queue),
        exchange_name=exchange.name if exchange is not None else None,
        exchange_type=exchange.type if exchange is not None else None,
        routing_key=getattr(queue, 'routing_key', None),
        queue_arguments=dict(getattr(queue, 'queue_arguments', None) or {}),
    )


def _binding_mismatch(ref, task, target, exchange, routing_key):
    queue = target.queue
    if queue is None or queue.exchange is None:
        return None
    q_exchange = queue.exchange.name or ''
    q_routing_key = queue.routing_key
    route_exchange = _exchange_name(exchange)
    bad_fields = []
    if route_exchange is not None and route_exchange != q_exchange:
        bad_fields.append('exchange')
    if (routing_key is not None and routing_key != q_routing_key
            and queue.exchange.type != 'fanout'):
        bad_fields.append('routing_key')
    if not bad_fields:
        return None
    # Publishing without an explicit exchange to a direct exchange is
    # rewritten to the anonymous exchange with the queue name, so a custom
    # routing key is discarded and the message still reaches the queue.
    rewritten = (
        'routing_key' in bad_fields
        and queue.exchange.type == 'direct'
        and route_exchange is None
    )
    return BindingMismatch(
        rule=ref, task=task, queue_name=queue.name,
        route_exchange=route_exchange, route_routing_key=routing_key,
        queue_exchange=q_exchange, queue_routing_key=q_routing_key,
        rewritten_at_publish=rewritten,
    )


def inspect_routing(*, version, prepared_routes, queues, missing_factory,
                    consume_from, task_names, task_options,
                    create_missing, create_missing_queue_type,
                    create_missing_queue_exchange_type,
                    default_queue_name, implicit_default_queue):
    """Inspect a snapshot of the routing configuration.

    All arguments describe a point-in-time snapshot; this function only
    reads them and never touches a broker.

    Arguments:
        version: Opaque version tag of the snapshot (set by
            :class:`~celery.app.amqp.AMQP`).
        prepared_routes: Prepared router list from
            :func:`celery.app.routes.prepare`.
        queues: Mapping of declared queue name to :class:`kombu.Queue`.
        missing_factory: Side-effect free callable reproducing
            :meth:`celery.app.amqp.Queues.new_missing`; it may raise
            :exc:`ValueError` for an invalid configured queue type.
        consume_from: Iterable of queue names this process subscribes to.
        task_names: Known (registered) task names to probe.
        task_options: Mapping of task name to its non-``None`` execution
            options (``queue``/``exchange``/``routing_key``).
        create_missing: The ``task_create_missing_queues`` flag.
        create_missing_queue_type / create_missing_queue_exchange_type:
            Parameters used when implicitly creating queues.
        default_queue_name: The ``task_default_queue`` name.
        implicit_default_queue: The default :class:`kombu.Queue` created
            implicitly when no queues are declared at all, or
            :const:`None` when queues are declared.
    """
    declared = OrderedDict(queues)
    errors = []

    if (create_missing and create_missing_queue_type
            and create_missing_queue_type not in ('classic', 'quorum')):
        errors.append(StaticError(
            None, None, INVALID_QUEUE_TYPE,
            f"Invalid task_create_missing_queue_type "
            f"{create_missing_queue_type!r}: valid types are 'classic' "
            f"and 'quorum'"))

    # --- flatten the prepared routing table into static/dynamic rules ----
    static_rules = []   # (RuleRef, matcher, outcome|None, error|None)
    undecidable = []
    for index, router in enumerate(prepared_routes):
        if isinstance(router, MapRoute):
            for name, value in router.map.items():
                ref = RuleRef(index, 'exact', name)
                outcome, message = _normalize_value(value)
                static_rules.append((ref, name.__eq__, outcome, message))
            for regex, value in router.patterns.items():
                ref = RuleRef(index, 'pattern', regex.pattern)
                outcome, message = _normalize_value(value)
                static_rules.append((ref, regex.match, outcome, message))
        else:
            if isinstance(router, lazy):
                kind, reason = (
                    'named',
                    'router imported by name decides from the task name, '
                    'args and kwargs at runtime')
            elif hasattr(router, 'route_for_task'):
                kind, reason = (
                    'router-class',
                    'pre 4.0 router class decides from the task name, args '
                    'and kwargs at runtime')
            else:
                kind, reason = (
                    'callable',
                    'callable router decides from the task name, args and '
                    'kwargs at runtime')
            undecidable.append(
                UndecidableRule(index, router, kind, reason))

    # --- destination resolution shared by rules and task probes ----------
    reachable = set()
    auto_queues = OrderedDict()
    mismatches = []

    def add_reachable(target):
        if target is None or target.queue is None:
            return
        name = target.queue.name
        reachable.add(name)
        if target.auto_created and name not in auto_queues:
            auto_queues[name] = _describe_auto_queue(name, target.queue)

    def resolve_queue(queue_part, source, code):
        """Resolve a queue name/instance against the declared set."""
        if isinstance(queue_part, Queue):
            if queue_part.name in declared:
                queue = declared[queue_part.name]
                return Target(queue, False, 'declared'), None
            if create_missing:
                # A Queue instance carries its own declaration parameters;
                # the publisher would declare it. It is not declared here.
                return Target(queue_part, True, 'rule-supplied queue'), None
            return None, StaticError(
                None, None, code,
                f"{source} targets queue {queue_part.name!r}, which is not "
                f"declared in task_queues and "
                f"task_create_missing_queues is disabled")
        name = queue_part
        if name in declared:
            return Target(declared[name], False, 'declared'), None
        if create_missing:
            try:
                queue = missing_factory(name)
            except ValueError as exc:
                return None, StaticError(None, None, INVALID_QUEUE_TYPE,
                                         str(exc))
            return Target(queue, True, 'implicitly created'), None
        return None, StaticError(
            None, None, code,
            f"{source} targets queue {name!r}, which is not declared in "
            f"task_queues and task_create_missing_queues is disabled")

    def resolve_default():
        if default_queue_name in declared:
            return Target(declared[default_queue_name], False,
                          'declared default queue'), None
        if create_missing:
            if implicit_default_queue is not None:
                return Target(implicit_default_queue, True,
                              'implicit default queue'), None
            try:
                queue = missing_factory(default_queue_name)
            except ValueError as exc:
                return None, StaticError(None, None, INVALID_QUEUE_TYPE,
                                         str(exc))
            return Target(queue, True, 'implicitly created'), None
        return None, StaticError(
            None, None, DEFAULT_QUEUE_NOT_FOUND,
            f"the default queue {default_queue_name!r} is not declared in "
            f"task_queues and task_create_missing_queues is disabled")

    def resolve_outcome(outcome, source, missing_code):
        """Resolve a normalized rule/task-option outcome to a Target."""
        part = outcome.queue_part
        if part is None:
            return Target(None, False, EXCHANGE_ONLY), None
        if part == _DEFAULT_PART:
            return resolve_default()
        return resolve_queue(part, source, missing_code)

    # The default queue is always a reachable destination: send_task() can
    # be invoked with names that are not registered locally.
    default_target, default_error = resolve_default()
    if default_error is not None:
        errors.append(default_error)
    else:
        add_reachable(default_target)

    # --- resolve every rule's destination exactly once -------------------
    # A rule destination is fixed by the configuration, so it is validated
    # independently of which task names are currently registered -- a
    # client can call send_task() with any name.
    rule_outcomes = {}   # ref -> normalized _Outcome
    rule_targets = {}    # ref -> resolved Target

    for ref, _matcher, outcome, message in static_rules:
        if message is not None:
            errors.append(StaticError(
                ref, None, INVALID_ROUTE_VALUE,
                f"{rule_source(ref)} has an invalid route value: {message}"))
            continue
        if outcome.noop:
            continue
        rule_outcomes[ref] = outcome
        target, error = resolve_outcome(
            outcome, rule_source(ref), QUEUE_NOT_FOUND)
        if error is not None:
            errors.append(error)
            continue
        rule_targets[ref] = target
        add_reachable(target)
        mismatch = _binding_mismatch(
            ref, None, target, outcome.exchange, outcome.routing_key)
        if mismatch is not None:
            mismatches.append(mismatch)

    # --- probe every known task name, keeping evaluation order -----------
    resolutions = []
    matched_by = OrderedDict((ref, []) for ref, _, _, _ in static_rules)
    won_by = set()
    ineffective = OrderedDict()   # ref -> list of task names

    for task in sorted(task_names):
        base = {k: v for k, v in (task_options.get(task) or {}).items()
                if v is not None and k in ('queue', 'exchange',
                                           'routing_key')}
        base_outcome = None
        if base:
            base_outcome, message = _normalize_value(base)
            if message is not None:
                errors.append(StaticError(
                    None, task, INVALID_QUEUE_PART,
                    f"execution options of task {task!r} are invalid: "
                    f"{message}"))
                base_outcome = None
        base_source = f"execution options of task {task!r}"

        winner = None
        losers = []
        ineffective_hits = []

        for ref, matcher, outcome, message in static_rules:
            if message is not None or not matcher(task):
                continue
            if outcome.noop:
                # The rule matches but yields no destination, so routing
                # falls through -- ineffective, not shadowed.
                ineffective_hits.append(ref)
                continue
            matched_by[ref].append(task)
            if winner is None:
                winner = ref
                won_by.add(ref)
            else:
                losers.append(RuleLoss(
                    ref,
                    f'matches but {rule_source(winner)} earlier in '
                    f'task_routes takes precedence'))
        for ref in ineffective_hits:
            ineffective.setdefault(ref, []).append(task)

        # Final destination, preserving priority:
        # winning rule > task execution options > default queue fallback.
        target = None
        explicit_exchange = None
        explicit_routing_key = None
        task_error = None
        if winner is not None:
            outcome = rule_outcomes.get(winner)
            target = rule_targets.get(winner)
            if outcome is not None:
                explicit_exchange = outcome.exchange
                explicit_routing_key = outcome.routing_key
        elif base_outcome is not None:
            target, task_error = resolve_outcome(
                base_outcome, base_source, INVALID_QUEUE_PART)
            explicit_exchange = base_outcome.exchange
            explicit_routing_key = base_outcome.routing_key
            if task_error is None and target is not None:
                add_reachable(target)
                mismatch = _binding_mismatch(
                    None, task, target, explicit_exchange,
                    explicit_routing_key)
                if mismatch is not None:
                    mismatches.append(mismatch)
        if target is None and task_error is None and winner is None:
            target = default_target
        if (task_error is not None
                and task_error.code != DEFAULT_QUEUE_NOT_FOUND):
            # A missing default queue is reported once globally; per-task
            # copies would only repeat the same misconfiguration.
            errors.append(task_error)

        note = UNDECIDABLE_NOTE.format(n=len(undecidable)) \
            if undecidable else None
        resolutions.append(TaskResolution(
            task=task, winner=winner, target=target,
            explicit_exchange=explicit_exchange,
            explicit_routing_key=explicit_routing_key,
            losers=tuple(losers), note=note,
        ))

    # --- aggregate hints --------------------------------------------------
    shadowed = []
    unmatched = []
    for ref, tasks in matched_by.items():
        if tasks:
            if ref not in won_by:
                shadowed_by = OrderedDict()
                for resolution in resolutions:
                    if resolution.task in tasks and resolution.winner:
                        shadowed_by.setdefault(
                            resolution.winner, None)
                shadowed.append(ShadowedRule(
                    ref, tuple(tasks), tuple(shadowed_by)))
        else:
            unmatched.append(UnmatchedRule(ref))

    ineffective_findings = [
        IneffectiveRule(ref, tuple(tasks), NOOP_REASON)
        for ref, tasks in ineffective.items()
    ]

    # De-duplicate findings: e.g. several tasks falling through the same
    # broken rule produce the same static conclusion.
    errors = list(OrderedDict((error, None) for error in errors))

    subscribed = set(consume_from)
    subscription_diff = SubscriptionDiff(
        reachable_only=tuple(sorted(reachable - subscribed)),
        subscribed_only=tuple(sorted(subscribed - reachable)),
    )

    return RouteReport(
        version=version,
        resolutions=tuple(resolutions),
        errors=tuple(errors),
        auto_queues=tuple(auto_queues.values()),
        mismatches=tuple(mismatches),
        shadowed=tuple(shadowed),
        unmatched=tuple(unmatched),
        ineffective=tuple(ineffective_findings),
        undecidable=tuple(undecidable),
        subscription_diff=subscription_diff,
        default_target=default_target,
        subscribed=tuple(sorted(subscribed)),
        reachable=tuple(sorted(reachable)),
    )


# --- human-readable rendering ---------------------------------------------

def _target_text(target, exchange=None, routing_key=None):
    if target is None:
        return 'unresolved'
    if target.queue is None:
        bits = [EXCHANGE_ONLY]
        if exchange is not None:
            bits.append(f"exchange={_exchange_name(exchange)!r}")
        if routing_key is not None:
            bits.append(f"routing_key={routing_key!r}")
        return ', '.join(bits)
    queue = target.queue
    ex = queue.exchange.name if queue.exchange is not None else None
    ex_type = queue.exchange.type if queue.exchange is not None else '?'
    created = ' (would be auto-created)' if target.auto_created else ''
    return (f"queue={queue.name!r}{created} "
            f"exchange={ex}({ex_type}) "
            f"binding_key={queue.routing_key!r}")


def format_errors(report):
    """Format the fatal findings of *report* for an exception message."""
    lines = ['Task routing validation failed:']
    for error in report.errors:
        if error.rule:
            source = rule_source(error.rule)
        elif error.task:
            source = f"execution options of task {error.task!r}"
        else:
            source = 'routing configuration'
        lines.append(f"- [{error.code}] {source}: {error.message}")
    lines.append(
        '\nFix the routing configuration or disable the check with '
        'task_routes_validate=False.')
    return '\n'.join(lines)


def format_report(report):
    """Render the full static routing report for startup logging."""
    lines = ['[task routing gate] static analysis results:']

    lines.append('')
    lines.append('Per-task resolution (evaluation order preserved):')
    if not report.resolutions:
        lines.append('  (no registered tasks to probe)')
    for resolution in report.resolutions:
        lines.append(
            f"- task {resolution.task!r}: winner="
            f"{rule_source(resolution.winner)} -> "
            f"{_target_text(resolution.target, resolution.explicit_exchange,
                            resolution.explicit_routing_key)}")
        for loss in resolution.losers:
            lines.append(
                f"    loses: {rule_source(loss.rule)} ({loss.reason})")
        if resolution.note:
            lines.append(f"    note: {resolution.note}")

    lines.append('')
    lines.append('Queues that would be auto-created on first publish:')
    if report.auto_queues:
        for queue in report.auto_queues:
            line = (
                f"- {queue.name!r}: type={queue.queue_type} "
                f"exchange={queue.exchange_name!r}({queue.exchange_type}) "
                f"routing_key={queue.routing_key!r}")
            if queue.queue_arguments:
                line += f" arguments={queue.queue_arguments}"
            lines.append(line)
    else:
        lines.append('  (none)')

    lines.append('')
    lines.append('Routing key / exchange disagreements with queue bindings:')
    if report.mismatches:
        for mismatch in report.mismatches:
            source = rule_source(mismatch.rule) if mismatch.rule else \
                'task execution options'
            lines.append(
                f"- {source} -> {mismatch.queue_name!r}: "
                f"route(exchange={mismatch.route_exchange!r}, "
                f"routing_key={mismatch.route_routing_key!r}) vs binding("
                f"exchange={mismatch.queue_exchange!r}, "
                f"routing_key={mismatch.queue_routing_key!r})"
                + (' [rewritten to the queue name at publish time; harmless]'
                   if mismatch.rewritten_at_publish else
                   ' [message may never reach the queue]'))
    else:
        lines.append('  (none)')

    lines.append('')
    lines.append('Subscription set vs routing-reachable queues:')
    diff = report.subscription_diff
    if diff.reachable_only:
        lines.append(
            "- routed but not consumed by this worker: "
            + ', '.join(diff.reachable_only))
    if diff.subscribed_only:
        lines.append(
            "- consumed but no known task routes here: "
            + ', '.join(diff.subscribed_only))
    if not diff.reachable_only and not diff.subscribed_only:
        lines.append('  (subscribed and reachable sets match)')

    lines.append('')
    lines.append('Fully shadowed rules (hints):')
    if report.shadowed:
        for item in report.shadowed:
            by = ', '.join(rule_source(ref) for ref in item.shadowed_by)
            lines.append(
                f"- {rule_source(item.rule)} never wins for tasks "
                f"{', '.join(map(repr, item.tasks))}; shadowed by: {by}")
    else:
        lines.append('  (none)')

    lines.append('')
    lines.append('Rules without a matching registered task name (hints):')
    if report.unmatched:
        for item in report.unmatched:
            lines.append(f"- {rule_source(item.rule)}")
    else:
        lines.append('  (none)')

    if report.ineffective:
        lines.append('')
        lines.append('Ineffective matches (hints):')
        for item in report.ineffective:
            lines.append(
                f"- {rule_source(item.rule)} matched by tasks "
                f"{', '.join(map(repr, item.tasks))}: {item.reason}")

    lines.append('')
    lines.append('Rules that cannot be decided statically:')
    if report.undecidable:
        for item in report.undecidable:
            name = getattr(item.router, '__name__', repr(item.router))
            lines.append(
                f"- task_routes[{item.router_index}] ({item.kind} {name}): "
                f"{item.reason}")
    else:
        lines.append('  (none)')

    return '\n'.join(lines)
