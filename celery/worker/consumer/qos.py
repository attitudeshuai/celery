"""Per-queue prefetch depth management for the worker task consumer.

The worker's prefetch window is normally a single channel-global integer
handed to the broker (see :class:`kombu.common.QoS`).  All consumed queues
share that window, so a single backed-up queue can starve every other queue
consumed by the same worker process.

This module adds *per-queue* prefetch caps on top of the same machinery:

* when **no** per-queue caps are declared, :class:`QoS` behaves exactly like
  :class:`kombu.common.QoS` -- one channel-global integer, broker call
  sequence unchanged;
* when at least one cap is declared (at startup via
  :setting:`worker_queue_prefetch_limits` or at runtime via the
  ``set_queue_prefetch`` remote control command), each queue is bound with
  its own per-consumer ``basic.qos`` window (``a_global=False``), applied
  right before its ``basic.consume``;
* queues without a cap follow the *automatic* value
  (``concurrency * worker_prefetch_multiplier``), queues with a cap take
  ``min(cap, automatic)``;
* when a cap is lowered, the number of messages already prefetched by the
  process but not yet handed to the execution pool (``held``) forms a hard
  floor: the window can only be reclaimed down to ``held`` -- already
  delivered, unacknowledged messages are never requeued, lost or
  duplicated and stay acknowledgeable before and after the adjustment;
* caps are stored independently of scale/restore recomputations, connection
  rebuilds and concurrent runtime adjustments, so none of those can wipe
  them.
"""
from __future__ import annotations

from weakref import WeakKeyDictionary

from kombu.common import PREFETCH_COUNT_MAX, QoS as _BaseQoS

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

__all__ = (
    'QoS',
    'validate_queue_prefetch_limit',
    'validate_queue_prefetch_limits',
)

logger = get_logger(__name__)

#: Attribute used to stamp a received message with its source queue.
MESSAGE_QUEUE_ATTR = '_celery_prefetch_queue'


def validate_queue_prefetch_limits(limits):
    """Validate a ``{queue_name: prefetch_cap}`` mapping.

    Returns a new dict with the validated entries.  Illegal values raise
    :exc:`~celery.exceptions.ImproperlyConfigured` so that misconfiguration
    fails loudly during worker startup instead of being silently ignored.

    Arguments:
        limits (Mapping[str, int] or None): per-queue prefetch caps.
            Every value must be a positive integer not exceeding the
            AMQP ``prefetch_count`` limit (``0xFFFF``).
    """
    if limits is None:
        return {}
    if not isinstance(limits, dict):
        raise ImproperlyConfigured(
            "worker_queue_prefetch_limits must be a mapping of queue names "
            f"to positive integers, got object of type "
            f"{type(limits).__name__}: {limits!r}"
        )
    validated = {}
    for queue, limit in limits.items():
        if not isinstance(queue, str) or not queue:
            raise ImproperlyConfigured(
                "worker_queue_prefetch_limits keys must be non-empty queue "
                f"name strings, got: {queue!r}"
            )
        # ``bool`` is a subclass of ``int`` -- reject it explicitly so
        # ``True``/``False`` are not accepted as prefetch values.
        if (isinstance(limit, bool) or not isinstance(limit, int)
                or limit < 1 or limit > PREFETCH_COUNT_MAX):
            raise ImproperlyConfigured(
                f"worker_queue_prefetch_limits[{queue!r}] must be an integer "
                f"between 1 and {PREFETCH_COUNT_MAX}, got: {limit!r}"
            )
        validated[queue] = limit
    return validated


def validate_queue_prefetch_limit(queue, limit):
    """Validate a single ``(queue, limit)`` pair.

    Same rules as :func:`validate_queue_prefetch_limits`, but raises plain
    :exc:`ValueError` for values rejected at runtime (e.g. coming from a
    remote control command), so the caller can refuse the request
    truthfully instead of treating it as startup misconfiguration.
    """
    try:
        validate_queue_prefetch_limits({queue: limit})
    except ImproperlyConfigured as exc:
        raise ValueError(str(exc))


class QoS(_BaseQoS):
    """Thread-safe QoS manager with optional per-queue prefetch caps.

    Legacy mode (the default, no caps declared) is byte-for-byte compatible
    with :class:`kombu.common.QoS`: ``value``/``prev``, :meth:`set`,
    :meth:`update`, :meth:`increment_eventually` and
    :meth:`decrement_eventually` keep their global-integer semantics.

    Per-queue mode tracks, for every consumed queue:

    * ``limit`` -- the configured cap (persistent across reconnects) or
      ``None``;
    * ``target`` -- the desired window: ``automatic`` for uncapped queues
      and ``min(limit, automatic)`` for capped ones;
    * ``actual`` -- the last window applied to the broker, always
      ``>= held``;
    * ``held`` -- messages prefetched in-process but not yet handed to the
      execution pool (provided by the consumer);
    * ``borrowed`` -- extra credit handed out for held ETA/rate-limited
      messages, mirroring the legacy ``increment_eventually``/
      ``decrement_eventually`` bookkeeping.

    All state is guarded by :attr:`_mutex` (replaced with a dummy lock on
    event-loop workers, where every caller already runs in a single
    thread), so concurrent runtime adjustments and autoscale recomputations
    are serialised and can never interleave into a torn target/actual
    pair.
    """

    def __init__(self, callback, initial_value, max_prefetch=None,
                 queue_limits=None):
        super().__init__(callback, initial_value, max_prefetch)
        # The same mapping instance is kept on the consumer and mutated in
        # place, so caps survive pool scaling and connection rebuilds.
        self._queue_limits = validate_queue_prefetch_limits(queue_limits)
        self._per_queue_enabled = False
        self._targets = {}
        self._actuals = {}
        self._borrowed = {}
        self._dirty = set()
        self._auto_provider = None
        self._held_provider = None
        self._apply = None
        # Fallback attribution registry for messages that cannot be
        # stamped directly (objects with __slots__).
        self._message_queues = WeakKeyDictionary()

    # -- mode --------------------------------------------------------------

    @property
    def per_queue_enabled(self):
        """:const:`True` when per-queue prefetch windows are in effect."""
        return self._per_queue_enabled

    @property
    def queue_limits(self):
        """The persistent ``{queue: cap}`` mapping (``{}`` = no caps)."""
        return self._queue_limits

    def configure(self, *, auto_provider, held_provider, apply,
                  initial_value=None, limits=None):
        """Switch from legacy mode to per-queue mode.

        Arguments:
            auto_provider (Callable[[], int]): returns the current
                automatic prefetch value.
            held_provider (Callable[[str], int]): returns the number of
                prefetched-but-not-handed messages held for a queue.
            apply (Callable[[str, int], None]): binds ``prefetch_count``
                to the given queue on the broker, cancelling/re-consuming
                the queue's consumer when it is already running.  Raising
                from this call signals that the broker did not accept the
                target.
            initial_value (int): optional value to seed the version
                counter with.
            limits (Mapping): an already-validated mapping to share with
                the caller (the consumer).  The same instance is mutated
                by runtime adjustments so caps survive connection
                rebuilds.
        """
        with self._mutex:
            self._auto_provider = auto_provider
            self._held_provider = held_provider
            self._apply = apply
            if limits is not None:
                self._queue_limits = limits
            self._per_queue_enabled = True
            # ``value``/``prev`` double as a version counter in per-queue
            # mode: every state change bumps ``value`` so the worker event
            # loop's ``qos.prev != qos.value`` check flushes pending broker
            # updates via :meth:`update`.
            self.value = self.prev = initial_value or 0
            self._targets = {}
            self._actuals = {}
            self._borrowed = {}
            self._dirty = set()

    # -- message -> queue attribution -------------------------------------

    def bind_message(self, message, active_tags=None):
        """Remember which queue a freshly received message came from.

        No-op in legacy mode.  ``active_tags`` is the task consumer's
        ``{queue_name: consumer_tag}`` mapping snapshot.  The resolved
        name is stamped on the message object itself (with a weak-key
        fallback), so attribution stays correct after the consumer tag is
        replaced by a runtime cancel/re-consume.
        """
        if not self._per_queue_enabled or message is None:
            return
        try:
            delivery_info = getattr(message, 'delivery_info', None) or {}
            queue = None
            consumer_tag = delivery_info.get('consumer_tag')
            if consumer_tag is not None and active_tags:
                for name, tag in active_tags.items():
                    if tag == consumer_tag:
                        queue = name
                        break
            if queue is None:
                # Virtual transports do not stamp ``consumer_tag``; the
                # routing key of a task message equals the queue binding.
                queue = delivery_info.get('routing_key')
            if queue:
                try:
                    setattr(message, MESSAGE_QUEUE_ATTR, queue)
                except (AttributeError, TypeError):
                    self._message_queues[message] = queue
        except Exception:  # pylint: disable=broad-except
            logger.debug('could not attribute message to a queue',
                         exc_info=True)

    def queue_of_message(self, message):
        """Return the queue name a received :class:`kombu.Message` came from."""
        if message is None:
            return None
        queue = getattr(message, MESSAGE_QUEUE_ATTR, None)
        if queue is not None:
            return queue
        try:
            queue = self._message_queues.get(message)
            if queue is not None:
                return queue
        except (TypeError, AttributeError):
            pass
        delivery_info = getattr(message, 'delivery_info', None) or {}
        return delivery_info.get('routing_key')

    # -- values ------------------------------------------------------------

    def _auto(self):
        try:
            auto = int(self._auto_provider() or 0)
        except Exception:  # pylint: disable=broad-except
            logger.debug('automatic prefetch provider failed', exc_info=True)
            auto = self.value or 1
        # Zero would mean "unlimited" on AMQP; values above the AMQP short
        # field would close the channel, so clamp to the legal range.
        return min(max(auto, 1), PREFETCH_COUNT_MAX)

    def _held(self, queue):
        try:
            return max(int(self._held_provider(queue) or 0), 0)
        except Exception:  # pylint: disable=broad-except
            logger.debug('held-message provider failed for queue %r',
                         queue, exc_info=True)
            return 0

    def _desired(self, queue):
        auto = self._auto()
        cap = self._queue_limits.get(queue)
        return auto if cap is None else min(cap, auto)

    def _compute_actual(self, queue):
        # The window cannot be reclaimed below the number of messages
        # already held in-process: those deliveries are unacknowledged and
        # physically occupy the credit until they are handed over or
        # acknowledged.
        return max(self._targets.get(queue, self._desired(queue))
                   + self._borrowed.get(queue, 0),
                   self._held(queue))

    def activate_queue(self, queue):
        """Register a queue before its first per-consumer ``basic.consume``.

        Returns the window the broker must be configured with.
        """
        with self._mutex:
            if queue not in self._targets:
                self._targets[queue] = self._desired(queue)
                self._borrowed.setdefault(queue, 0)
            actual = self._compute_actual(queue)
            self._actuals[queue] = actual
            self._dirty.discard(queue)
            return actual

    def deactivate_queue(self, queue):
        """Forget per-queue state of a queue the worker stopped consuming.

        Configured caps are deliberately kept so re-adding the queue
        restores them.
        """
        with self._mutex:
            self._targets.pop(queue, None)
            self._actuals.pop(queue, None)
            self._borrowed.pop(queue, None)
            self._dirty.discard(queue)

    def set_limit(self, queue, limit):
        """Set or replace the runtime cap of one queue and apply it.

        Raises:
            ValueError: if the value is illegal, the queue is not being
                consumed or the broker rejected the target.
        """
        validate_queue_prefetch_limit(queue, limit)
        with self._mutex:
            self._require_queue(queue)
            old_limit = self._queue_limits.get(queue)
            old_target = self._targets.get(queue)
            self._queue_limits[queue] = limit
            self._targets[queue] = self._desired(queue)
            try:
                self._apply_now(queue)
            except Exception:
                # Honest rollback: never report success for a target the
                # broker did not accept.
                if old_limit is None:
                    self._queue_limits.pop(queue, None)
                else:
                    self._queue_limits[queue] = old_limit
                self._targets[queue] = old_target
                raise
            return self._report_locked(queue)

    def clear_limit(self, queue):
        """Remove a queue's cap so it follows the automatic value."""
        with self._mutex:
            self._require_queue(queue)
            if queue not in self._queue_limits:
                return self._report_locked(queue)
            old_limit = self._queue_limits.pop(queue)
            self._targets[queue] = self._desired(queue)
            try:
                self._apply_now(queue)
            except Exception:
                self._queue_limits[queue] = old_limit
                self._targets[queue] = min(old_limit, self._auto())
                raise
            return self._report_locked(queue)

    def _require_queue(self, queue):
        if not self._per_queue_enabled:
            raise ValueError(
                'per-queue prefetch limits are not enabled; declare '
                'worker_queue_prefetch_limits at startup or set a limit '
                'at runtime first'
            )
        if queue not in self._targets:
            raise ValueError(f"worker is not consuming from queue {queue!r}")

    def _apply_now(self, queue):
        actual = self._compute_actual(queue)
        self._apply(queue, actual)
        self._actuals[queue] = actual
        self._dirty.discard(queue)
        self.value += 1
        self.prev = self.value

    def recompute(self):
        """Recompute all targets from the current automatic value.

        Used by pool grow/shrink and by post-reconnect restoration.
        Configured caps are inputs (never overwritten); capped queues whose
        cap stays below the automatic value are left untouched.
        """
        with self._mutex:
            if not self._per_queue_enabled:
                return
            for queue in list(self._targets):
                target = self._desired(queue)
                if target != self._targets[queue]:
                    self._targets[queue] = target
                    self._dirty.add(queue)
            if self._dirty:
                self.value += 1

    def flush(self):
        """Apply pending per-queue changes to the broker.

        Safe to call from the I/O thread (worker event loop).  Queues
        whose broker call fails stay dirty and are retried on the next
        flush.
        """
        with self._mutex:
            for queue in list(self._dirty):
                actual = self._compute_actual(queue)
                try:
                    self._apply(queue, actual)
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        'Failed to apply prefetch target %s to queue %r; '
                        'will retry', actual, queue,
                    )
                    # Keep the queue dirty and nudge the version counter so
                    # the event loop retries instead of going idle.
                    self.value += 1
                    continue
                self._actuals[queue] = actual
                self._dirty.discard(queue)
            if not self._dirty:
                self.prev = self.value

    # -- legacy-compatible global bookkeeping ------------------------------

    def update(self):
        """Update the broker with current QoS state (event loop hook)."""
        if not self._per_queue_enabled:
            return super().update()
        return self.flush()

    def increment_eventually(self, n=1, queue=None):
        """Lend extra prefetch credit (held ETA/rate-limited messages).

        Legacy mode keeps the global-integer semantics of
        :class:`kombu.common.QoS`.  In per-queue mode a queue-less call
        does not move the (now unused) global counter; queue attribution
        goes through :meth:`note_borrow`.
        """
        with self._mutex:
            if not self._per_queue_enabled or queue is None:
                if not self._per_queue_enabled:
                    return super().increment_eventually(n)
                return self.value
            self._borrow(queue, max(n, 0))
        return self.value

    def decrement_eventually(self, n=1, queue=None):
        """Return previously lent prefetch credit."""
        with self._mutex:
            if not self._per_queue_enabled or queue is None:
                if not self._per_queue_enabled:
                    return super().decrement_eventually(n)
                return self.value
            self._return(queue, n)
        return self.value

    def note_borrow(self, message):
        """Attribute a global ``increment_eventually`` to a message's queue.

        Called together with the legacy :meth:`increment_eventually` so
        the legacy call keeps working unchanged while per-queue mode can
        lend the credit to the source queue only.  No-op in legacy mode.
        """
        if not self._per_queue_enabled:
            return
        queue = self.queue_of_message(message)
        if queue is not None:
            with self._mutex:
                self._borrow(queue, 1)

    def note_return(self, message):
        """Counterpart of :meth:`note_borrow` when the message is handed."""
        if not self._per_queue_enabled:
            return
        queue = self.queue_of_message(message)
        if queue is not None:
            with self._mutex:
                self._return(queue, 1)

    def _borrow(self, queue, n):
        # Caller must hold ``self._mutex``.
        if queue in self._borrowed:
            self._borrowed[queue] += max(n, 0)
            self._dirty.add(queue)
            self.value += 1

    def _return(self, queue, n):
        # Caller must hold ``self._mutex``.
        borrowed = self._borrowed.get(queue)
        if borrowed:
            self._borrowed[queue] = max(0, borrowed - n)
            self._dirty.add(queue)
            self.value += 1

    # -- reporting ----------------------------------------------------------

    def _report_locked(self, queue):
        target = self._targets[queue]
        actual = self._actuals.get(queue)
        held = self._held(queue)
        return {
            'queue': queue,
            'limit': self._queue_limits.get(queue),
            'auto': self._auto(),
            'target': target,
            'actual': actual if actual is not None
            else max(target, held),
            'held': held,
            # Credit still occupied by held messages beyond the target.
            'reclaimable': max(0, (actual if actual is not None
                                   else max(target, held)) - target),
        }

    def info(self):
        """Report target/actual (and supporting) values for every queue."""
        with self._mutex:
            if not self._per_queue_enabled:
                return {
                    'mode': 'global',
                    'prefetch_count': self.value,
                    'queues': {},
                }
            auto = self._auto()
            queues = {}
            for queue in sorted(self._targets):
                target = self._targets[queue]
                held = self._held(queue)
                actual = self._actuals.get(queue)
                if actual is None:
                    actual = max(target, held)
                queues[queue] = {
                    'queue': queue,
                    'limit': self._queue_limits.get(queue),
                    'auto': auto,
                    'target': target,
                    'actual': actual,
                    'held': held,
                    'reclaimable': max(0, actual - target),
                }
            return {
                'mode': 'per-queue',
                'prefetch_count': sum(self._actuals.values()),
                'queues': queues,
            }
