"""Event receiver implementation."""
import time
from operator import itemgetter

from kombu import Queue
from kombu.connection import maybe_channel
from kombu.mixins import ConsumerMixin

from celery import uuid
from celery.app import app_or_default
from celery.exceptions import ImproperlyConfigured
from celery.utils.time import adjust_timestamp

from .continuity import ContinuityTracker
from .event import get_exchange

__all__ = ('EventReceiver',)

CLIENT_CLOCK_SKEW = -1

_TZGETTER = itemgetter('utcoffset', 'timestamp')


class EventReceiver(ConsumerMixin):
    """Capture events.

    Arguments:
        channel (kombu.Channel): Channel to consume events on. A
            :class:`kombu.Connection` is also accepted, in which case its
            default channel is used.
        handlers (Mapping[Callable]): Event handlers.
            This is  a map of event type names and their handlers.
            The special handler `"*"` captures all events that don't have a
            handler.
        continuity (ContinuityTracker | bool): Continuity tracker used to
            order events by session/sequence number, detect gaps and drive
            state resynchronization.  Defaults to :const:`None`, which
            enables tracking automatically when
            :setting:`event_continuity_enabled` is set.  Pass :const:`False`
            to force it off, or a pre-configured
            :class:`~celery.events.continuity.ContinuityTracker`.
    """

    app = None

    def __init__(self, channel, handlers=None, routing_key='#',
                 node_id=None, app=None, queue_prefix=None,
                 accept=None, queue_ttl=None, queue_expires=None,
                 queue_exclusive=None,
                 queue_durable=None, continuity=None):
        self.app = app_or_default(app or self.app)
        self.channel = maybe_channel(channel)
        self.handlers = {} if handlers is None else handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue_prefix = queue_prefix or self.app.conf.event_queue_prefix
        self.exchange = get_exchange(
            self.connection or self.app.connection_for_write(),
            name=self.app.conf.event_exchange)
        if queue_ttl is None:
            queue_ttl = self.app.conf.event_queue_ttl
        if queue_expires is None:
            queue_expires = self.app.conf.event_queue_expires
        if queue_exclusive is None:
            queue_exclusive = self.app.conf.event_queue_exclusive
        if queue_durable is None:
            queue_durable = self.app.conf.event_queue_durable
        if queue_exclusive and queue_durable:
            raise ImproperlyConfigured(
                'Queue cannot be both exclusive and durable, '
                'choose one or the other.'
            )
        self.queue = Queue(
            '.'.join([self.queue_prefix, self.node_id]),
            exchange=self.exchange,
            routing_key=self.routing_key,
            auto_delete=not queue_durable,
            durable=queue_durable,
            exclusive=queue_exclusive,
            message_ttl=queue_ttl,
            expires=queue_expires,
        )
        self.clock = self.app.clock
        self.adjust_clock = self.clock.adjust
        self.forward_clock = self.clock.forward
        if accept is None:
            accept = {self.app.conf.event_serializer, 'json'}
        self.accept = accept
        if continuity is False:
            self.continuity = None
        elif isinstance(continuity, ContinuityTracker):
            self.continuity = continuity
        elif continuity is None and self.app.conf.event_continuity_enabled:
            # Validates the continuity settings as well, raising before the
            # monitor starts serving when a value is invalid.
            self.continuity = ContinuityTracker.from_app(self.app)
        else:
            self.continuity = None

    def process(self, type, event):
        """Process event by dispatching to configured handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        handler and handler(event)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=True,
                         accept=self.accept)]

    def on_consume_ready(self, connection, channel, consumers,
                         wakeup=True, **kwargs):
        if wakeup:
            self.wakeup_workers(channel=channel)

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        return self.consume(limit=limit, timeout=timeout, wakeup=wakeup)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never stop
        unless :attr:`EventDispatcher.should_stop` is set to True, or
        forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.
        """
        for _ in self.consume(limit=limit, timeout=timeout, wakeup=wakeup):
            pass

    def wakeup_workers(self, channel=None):
        self.app.control.broadcast('heartbeat',
                                   connection=self.connection,
                                   channel=channel)

    def event_from_message(self, body, localize=True,
                           now=time.time, tzfields=_TZGETTER,
                           adjust_timestamp=adjust_timestamp,
                           CLIENT_CLOCK_SKEW=CLIENT_CLOCK_SKEW):
        type = body['type']
        if type == 'task-sent':
            # clients never sync so cannot use their clock value
            _c = body['clock'] = (self.clock.value or 1) + CLIENT_CLOCK_SKEW
            self.adjust_clock(_c)
        else:
            try:
                clock = body['clock']
            except KeyError:
                body['clock'] = self.forward_clock()
            else:
                self.adjust_clock(clock)

        if localize:
            try:
                offset, timestamp = tzfields(body)
            except KeyError:
                pass
            else:
                body['timestamp'] = adjust_timestamp(timestamp, offset)
        body['local_received'] = now()
        return type, body

    def _receive(self, body, message, list=list, isinstance=isinstance):
        if isinstance(body, list):  # celery 4.0+: List of events
            # Batch messages preserve their intra-list order; the
            # continuity tracker needs to see the events in that order.
            [self._dispatch(self.event_from_message(event))
             for event in body]
        else:
            self._dispatch(self.event_from_message(body))

    def _dispatch(self, received):
        tracker = self.continuity
        if tracker is None:
            self.process(*received)
            return
        type, event = received
        # The tracker holds out-of-order events, drops duplicates and may
        # return previously held (or post-resync) events; every delivered
        # event is dispatched in per-session sequence order. Events from
        # old workers (without session/seq) pass through unchanged.
        for delivered in tracker.observe(event):
            self.process(delivered['type'], delivered)

    @property
    def connection(self):
        return self.channel.connection.client if self.channel else None
