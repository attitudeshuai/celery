"""Event dispatcher sends events."""

import os
import threading
import time
from collections import defaultdict, deque

from kombu import Producer

from celery import uuid
from celery.app import app_or_default
from celery.utils.nodenames import anon_nodename
from celery.utils.time import utcoffset

from .continuity import validate_continuity_settings
from .event import Event, get_exchange, group_from

__all__ = ('EventDispatcher',)


class _ContinuityRegistry:
    """Process-wide continuity session/seq allocation.

    Keyed by pid so that the session id and the sequence counter survive
    broker reconnects (which recreate the dispatcher in the same process)
    while a forked child or a restarted worker process starts a brand new
    session with its own counter.
    """

    def __init__(self):
        self._mutex = threading.Lock()
        self._sessions = {}
        self._counters = {}

    def allocate(self, pid):
        """Return ``(session_id, next_seq)`` for ``pid``.

        Sequence numbers are strictly monotonic and never repeat within a
        session, regardless of batch sends, remote event toggling or
        offline buffering.
        """
        with self._mutex:
            try:
                seq = self._counters[pid] + 1
            except KeyError:
                self._sessions[pid] = uuid()
                seq = 1
            self._counters[pid] = seq
            return self._sessions[pid], seq

    def current(self, pid):
        """Return ``(session_id, last_seq)`` for ``pid``.

        ``session_id`` may be :const:`None` when no seq was allocated yet.
        """
        with self._mutex:
            return self._sessions.get(pid), self._counters.get(pid, 0)

    def reset(self):
        """Forget all sessions (used by tests)."""
        with self._mutex:
            self._sessions.clear()
            self._counters.clear()


#: Process-wide registry shared by every dispatcher in this process.
process_continuity = _ContinuityRegistry()


class EventDispatcher:
    """Dispatches event messages.

    Arguments:
        connection (kombu.Connection): Connection to the broker.

        hostname (str): Hostname to identify ourselves as,
            by default uses the hostname returned by
            :func:`~celery.utils.anon_nodename`.

        groups (Sequence[str]): List of groups to send events for.
            :meth:`send` will ignore send requests to groups not in this list.
            If this is :const:`None`, all events will be sent.
            Example groups include ``"task"`` and ``"worker"``.

        enabled (bool): Set to :const:`False` to not actually publish any
            events, making :meth:`send` a no-op.

        channel (kombu.Channel): Can be used instead of `connection` to specify
            an exact channel to use when sending events.

        buffer_while_offline (bool): If enabled events will be buffered
            while the connection is down. :meth:`flush` must be called
            as soon as the connection is re-established.

    Note:
        You need to :meth:`close` this after use.
    """

    DISABLED_TRANSPORTS = {'sql'}

    #: Max events kept in the offline outbound buffer when the continuity
    #: channel is enabled; beyond this the oldest buffered events are
    #: dropped (and recorded) instead of growing memory without bound.
    CONTINUITY_OUTBOUND_LIMIT = 10000

    #: How many dropped sequence numbers to remember for a snapshot reply.
    CONTINUITY_DROPPED_LEDGER = 1000

    app = None

    # set of callbacks to be called when :meth:`enabled`.
    on_enabled = None

    # set of callbacks to be called when :meth:`disabled`.
    on_disabled = None

    def __init__(self, connection=None, hostname=None, enabled=True,
                 channel=None, buffer_while_offline=True, app=None,
                 serializer=None, groups=None, delivery_mode=1,
                 buffer_group=None, buffer_limit=24, on_send_buffered=None):
        self.app = app_or_default(app or self.app)
        self.connection = connection
        self.channel = channel
        self.hostname = hostname or anon_nodename()
        self.buffer_while_offline = buffer_while_offline
        self.buffer_group = buffer_group or frozenset()
        self.buffer_limit = buffer_limit
        self.on_send_buffered = on_send_buffered
        self._group_buffer = defaultdict(list)
        self.mutex = threading.Lock()
        self.producer = None
        self._outbound_buffer = deque()
        self.serializer = serializer or self.app.conf.event_serializer
        self.on_enabled = set()
        self.on_disabled = set()
        self.groups = set(groups or [])
        self.tzoffset = [-time.timezone, -time.altzone]
        self.clock = self.app.clock
        self.delivery_mode = delivery_mode
        if not connection and channel:
            self.connection = channel.connection.client
        self.enabled = enabled
        # The continuity channel is off by default; when off no
        # session/seq fields are produced and behaviour is unchanged.
        self.continuity_enabled = bool(
            self.app.conf.event_continuity_enabled)
        if self.continuity_enabled:
            # Fail fast, before the worker/monitor starts serving.
            validate_continuity_settings(self.app.conf)
        self.continuity_dropped = 0
        self._continuity_dropped_seqs = deque(
            maxlen=self.CONTINUITY_DROPPED_LEDGER)
        conninfo = self.connection or self.app.connection_for_write()
        self.exchange = get_exchange(conninfo,
                                     name=self.app.conf.event_exchange)
        if conninfo.transport.driver_type in self.DISABLED_TRANSPORTS:
            self.enabled = False
        if self.enabled:
            self.enable()
        self.headers = {'hostname': self.hostname}
        self.pid = os.getpid()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def enable(self):
        self.producer = Producer(self.channel or self.connection,
                                 exchange=self.exchange,
                                 serializer=self.serializer,
                                 auto_declare=False)
        self.enabled = True
        for callback in self.on_enabled:
            callback()

    def disable(self):
        if self.enabled:
            self.enabled = False
            self.close()
            for callback in self.on_disabled:
                callback()

    def publish(self, type, fields, producer,
                blind=False, Event=Event, **kwargs):
        """Publish event using custom :class:`~kombu.Producer`.

        Arguments:
            type (str): Event type name, with group separated by dash (`-`).
                fields: Dictionary of event fields, must be json serializable.
            producer (kombu.Producer): Producer instance to use:
                only the ``publish`` method will be called.
            retry (bool): Retry in the event of connection failure.
            retry_policy (Mapping): Map of custom retry policy options.
                See :meth:`~kombu.Connection.ensure`.
            blind (bool): Don't set logical clock value (also don't forward
                the internal logical clock).
            Event (Callable): Event type used to create event.
                Defaults to :func:`Event`.
            utcoffset (Callable): Function returning the current
                utc offset in hours.
        """
        clock = None if blind else self.clock.forward()
        event = Event(type, hostname=self.hostname, utcoffset=utcoffset(),
                      pid=self.pid, clock=clock, **fields)
        self._stamp_continuity(event)
        with self.mutex:
            return self._publish(event, producer,
                                 routing_key=type.replace('-', '.'), **kwargs)

    def _stamp_continuity(self, event):
        """Stamp an event with its session id and sequence number.

        Each event gets exactly one sequence number, and the number is
        never reused or reset: buffered batch sends, remote event toggling
        and offline replay all go through the same process-wide allocator.
        Replayed events already carry a stamp and are left untouched.
        """
        if not self.continuity_enabled or 'seq' in event:
            return
        session, seq = process_continuity.allocate(self.pid)
        event['session'] = session
        event['seq'] = seq

    def _record_continuity_drop(self, event):
        """Account for event(s) that could not be buffered nor published."""
        events = event if isinstance(event, list) else [event]
        for ev in events:
            seq = ev.get('seq') if isinstance(ev, dict) else None
            if seq is not None:
                self._continuity_dropped_seqs.append(seq)
            self.continuity_dropped += 1

    def continuity_info(self):
        """Return continuity bookkeeping for a resync snapshot reply."""
        if not self.continuity_enabled:
            return {'enabled': False}
        session, seq = process_continuity.current(self.pid)
        return {
            'enabled': True,
            'session': session,
            'seq': seq,
            'dropped': self.continuity_dropped,
            'dropped_seqs': list(self._continuity_dropped_seqs),
        }

    def _publish(self, event, producer, routing_key, retry=False,
                 retry_policy=None, utcoffset=utcoffset):
        if producer is None:
            return
        exchange = self.exchange
        try:
            producer.publish(
                event,
                routing_key=routing_key,
                exchange=exchange.name,
                retry=retry,
                retry_policy=retry_policy,
                declare=[exchange],
                serializer=self.serializer,
                headers=self.headers,
                delivery_mode=self.delivery_mode,
            )
        except Exception:
            if not self.buffer_while_offline:
                raise
            if (self.continuity_enabled and
                    len(self._outbound_buffer) >=
                    self.CONTINUITY_OUTBOUND_LIMIT):
                # The buffer is full after an outage: drop the new event
                # but leave an auditable trace (the monitor will see the
                # missing sequence number and resync against the snapshot).
                self._record_continuity_drop(event)
                return
            self._outbound_buffer.append((event, routing_key))

    def send(self, type, blind=False, utcoffset=utcoffset, retry=False,
             retry_policy=None, Event=Event, **fields):
        """Send event.

        Arguments:
            type (str): Event type name, with group separated by dash (`-`).
            retry (bool): Retry in the event of connection failure.
            retry_policy (Mapping): Map of custom retry policy options.
                See :meth:`~kombu.Connection.ensure`.
            blind (bool): Don't set logical clock value (also don't forward
                the internal logical clock).
            Event (Callable): Event type used to create event,
                defaults to :func:`Event`.
            utcoffset (Callable): unction returning the current utc offset
                in hours.
            **fields (Any): Event fields -- must be json serializable.
        """
        if self.enabled:
            groups, group = self.groups, group_from(type)
            if groups and group not in groups:
                return
            if group in self.buffer_group:
                clock = self.clock.forward()
                event = Event(type, hostname=self.hostname,
                              utcoffset=utcoffset(),
                              pid=self.pid, clock=clock, **fields)
                # Every event in a future *.multi batch gets its own
                # sequence number at enqueue time, so batching neither
                # skips nor duplicates sequence numbers.
                self._stamp_continuity(event)
                buf = self._group_buffer[group]
                buf.append(event)
                if len(buf) >= self.buffer_limit:
                    self.flush()
                elif self.on_send_buffered:
                    self.on_send_buffered()
            else:
                return self.publish(type, fields, self.producer, blind=blind,
                                    Event=Event, retry=retry,
                                    retry_policy=retry_policy)

    def flush(self, errors=True, groups=True):
        """Flush the outbound buffer."""
        if errors:
            buf = list(self._outbound_buffer)
            self._outbound_buffer.clear()
            with self.mutex:
                for event, routing_key in buf:
                    self._publish(event, self.producer, routing_key)
        if groups:
            with self.mutex:
                for group, events in self._group_buffer.items():
                    if not events:
                        continue
                    # Publish a detached copy, since _publish re-buffers the
                    # object it was handed when offline. Clear only what was
                    # published: other threads append during the socket write.
                    batch = list(events)
                    self._publish(batch, self.producer, '%s.multi' % group)
                    del events[:len(batch)]

    def extend_buffer(self, other):
        """Copy the outbound buffer of another instance."""
        self._outbound_buffer.extend(other._outbound_buffer)

    def close(self):
        """Close the event dispatcher."""
        self.mutex.locked() and self.mutex.release()
        self.producer = None

    def _get_publisher(self):
        return self.producer

    def _set_publisher(self, producer):
        self.producer = producer
    publisher = property(_get_publisher, _set_publisher)  # XXX compat
