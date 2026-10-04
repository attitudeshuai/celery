"""Sending/Receiving Messages (Kombu integration)."""
import numbers
import threading
from collections import namedtuple
from collections.abc import Mapping
from datetime import timedelta
from weakref import WeakValueDictionary

from kombu import Connection, Consumer, Exchange, Producer, Queue, pools
from kombu.common import Broadcast
from kombu.utils.functional import maybe_list
from kombu.utils.objects import cached_property

from celery import signals
from celery.exceptions import RouteValidationError
from celery.utils.nodenames import anon_nodename
from celery.utils.saferepr import saferepr
from celery.utils.text import indent as textindent
from celery.utils.time import maybe_make_aware

from . import route_checks
from . import routes as _routes

__all__ = ('AMQP', 'Queues', 'task_message')

#: Configuration keys that feed the prepared routing table.
_ROUTES_SETTING = 'task_routes'
#: Configuration keys that change the declared queue set or the way
#: missing queues are created, and therefore invalidate a static
#: routing decision just like a ``task_routes`` change.
_QUEUE_SETTINGS = frozenset({
    'task_queues', 'task_create_missing_queues',
    'task_create_missing_queue_type',
    'task_create_missing_queue_exchange_type',
    'task_queue_max_priority', 'task_default_queue',
    'task_default_exchange', 'task_default_exchange_type',
    'task_default_routing_key',
})
_ROUTING_SETTINGS = frozenset({_ROUTES_SETTING}) | _QUEUE_SETTINGS

#: earliest date supported by time.mktime.
INT_MIN = -2147483648

#: Human readable queue declaration.
QUEUE_FORMAT = """
.> {0.name:<16} exchange={0.exchange.name}({0.exchange.type}) \
key={0.routing_key}
"""

task_message = namedtuple('task_message',
                          ('headers', 'properties', 'body', 'sent_event'))


def utf8dict(d, encoding='utf-8'):
    return {k.decode(encoding) if isinstance(k, bytes) else k: v
            for k, v in d.items()}


class Queues(dict):
    """Queue name⇒ declaration mapping.

    Arguments:
        queues (Iterable): Initial list/tuple or dict of queues.
        create_missing (bool): By default any unknown queues will be
            added automatically, but if this flag is disabled the occurrence
            of unknown queues in `wanted` will raise :exc:`KeyError`.
        create_missing_queue_type (str): Type of queue to create for missing queues.
            Must be either 'classic' (default) or 'quorum'. If set to 'quorum',
            the broker will declare new queues using the quorum type.
        create_missing_queue_exchange_type (str): Type of exchange to use
            when creating missing queues. If not set, the default exchange type
            will be used. If set, the exchange type will be set to this value
            when creating missing queues.
        max_priority (int): Default x-max-priority for queues with none set.
    """

    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from = None

    def __init__(
            self, queues=None, default_exchange=None,
            create_missing=True, create_missing_queue_type=None,
            create_missing_queue_exchange_type=None, autoexchange=None,
            max_priority=None, default_routing_key=None,
    ):
        super().__init__()
        self.aliases = WeakValueDictionary()
        self.default_exchange = default_exchange
        self.default_routing_key = default_routing_key
        self.create_missing = create_missing
        self.create_missing_queue_type = create_missing_queue_type
        self.create_missing_queue_exchange_type = create_missing_queue_exchange_type
        self.autoexchange = Exchange if autoexchange is None else autoexchange
        self.max_priority = max_priority
        if queues is not None and not isinstance(queues, Mapping):
            queues = {q.name: q for q in queues}
        queues = queues or {}
        for name, q in queues.items():
            self.add(q) if isinstance(q, Queue) else self.add_compat(name, **q)
        # The default set of queues to consume from if no -Q option is set.
        self._default_consume_from = {**self}

    def __getitem__(self, name):
        try:
            return self.aliases[name]
        except KeyError:
            return super().__getitem__(name)

    def __setitem__(self, name, queue):
        if self.default_exchange and not queue.exchange:
            queue.exchange = self.default_exchange
        if self.max_priority is not None:
            if queue.queue_arguments is None:
                queue.queue_arguments = {}
            self._set_max_priority(queue.queue_arguments)
        super().__setitem__(name, queue)
        if queue.alias:
            self.aliases[queue.alias] = queue

    def __missing__(self, name):
        if self.create_missing:
            return self.add(self.new_missing(name))
        raise KeyError(name)

    def add(self, queue, **kwargs):
        """Add new queue.

        The first argument can either be a :class:`kombu.Queue` instance,
        or the name of a queue.  If the former the rest of the keyword
        arguments are ignored, and options are simply taken from the queue
        instance.

        Arguments:
            queue (kombu.Queue, str): Queue to add.
            exchange (kombu.Exchange, str):
                if queue is str, specifies exchange name.
            routing_key (str): if queue is str, specifies binding key.
            exchange_type (str): if queue is str, specifies type of exchange.
            **options (Any): Additional declaration options used when
                queue is a str.
        """
        if not isinstance(queue, Queue):
            return self.add_compat(queue, **kwargs)
        return self._add(queue)

    def add_compat(self, name, **options):
        # docs used to use binding_key as routing key
        options.setdefault('routing_key', options.get('binding_key'))
        if options['routing_key'] is None:
            options['routing_key'] = name
        return self._add(Queue.from_dict(name, **options))

    def _add(self, queue):
        if queue.exchange is None or queue.exchange.name == '':
            queue.exchange = self.default_exchange
        if not queue.routing_key:
            queue.routing_key = self.default_routing_key
        self[queue.name] = queue
        return queue

    def _set_max_priority(self, args):
        if 'x-max-priority' not in args and self.max_priority is not None:
            return args.update({'x-max-priority': self.max_priority})

    def format(self, indent=0, indent_first=True):
        """Format routing table into string for log dumps."""
        active = self.consume_from
        if not active:
            return ''
        info = [QUEUE_FORMAT.strip().format(q)
                for _, q in sorted(active.items())]
        if indent_first:
            return textindent('\n'.join(info), indent)
        return info[0] + '\n' + textindent('\n'.join(info[1:]), indent)

    def select_add(self, queue, **kwargs):
        """Add new task queue that'll be consumed from.

        The queue will be active even when a subset has been selected
        using the :option:`celery worker -Q` option.
        """
        q = self.add(queue, **kwargs)
        if self._consume_from is not None:
            self._consume_from[q.name] = q
        else:
            self._default_consume_from[q.name] = q
        return q

    def select(self, include):
        """Select a subset of currently defined queues to consume from.

        Arguments:
            include (Sequence[str], str): Names of queues to consume from.
        """
        if include:
            self._consume_from = {}
            for name in maybe_list(include):
                q = self[name]
                self._consume_from[q.name] = q

    def deselect(self, exclude):
        """Deselect queues so that they won't be consumed from.

        Arguments:
            exclude (Sequence[str], str): Names of queues to avoid
                consuming from.
        """
        if exclude:
            exclude = maybe_list(exclude)
            if self._consume_from is None:
                consume_from = self._default_consume_from
            else:
                consume_from = self._consume_from

            for name in exclude:
                queue = self.aliases.get(name)
                consume_from.pop(queue.name if queue is not None else name, None)

    def new_missing(self, name):
        queue_arguments = None
        if self.create_missing_queue_type and self.create_missing_queue_type != "classic":
            if self.create_missing_queue_type not in ("classic", "quorum"):
                raise ValueError(
                    f"Invalid queue type '{self.create_missing_queue_type}'. "
                    "Valid types are 'classic' and 'quorum'."
                )
            queue_arguments = {"x-queue-type": self.create_missing_queue_type}

        if self.create_missing_queue_exchange_type:
            exchange = Exchange(name, self.create_missing_queue_exchange_type)
        else:
            exchange = self.autoexchange(name)

        return Queue(name, exchange, name, queue_arguments=queue_arguments)

    @property
    def consume_from(self):
        if self._consume_from is not None:
            return self._consume_from
        return self._default_consume_from


class AMQP:
    """App AMQP API: app.amqp."""

    Connection = Connection
    Consumer = Consumer
    Producer = Producer

    #: compat alias to Connection
    BrokerConnection = Connection

    queues_cls = Queues

    #: Cached and prepared routing table.
    _rtable = None

    #: Underlying producer pool instance automatically
    #: set by the :attr:`producer_pool`.
    _producer_pool = None

    # Exchange class/function used when defining automatic queues.
    # For example, you can use ``autoexchange = lambda n: None`` to use the
    # AMQP default exchange: a shortcut to bypass routing
    # and instead send directly to the queue named in the routing key.
    autoexchange = None

    #: Max size of positional argument representation used for
    #: logging purposes.
    argsrepr_maxsize = 1024

    #: Max size of keyword argument representation used for logging purposes.
    kwargsrepr_maxsize = 1024

    def __init__(self, app):
        self.app = app
        self.task_protocols = {
            1: self.as_task_v1,
            2: self.as_task_v2,
        }
        # Version boundary for routing decisions.  Only used when
        # task_routes_validate is enabled; when the gate is disabled every
        # code path behaves exactly as it did before (the lock is simply
        # never taken).
        self._routing_lock = threading.RLock()
        #: Monotonic version of the (routing table, queue declarations)
        #: snapshot.  Bumped whenever either is rebuilt.
        self._rtable_version = 0
        #: Cached static route report for ``_rtable_version``.
        self._route_report = None
        self.app._conf.bind_to(self._handle_conf_update)

    def _routing_validate_enabled(self):
        return bool(self.app.conf.task_routes_validate)

    @cached_property
    def create_task_message(self):
        return self.task_protocols[self.app.conf.task_protocol]

    @cached_property
    def send_task_message(self):
        return self._create_task_sender()

    def Queues(self, queues, create_missing=None, create_missing_queue_type=None,
               create_missing_queue_exchange_type=None, autoexchange=None, max_priority=None):
        # Create new :class:`Queues` instance, using queue defaults
        # from the current configuration.
        conf = self.app.conf
        default_routing_key = conf.task_default_routing_key
        if create_missing is None:
            create_missing = conf.task_create_missing_queues
        if create_missing_queue_type is None:
            create_missing_queue_type = conf.task_create_missing_queue_type
        if create_missing_queue_exchange_type is None:
            create_missing_queue_exchange_type = conf.task_create_missing_queue_exchange_type
        if max_priority is None:
            max_priority = conf.task_queue_max_priority
        if not queues and conf.task_default_queue:
            queue_arguments = None
            if conf.task_default_queue_type == 'quorum':
                queue_arguments = {'x-queue-type': 'quorum'}
            queues = (Queue(conf.task_default_queue,
                            exchange=self.default_exchange,
                            routing_key=default_routing_key,
                            queue_arguments=queue_arguments),)
        autoexchange = (self.autoexchange if autoexchange is None
                        else autoexchange)
        return self.queues_cls(
            queues,
            default_exchange=self.default_exchange,
            create_missing=create_missing,
            create_missing_queue_type=create_missing_queue_type,
            create_missing_queue_exchange_type=create_missing_queue_exchange_type,
            autoexchange=autoexchange,
            max_priority=max_priority,
            default_routing_key=default_routing_key,
        )

    def Router(self, queues=None, create_missing=None):
        """Return the current task router."""
        return _routes.Router(self.routes, queues or self.queues,
                              self.app.either('task_create_missing_queues',
                                              create_missing), app=self.app)

    def flush_routes(self):
        """Rebuild the prepared routing table from ``task_routes``."""
        if self._routing_validate_enabled():
            with self._routing_lock:
                self._rebuild_routing_locked(routes_changed=True)
        else:
            self._rtable = _routes.prepare(self.app.conf.task_routes)

    def _rebuild_routing_locked(self, routes_changed=False,
                                queues_changed=False):
        """Atomically swap routing state and invalidate old decisions.

        ``_routing_lock`` must be held by the caller.  The new routing
        table and queue mapping are built before they are published, so a
        concurrent reader either sees the complete old snapshot or the
        complete new one -- never an intermediate state.  Previous
        conclusions are dropped and the version is bumped before the new
        state becomes visible, so stale results can never be served for a
        new configuration.
        """
        if routes_changed or self._rtable is None:
            rtable = _routes.prepare(self.app.conf.task_routes)
        else:
            rtable = self._rtable
        new_queues = None
        selected = None
        if queues_changed:
            old = self.__dict__.get('queues')
            if old is not None and old._consume_from is not None:
                # Preserve an active `worker -Q` subscription selection
                # across the rebuild.
                selected = list(old._consume_from)
            new_queues = self.Queues(self.app.conf.task_queues)
            for attrname in ('default_queue', 'default_exchange'):
                self.__dict__.pop(attrname, None)
            # Invalidate first: nothing between this point and the swap
            # may answer with conclusions derived from the old snapshot.
            self._route_report = None
            self._rtable_version += 1
        else:
            self._route_report = None
            self._rtable_version += 1
        self._rtable = rtable
        if new_queues is not None:
            self.__dict__['queues'] = new_queues
            if selected is not None:
                new_queues.select(selected)
        self.__dict__.pop('router', None)

    def _ensure_routing_locked(self):
        if self._rtable is None:
            self._rebuild_routing_locked(routes_changed=True)

    def TaskConsumer(self, channel, queues=None, accept=None, **kw):
        if accept is None:
            accept = self.app.conf.accept_content
        return self.Consumer(
            channel, accept=accept,
            queues=queues or list(self.queues.consume_from.values()),
            **kw
        )

    def as_task_v2(self, task_id, name, args=None, kwargs=None,
                   countdown=None, eta=None, group_id=None, group_index=None,
                   expires=None, retries=0, chord=None,
                   callbacks=None, errbacks=None, reply_to=None,
                   time_limit=None, soft_time_limit=None,
                   create_sent_event=False, root_id=None, parent_id=None,
                   shadow=None, chain=None, now=None, timezone=None,
                   origin=None, ignore_result=False, argsrepr=None, kwargsrepr=None, stamped_headers=None,
                   replaced_task_nesting=0, **options):

        args = args or ()
        kwargs = kwargs or {}
        if not isinstance(args, (list, tuple)):
            raise TypeError('task args must be a list or tuple')
        if not isinstance(kwargs, Mapping):
            raise TypeError('task keyword arguments must be a mapping')
        if countdown:  # convert countdown to ETA
            self._verify_seconds(countdown, 'countdown')
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            eta = maybe_make_aware(
                now + timedelta(seconds=countdown), tz=timezone,
            )
        if isinstance(expires, numbers.Real):
            self._verify_seconds(expires, 'expires')
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            expires = maybe_make_aware(
                now + timedelta(seconds=expires), tz=timezone,
            )
        if not isinstance(eta, str):
            eta = eta and eta.isoformat()
        # If we retry a task `expires` will already be ISO8601-formatted.
        if not isinstance(expires, str):
            expires = expires and expires.isoformat()

        if argsrepr is None:
            argsrepr = saferepr(args, self.argsrepr_maxsize, maxlevels=self.app.conf.task_repr_maxlevels)
        if kwargsrepr is None:
            kwargsrepr = saferepr(kwargs, self.kwargsrepr_maxsize, maxlevels=self.app.conf.task_repr_maxlevels)

        if not root_id:  # empty root_id defaults to task_id
            root_id = task_id

        stamps = {header: options[header] for header in stamped_headers or []}
        headers = {
            'lang': 'py',
            'task': name,
            'id': task_id,
            'shadow': shadow,
            'eta': eta,
            'expires': expires,
            'group': group_id,
            'group_index': group_index,
            'retries': retries,
            'timelimit': [time_limit, soft_time_limit],
            'root_id': root_id,
            'parent_id': parent_id,
            'argsrepr': argsrepr,
            'kwargsrepr': kwargsrepr,
            'origin': origin or anon_nodename(),
            'ignore_result': ignore_result,
            'replaced_task_nesting': replaced_task_nesting,
            'stamped_headers': stamped_headers,
            'stamps': stamps,
        }

        return task_message(
            headers=headers,
            properties={
                'correlation_id': task_id,
                'reply_to': reply_to or '',
            },
            body=(
                args, kwargs, {
                    'callbacks': callbacks,
                    'errbacks': errbacks,
                    'chain': chain,
                    'chord': chord,
                },
            ),
            sent_event={
                'uuid': task_id,
                'root_id': root_id,
                'parent_id': parent_id,
                'name': name,
                'args': argsrepr,
                'kwargs': kwargsrepr,
                'retries': retries,
                'eta': eta,
                'expires': expires,
            } if create_sent_event else None,
        )

    def as_task_v1(self, task_id, name, args=None, kwargs=None,
                   countdown=None, eta=None, group_id=None, group_index=None,
                   expires=None, retries=0,
                   chord=None, callbacks=None, errbacks=None, reply_to=None,
                   time_limit=None, soft_time_limit=None,
                   create_sent_event=False, root_id=None, parent_id=None,
                   shadow=None, now=None, timezone=None,
                   **compat_kwargs):
        args = args or ()
        kwargs = kwargs or {}
        utc = self.utc
        if not isinstance(args, (list, tuple)):
            raise TypeError('task args must be a list or tuple')
        if not isinstance(kwargs, Mapping):
            raise TypeError('task keyword arguments must be a mapping')
        if countdown:  # convert countdown to ETA
            self._verify_seconds(countdown, 'countdown')
            now = now or self.app.now()
            eta = now + timedelta(seconds=countdown)
        if isinstance(expires, numbers.Real):
            self._verify_seconds(expires, 'expires')
            now = now or self.app.now()
            expires = now + timedelta(seconds=expires)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        return task_message(
            headers={},
            properties={
                'correlation_id': task_id,
                'reply_to': reply_to or '',
            },
            body={
                'task': name,
                'id': task_id,
                'args': args,
                'kwargs': kwargs,
                'group': group_id,
                'group_index': group_index,
                'retries': retries,
                'eta': eta,
                'expires': expires,
                'utc': utc,
                'callbacks': callbacks,
                'errbacks': errbacks,
                'timelimit': (time_limit, soft_time_limit),
                'taskset': group_id,
                'chord': chord,
            },
            sent_event={
                'uuid': task_id,
                'name': name,
                'args': saferepr(args),
                'kwargs': saferepr(kwargs),
                'retries': retries,
                'eta': eta,
                'expires': expires,
            } if create_sent_event else None,
        )

    def _verify_seconds(self, s, what):
        if s < INT_MIN:
            raise ValueError(f'{what} is out of range: {s!r}')
        return s

    def _create_task_sender(self):
        amqp = self
        default_retry = self.app.conf.task_publish_retry
        default_policy = self.app.conf.task_publish_retry_policy
        default_delivery_mode = self.app.conf.task_default_delivery_mode
        queues = self.queues
        send_before_publish = signals.before_task_publish.send
        before_receivers = signals.before_task_publish.receivers
        send_after_publish = signals.after_task_publish.send
        after_receivers = signals.after_task_publish.receivers

        send_task_sent = signals.task_sent.send   # XXX compat (remove 6.0)
        sent_receivers = signals.task_sent.receivers   # XXX compat (remove 6.0)

        default_evd = self._event_dispatcher
        default_exchange = self.default_exchange

        default_rkey = self.app.conf.task_default_routing_key
        default_serializer = self.app.conf.task_serializer
        default_compressor = self.app.conf.task_compression

        def send_task_message(producer, name, message,
                              exchange=None, routing_key=None, queue=None,
                              event_dispatcher=None,
                              retry=None, retry_policy=None,
                              serializer=None, delivery_mode=None,
                              compression=None, declare=None,
                              headers=None, exchange_type=None,
                              timeout=None, confirm_timeout=None, **kwargs):
            retry = default_retry if retry is None else retry
            headers2, properties, body, sent_event = message
            if headers:
                headers2.update(headers)
            if kwargs:
                properties.update(kwargs)

            qname = queue
            if queue is None and exchange is None:
                queue = amqp.default_queue
            if queue is not None:
                if isinstance(queue, str):
                    qname, queue = queue, queues[queue]
                else:
                    qname = queue.name

            if delivery_mode is None:
                try:
                    delivery_mode = queue.exchange.delivery_mode
                except AttributeError:
                    pass
                delivery_mode = delivery_mode or default_delivery_mode

            if exchange_type is None:
                try:
                    exchange_type = queue.exchange.type
                except AttributeError:
                    exchange_type = 'direct'

            # convert to anon-exchange, when exchange not set and direct ex.
            if (not exchange or not routing_key) and exchange_type == 'direct':
                exchange, routing_key = '', qname
            elif exchange is None:
                # not topic exchange, and exchange not undefined
                exchange = queue.exchange.name or default_exchange
                routing_key = routing_key or queue.routing_key or default_rkey
            if declare is None and queue and not isinstance(queue, Broadcast):
                declare = [queue]

            # merge default and custom policy
            retry = default_retry if retry is None else retry
            _rp = (dict(default_policy, **retry_policy) if retry_policy
                   else default_policy)

            if before_receivers:
                send_before_publish(
                    sender=name, body=body,
                    exchange=exchange, routing_key=routing_key,
                    declare=declare, headers=headers2,
                    properties=properties, retry_policy=retry_policy,
                )
            ret = producer.publish(
                body,
                exchange=exchange,
                routing_key=routing_key,
                serializer=serializer or default_serializer,
                compression=compression or default_compressor,
                retry=retry, retry_policy=_rp,
                delivery_mode=delivery_mode, declare=declare,
                headers=headers2,
                timeout=timeout, confirm_timeout=confirm_timeout,
                **properties
            )
            if after_receivers:
                send_after_publish(sender=name, body=body, headers=headers2,
                                   exchange=exchange, routing_key=routing_key)
            if sent_receivers:  # XXX deprecated
                if isinstance(body, tuple):  # protocol version 2
                    send_task_sent(
                        sender=name, task_id=headers2['id'], task=name,
                        args=body[0], kwargs=body[1],
                        eta=headers2['eta'], taskset=headers2['group'],
                    )
                else:  # protocol version 1
                    send_task_sent(
                        sender=name, task_id=body['id'], task=name,
                        args=body['args'], kwargs=body['kwargs'],
                        eta=body['eta'], taskset=body['taskset'],
                    )
            if sent_event:
                evd = event_dispatcher or default_evd
                exname = exchange
                if isinstance(exname, Exchange):
                    exname = exname.name
                sent_event.update({
                    'queue': qname,
                    'exchange': exname,
                    'routing_key': routing_key,
                })
                evd.publish('task-sent', sent_event,
                            producer, retry=retry, retry_policy=retry_policy)
            return ret
        return send_task_message

    @cached_property
    def default_queue(self):
        return self.queues[self.app.conf.task_default_queue]

    @cached_property
    def queues(self):
        """Queue name⇒ declaration mapping."""
        return self.Queues(self.app.conf.task_queues)

    @queues.setter
    def queues(self, queues):
        new_queues = self.Queues(queues)
        if self._routing_validate_enabled():
            with self._routing_lock:
                self.__dict__['queues'] = new_queues
                self.__dict__.pop('default_queue', None)
                self._route_report = None
                self._rtable_version += 1
                self.__dict__.pop('router', None)
        return new_queues

    @property
    def routes(self):
        if self._routing_validate_enabled():
            with self._routing_lock:
                self._ensure_routing_locked()
                return self._rtable
        if self._rtable is None:
            self.flush_routes()
        return self._rtable

    @property
    def router(self):
        # Mirrors cached_property semantics when the gate is disabled: the
        # first Router() built for a snapshot is reused until a routing
        # setting changes.
        cached = self.__dict__.get('router')
        if cached is not None:
            return cached
        if self._routing_validate_enabled():
            with self._routing_lock:
                cached = self.__dict__.get('router')
                if cached is None:
                    self._ensure_routing_locked()
                    cached = self.Router()
                    self.__dict__['router'] = cached
                return cached
        cached = self.Router()
        self.__dict__['router'] = cached
        return cached

    @router.setter
    def router(self, value):
        self.__dict__['router'] = value
        return value

    @property
    def producer_pool(self):
        if self._producer_pool is None:
            self._producer_pool = pools.producers[
                self.app.connection_for_write()]
            self._producer_pool.limit = self.app.pool.limit
        return self._producer_pool
    publisher_pool = producer_pool  # compat alias

    @cached_property
    def default_exchange(self):
        return Exchange(self.app.conf.task_default_exchange,
                        self.app.conf.task_default_exchange_type)

    @cached_property
    def utc(self):
        return self.app.conf.enable_utc

    @cached_property
    def _event_dispatcher(self):
        # We call Dispatcher.publish with a custom producer
        # so don't need the dispatcher to be enabled.
        return self.app.events.Dispatcher(enabled=False)

    def _handle_conf_update(self, *args, **kwargs):
        changed = set(kwargs)
        for arg in args:
            # conf.update(mapping) forwards the mapping positionally.
            if isinstance(arg, Mapping):
                changed.update(arg)
            else:
                changed.add(arg)
        if self._routing_validate_enabled():
            relevant = changed & _ROUTING_SETTINGS
            if relevant:
                with self._routing_lock:
                    self._rebuild_routing_locked(
                        routes_changed=_ROUTES_SETTING in relevant,
                        queues_changed=bool(relevant & _QUEUE_SETTINGS),
                    )
            return
        # Gate disabled: keep the historical behavior of only reacting to
        # task_routes changes.
        if _ROUTES_SETTING in changed:
            self.flush_routes()
            self.router = self.Router()

    @staticmethod
    def _stable_copy(mapping):
        """Copy a mapping that may grow while a send auto-creates a queue.

        ``Router.route()`` can add a missing queue to the live mapping
        without going through a configuration update.  Copying such a
        mapping concurrently may raise ``RuntimeError: dictionary changed
        size``; retrying yields the strictly more complete state instead
        of an intermediate one.
        """
        while True:
            try:
                return dict(mapping)
            except RuntimeError:
                continue

    def _routing_snapshot_locked(self):
        """Build a consistent, broker-free snapshot for static analysis."""
        self._ensure_routing_locked()
        conf = self.app.conf
        queues = self.queues
        declared = self._stable_copy(queues)
        consume_from = set(self._stable_copy(queues.consume_from))
        if declared:
            implicit_default = None
        else:
            # Reproduce the queue the app creates implicitly when no
            # task_queues are configured, without touching the live set.
            implicit_default = self.Queues(())[conf.task_default_queue]
        task_names = list(self.app.tasks.keys())
        task_options = {}
        for name in task_names:
            task = self.app.tasks[name]
            get_options = getattr(task, '_get_exec_options', None)
            if callable(get_options):
                try:
                    options = get_options() or {}
                except Exception:  # pragma: no cover
                    options = {}
                task_options[name] = {
                    key: value for key, value in options.items()
                    if value is not None and key in (
                        'queue', 'exchange', 'routing_key')
                }
        return {
            'version': self._rtable_version,
            'prepared_routes': self._rtable,
            'queues': declared,
            'missing_factory': queues.new_missing,
            'consume_from': consume_from,
            'task_names': task_names,
            'task_options': task_options,
            'create_missing': queues.create_missing,
            'create_missing_queue_type': queues.create_missing_queue_type,
            'create_missing_queue_exchange_type':
                queues.create_missing_queue_exchange_type,
            'default_queue_name': conf.task_default_queue,
            'implicit_default_queue': implicit_default,
        }

    def inspect_routes(self):
        """Return a static report for the current routing configuration.

        Purely introspective: never connects to a broker, never declares
        queues and never publishes messages.  When the gate is enabled the
        report is cached per routing version and recomputed after every
        routing-relevant configuration change.
        """
        if self._routing_validate_enabled():
            with self._routing_lock:
                return self._inspect_routes_locked()
        return self._inspect_routes_locked()

    def _inspect_routes_locked(self):
        report = self._route_report
        if report is not None and report.version == self._rtable_version:
            return report
        report = route_checks.inspect_routing(
            **self._routing_snapshot_locked())
        if self._routing_validate_enabled():
            self._route_report = report
        return report

    def check_routes(self):
        """Validate routing configuration before anything is delivered.

        No-op when :setting:`task_routes_validate` is disabled (the
        default), keeping startup and delivery behavior unchanged.
        Otherwise:

        * evaluates all rules against the current routing snapshot without
          connecting to the broker, declaring queues or publishing;
        * raises :exc:`~celery.exceptions.RouteValidationError` if a rule
          cannot be resolved;
        * returns the cached :class:`~celery.app.route_checks.RouteReport`
          otherwise, recomputing it after a hot configuration update.
        """
        if not self._routing_validate_enabled():
            return None
        with self._routing_lock:
            report = self._inspect_routes_locked()
            if report.errors:
                raise RouteValidationError(
                    route_checks.format_errors(report))
            return report
