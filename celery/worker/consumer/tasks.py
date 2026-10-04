"""Worker Task Consumer Bootstep."""

from __future__ import annotations

from kombu.common import ignore_errors

from celery import bootsteps
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger
from celery.utils.quorum_queues import detect_quorum_queues

from .mingle import Mingle
from .qos import QoS

__all__ = ('Tasks',)


logger = get_logger(__name__)
debug = logger.debug


class Tasks(bootsteps.StartStopStep):
    """Bootstep starting the task message consumer."""

    requires = (Mingle,)

    def __init__(self, c, **kwargs):
        c.task_consumer = c.qos = None
        super().__init__(c, **kwargs)

    def start(self, c):
        """Start task consumer."""
        c.update_strategies()

        qos_global = self.qos_global(c)
        # Record effective QoS mode on the consumer so the reconnect path
        # (Consumer.on_connection_error_after_connected) can decide whether
        # the prefetch reduction/restoration mechanism is safe. Per-consumer
        # QoS does not support it. See #9512.
        c.qos_global = qos_global

        eta_task_limit = c.app.conf.worker_eta_task_limit
        # Per-queue caps declared at startup switch the task consumer to
        # per-consumer QoS windows (one window per queue).  With no caps
        # declared the legacy channel-global integer path below is kept
        # exactly as-is.  The isinstance check also keeps test doubles
        # (Mock consumers) on the legacy path.
        queue_limits = getattr(c, 'queue_prefetch_limits', None)
        if isinstance(queue_limits, dict) and queue_limits:
            self._start_per_queue_qos(c, queue_limits, eta_task_limit)
        else:
            # set initial prefetch count
            c.connection.default_channel.basic_qos(
                0, c.initial_prefetch_count, qos_global,
            )

            c.task_consumer = c.app.amqp.TaskConsumer(
                c.connection, on_decode_error=c.on_decode_error,
            )

            def set_prefetch_count(prefetch_count):
                return c.task_consumer.qos(
                    prefetch_count=prefetch_count,
                    apply_global=qos_global,
                )
            c.qos = QoS(
                set_prefetch_count, c.initial_prefetch_count,
                max_prefetch=eta_task_limit,
            )

        if c.app.conf.worker_disable_prefetch:
            # Only apply disable-prefetch for Redis brokers
            is_redis_broker = c.connection.transport.driver_type == 'redis'
            if not is_redis_broker:
                logger.warning(
                    f"worker_disable_prefetch is only supported for Redis brokers. "
                    f"Current broker transport: {c.connection.transport.driver_type}. "
                    f"Ignoring disable_prefetch setting."
                )
                return

            from types import MethodType

            from celery.worker import state
            channel_qos = c.task_consumer.channel.qos
            original_can_consume = channel_qos.can_consume

            def can_consume(self):
                # Prefer autoscaler's max_concurrency if set; otherwise fall back to pool size
                limit = getattr(c.controller, "max_concurrency", None) or c.pool.num_processes
                if len(state.reserved_requests) >= limit:
                    return False
                return original_can_consume()

            channel_qos.can_consume = MethodType(can_consume, channel_qos)

    def _start_per_queue_qos(self, c, queue_limits, eta_task_limit):
        """Bind every consumed queue with its own per-consumer QoS window."""
        # Per-consumer QoS windows (``a_global=False``) are an AMQP broker
        # feature.  Virtual transports (Redis/SQS/...) only emulate a single
        # channel-wide window, so starting up with caps they cannot enforce
        # is a configuration error rather than something to silently ignore.
        if c.connection.transport.driver_type != 'amqp':
            raise ImproperlyConfigured(
                "worker_queue_prefetch_limits requires per-consumer QoS "
                "support, which is only available with AMQP brokers; "
                f"current transport is "
                f"{c.connection.transport.driver_type!r}. Remove the setting "
                "or use an AMQP broker."
            )
        if not c.initial_prefetch_count:
            raise ImproperlyConfigured(
                "worker_queue_prefetch_limits cannot be combined with "
                "worker_prefetch_multiplier=0 (prefetch disabled); the "
                "automatic per-queue value would be zero."
            )

        c.task_consumer = c.app.amqp.TaskConsumer(
            c.connection, on_decode_error=c.on_decode_error,
        )

        # Allow caps to be declared under queue aliases as well; key the
        # persistent mapping by the real broker queue names used by the
        # task consumer.
        for queue in c.task_consumer.queues:
            alias = getattr(queue, 'alias', None)
            if alias and alias in queue_limits and queue.name not in queue_limits:
                queue_limits[queue.name] = queue_limits.pop(alias)

        def set_prefetch_count(prefetch_count):
            return c.task_consumer.qos(
                prefetch_count=prefetch_count, apply_global=False,
            )

        c.qos = QoS(
            set_prefetch_count, c.initial_prefetch_count,
            max_prefetch=eta_task_limit, queue_limits=queue_limits,
        )
        c.enable_per_queue_qos()
        # Apply ``basic.qos`` immediately before each ``basic.consume`` so
        # the per-consumer window is frozen with the right value (RabbitMQ
        # only applies per-consumer QoS to consumers created afterwards).
        task_consumer = c.task_consumer
        queues = list(task_consumer.queues)
        for index, queue in enumerate(queues):
            prefetch_count = c.qos.activate_queue(queue.name)
            sync_consume = index == len(queues) - 1
            c.bind_queue_prefetch(
                queue, prefetch_count, sync_consume=sync_consume,
            )

    def stop(self, c):
        """Stop task consumer."""
        if c.task_consumer:
            debug('Canceling task consumer...')
            ignore_errors(c, c.task_consumer.cancel)

    def shutdown(self, c):
        """Shutdown task consumer."""
        if c.task_consumer:
            self.stop(c)
            debug('Closing consumer channel...')
            ignore_errors(c, c.task_consumer.close)
            c.task_consumer = None

    def info(self, c):
        """Return task consumer info."""
        info = {'prefetch_count': c.qos.value if c.qos else 'N/A'}
        if c.qos is not None and getattr(
                c.qos, 'per_queue_enabled', False) is True:
            info['queue_prefetch'] = c.qos.info()
        return info

    def qos_global(self, c) -> bool:
        """Determine if global QoS should be applied.

        Additional information:
            https://www.rabbitmq.com/docs/consumer-prefetch
            https://www.rabbitmq.com/docs/quorum-queues#global-qos
        """
        # - RabbitMQ 3.3 completely redefines how basic_qos works...
        # This will detect if the new qos semantics is in effect,
        # and if so make sure the 'apply_global' flag is set on qos updates.
        qos_global = not c.connection.qos_semantics_matches_spec

        if c.app.conf.worker_detect_quorum_queues:
            using_quorum_queues, _ = detect_quorum_queues(
                c.app, c.connection.transport.driver_type
            )

            if using_quorum_queues:
                qos_global = False
                logger.info("Global QoS is disabled. Prefetch count is now static.")

        return qos_global
