"""File-system result store backend."""
import locale
import os
import time
from contextlib import contextmanager
from datetime import datetime

from kombu.utils.encoding import ensure_bytes

from celery import uuid
from celery.backends.base import KeyValueStoreBackend
from celery.backends.governance import ScannedValue
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

default_encoding = locale.getpreferredencoding(False)

logger = get_logger(__name__)

E_NO_PATH_SET = 'You need to configure a path for the file-system backend'
E_PATH_NON_CONFORMING_SCHEME = (
    'A path for the file-system backend should conform to the file URI scheme'
)
E_PATH_INVALID = """\
The configured path for the file-system backend does not
work correctly, please make sure that it exists and has
the correct permissions.\
"""


class FilesystemBackend(KeyValueStoreBackend):
    """File-system result backend.

    Arguments:
        url (str):  URL to the directory we should use
        open (Callable): open function to use when opening files
        unlink (Callable): unlink function to use when deleting files
        sep (str): directory separator (to join the directory with the key)
        encoding (str): encoding used on the file-system
    """

    # Result files are opened in binary mode in both directions.
    supports_result_compression = True
    # Results are files in one directory, so enumeration is a listdir().
    supports_capacity_governance = True

    governance_lock_name = b'.celery-governance.lock'

    def __init__(self, url=None, open=open, unlink=os.unlink, sep=os.sep,
                 encoding=default_encoding, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        path = self._find_path(url)

        # Remove forwarding "/" for Windows os
        if os.name == "nt" and path.startswith("/"):
            path = path[1:]

        # We need the path and separator as bytes objects
        self.path = path.encode(encoding)
        self.sep = sep.encode(encoding)

        self.open = open
        self.unlink = unlink

        # Let's verify that we've everything setup right
        self._do_directory_test(b'.fs-backend-' + uuid().encode(encoding))

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return super().__reduce__(args, {**kwargs, 'url': self.url})

    def _find_path(self, url):
        if not url:
            raise ImproperlyConfigured(E_NO_PATH_SET)
        if url.startswith('file://localhost/'):
            return url[16:]
        if url.startswith('file://'):
            return url[7:]
        raise ImproperlyConfigured(E_PATH_NON_CONFORMING_SCHEME)

    def _do_directory_test(self, key):
        try:
            self.set(key, b'test value')
            assert self.get(key) == b'test value'
            self.delete(key)
        except OSError:
            raise ImproperlyConfigured(E_PATH_INVALID)

    def _filename(self, key):
        return self.sep.join((self.path, key))

    def get(self, key):
        try:
            with self.open(self._filename(key), 'rb') as infile:
                return infile.read()
        except FileNotFoundError:
            pass

    def set(self, key, value):
        with self.open(self._filename(key), 'wb') as outfile:
            outfile.write(ensure_bytes(value))

    def mget(self, keys):
        for key in keys:
            yield self.get(key)

    def delete(self, key):
        self.unlink(self._filename(key))

    # -- capacity governance storage primitives -------------------------

    def _iter_result_payloads(self):
        for name in os.listdir(self.path):
            if not name.startswith(self.task_keyprefix):
                # Group metadata, chord counters and the governance lock
                # live in the same directory but are never task results.
                continue
            path = self._filename(name)
            try:
                stat = os.stat(path)
                with self.open(path, 'rb') as infile:
                    raw = infile.read()
            except FileNotFoundError:
                # Vanished between listdir() and open()/stat().
                continue
            yield ScannedValue(name, raw, stat.st_size, stat.st_mtime)

    @contextmanager
    def _governance_storage_lock(self):
        lock_path = self._filename(self.governance_lock_name)
        timeout = float(
            self._governance_option('governance_lock_timeout', 10.0))
        ttl = float(self._governance_option('governance_lock_ttl', 30.0))
        token = uuid().encode()
        acquired = False
        with self._governance_thread_lock:
            try:
                deadline = time.monotonic() + timeout
                while True:
                    try:
                        fd = os.open(
                            lock_path,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                        with os.fdopen(fd, 'wb') as lockfile:
                            lockfile.write(token)
                        acquired = True
                        break
                    except FileExistsError:
                        # Steal locks whose owner clearly died (stale mtime).
                        try:
                            if time.time() - os.stat(lock_path).st_mtime > ttl:
                                self.unlink(lock_path)
                                continue
                        except FileNotFoundError:
                            continue
                        if time.monotonic() >= deadline:
                            logger.warning(
                                'Timed out after %ss waiting for the result '
                                'governance lock; proceeding without it.',
                                timeout)
                            break
                        time.sleep(0.05)
                yield
            finally:
                if acquired:
                    try:
                        with self.open(lock_path, 'rb') as lockfile:
                            owner = lockfile.read(len(token))
                        if owner == token:
                            self.unlink(lock_path)
                    except FileNotFoundError:
                        pass

    def cleanup(self):
        """Delete expired/over-capacity meta-data."""
        if self.governance_enabled:
            return super().cleanup()
        self._cleanup_expired_results()

    def _cleanup_expired_results(self):
        """The legacy, expiry-only, scan of the result directory."""
        if not self.expires:
            return
        epoch = datetime(1970, 1, 1, tzinfo=self.app.timezone)
        now_ts = (self.app.now() - epoch).total_seconds()
        cutoff_ts = now_ts - self.expires
        for filename in os.listdir(self.path):
            for prefix in (self.task_keyprefix, self.group_keyprefix,
                           self.chord_keyprefix):
                if filename.startswith(prefix):
                    path = os.path.join(self.path, filename)
                    if os.stat(path).st_mtime < cutoff_ts:
                        self.unlink(path)
                    break
