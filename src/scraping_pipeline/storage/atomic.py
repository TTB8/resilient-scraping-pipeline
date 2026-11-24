"""Crash-safe file replacement: a reader sees the old file or the complete new one, never a torn mix.

Every file the pipeline writes (cache objects, cache entries, Parquet and CSV deliverables) goes
through this module, because a scraper is a long-running process that *will* be killed mid-write:
Ctrl-C, an OOM kill, a laptop lid, a CI timeout. A plain ``open(path, "w")`` truncates the old file
first and fills it afterwards, so a kill in between leaves an empty or half-written file under the
real name, and the next run trusts it.

The recipe is the classic POSIX one:

1. write the new bytes to a temporary file in the **same directory** as the target (``os.replace``
   is only atomic within one filesystem, and the same directory guarantees that);
2. ``flush`` and ``os.fsync`` the temporary file, so its *data* is on disk before its *name* is;
3. ``os.replace`` it onto the target: one atomic rename, which also overwrites an existing file;
4. ``os.fsync`` the directory, so the rename itself survives a power loss.

Without step 2 a crash right after the rename can leave the new name pointing at a file whose
blocks never reached the disk (zero-length after reboot on ext4 with delayed allocation). Without
step 4 the rename can be lost and the old file reappears, which is safe but silently undoes work.

Temporary names are ``.<target name>.<random hex>.tmp``, created with ``O_CREAT | O_EXCL`` so two
concurrent writers to the same target can never share one, and removed on *any* exception
(``BaseException``, so ``KeyboardInterrupt`` too). A process that dies without running its
``finally`` (SIGKILL) can leave one behind: it never shadows a real file, and
:func:`is_temp_name` lets maintenance code such as ``PageCache.gc`` recognise and remove it.

Directory fsync is POSIX behaviour. Where the platform cannot open a directory (Windows) the step
is skipped: ``os.replace`` is still atomic there, only the durability of the rename is weaker.
"""

import os
import re
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["atomic_path", "atomic_write_bytes", "atomic_write_text", "is_temp_name"]

_TEMP_SUFFIX = ".tmp"
_TEMP_NAME = re.compile(r"^\..+\.[0-9a-f]{16}\.tmp$")
# 0o666 filtered by the process umask, the same permissions a plain open() would give.
_FILE_MODE = 0o666
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)


def is_temp_name(name: str) -> bool:
    """True when ``name`` is a temporary file name this module creates (for cleanup tooling)."""
    return bool(_TEMP_NAME.match(name))


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Replace ``path`` with ``data`` atomically and durably, creating parent directories.

    On any exception the target is left exactly as it was and no temporary file remains.
    """
    target = Path(path)
    _ensure_dir(target.parent)
    temp, fd = _create_temp(target)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    _fsync_dir(target.parent)


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    """Replace ``path`` with ``text`` encoded as ``encoding``, atomically.

    The text is written verbatim: no newline translation, so ``"\\n"`` stays ``"\\n"`` on every
    platform, which is what deterministic outputs need.
    """
    atomic_write_bytes(path, text.encode(encoding))


@contextmanager
def atomic_path(target: Path) -> Iterator[Path]:
    """Yield a temporary path for a writer that wants a file name (e.g. pyarrow), then commit it.

    The yielded path already exists (empty) so the name is reserved; the writer may overwrite or
    reopen it. When the ``with`` body finishes normally the file is fsynced and moved onto
    ``target``; when it raises, the temporary file is deleted and ``target`` is untouched.

    >>> # with atomic_path(out) as tmp: pyarrow.parquet.write_table(table, tmp)
    """
    final = Path(target)
    _ensure_dir(final.parent)
    temp, fd = _create_temp(final)
    os.close(fd)
    try:
        yield temp
        _fsync_file(temp)
        os.replace(temp, final)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    _fsync_dir(final.parent)


def _create_temp(target: Path) -> tuple[Path, int]:
    """Create and open a new, uniquely named temporary file next to ``target``."""
    while True:
        temp = target.with_name(f".{target.name}.{secrets.token_hex(8)}{_TEMP_SUFFIX}")
        try:
            return temp, os.open(temp, _CREATE_FLAGS, _FILE_MODE)
        except FileExistsError:  # pragma: no cover - 64 random bits colliding
            continue


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(directory: Path) -> None:
    """Persist a directory's entries (a rename, a new file). Best effort where unsupported."""
    try:
        fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:  # e.g. Windows cannot open a directory as a file
        return
    try:
        os.fsync(fd)
    except OSError:  # some filesystems reject fsync on a directory; the rename is still atomic
        pass
    finally:
        os.close(fd)


def _ensure_dir(directory: Path) -> None:
    """``mkdir -p`` that also fsyncs each parent it adds an entry to, so new shards are durable."""
    if directory.is_dir():
        return
    _ensure_dir(directory.parent)
    try:
        directory.mkdir()
    except FileExistsError:  # created concurrently by another writer
        return
    _fsync_dir(directory.parent)
