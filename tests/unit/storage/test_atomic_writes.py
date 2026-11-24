import os
import stat
import threading
from pathlib import Path

import pytest

from scraping_pipeline.storage.atomic import (
    atomic_path,
    atomic_write_bytes,
    atomic_write_text,
    is_temp_name,
)


def _leftovers(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir() if is_temp_name(p.name))


def test_write_creates_parents_and_replaces(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b" / "file.bin"
    atomic_write_bytes(target, b"one")
    atomic_write_bytes(target, b"two")
    assert target.read_bytes() == b"two"
    assert _leftovers(target.parent) == []


def test_text_is_written_verbatim_without_newline_translation(tmp_path: Path) -> None:
    target = tmp_path / "t.csv"
    atomic_write_text(target, "a,b\nñ,2\n")
    assert target.read_bytes() == "a,b\nñ,2\n".encode()


def test_new_file_gets_umask_permissions_not_0600(tmp_path: Path) -> None:
    target = tmp_path / "perm.txt"
    atomic_write_bytes(target, b"x")
    umask = os.umask(0)
    os.umask(umask)
    assert target.stat().st_mode & 0o777 == 0o666 & ~umask


def test_failed_replace_keeps_old_file_and_removes_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "f.txt"
    target.write_bytes(b"old")

    def boom(src: object, dst: object) -> None:
        raise OSError("disk on fire")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="disk on fire"):
        atomic_write_bytes(target, b"new")
    assert target.read_bytes() == b"old"
    assert _leftovers(tmp_path) == []


def test_failed_fsync_mid_write_keeps_old_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "f.txt"
    target.write_bytes(b"old")
    seen: list[bytes] = []

    def failing_fsync(fd: int) -> None:
        # The temp file already holds the new bytes; the target must still hold the old ones.
        seen.append(target.read_bytes())
        raise OSError("fsync failed")

    monkeypatch.setattr(os, "fsync", failing_fsync)
    with pytest.raises(OSError, match="fsync failed"):
        atomic_write_bytes(target, b"new")
    assert seen == [b"old"]
    assert target.read_bytes() == b"old"
    assert _leftovers(tmp_path) == []


def test_data_is_fsynced_before_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd: int) -> None:
        events.append("fsync")
        real_fsync(fd)

    def replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        events.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)
    (tmp_path / "d").mkdir()
    atomic_write_bytes(tmp_path / "d" / "x", b"data")
    # file fsync, rename, then directory fsync
    assert events == ["fsync", "replace", "fsync"]


def test_keyboard_interrupt_also_cleans_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupted(src: object, dst: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(os, "replace", interrupted)
    with pytest.raises(KeyboardInterrupt):
        atomic_write_bytes(tmp_path / "f", b"x")
    assert list(tmp_path.iterdir()) == []


def test_atomic_path_commits_on_success(tmp_path: Path) -> None:
    target = tmp_path / "out" / "table.parquet"
    with atomic_path(target) as temp:
        assert temp.parent == target.parent
        assert is_temp_name(temp.name)
        assert temp.exists()  # the name is reserved
        assert not target.exists()  # nothing visible until commit
        temp.write_bytes(b"streamed")
    assert target.read_bytes() == b"streamed"
    assert _leftovers(target.parent) == []


def test_atomic_path_raising_midway_keeps_old_file(tmp_path: Path) -> None:
    target = tmp_path / "table.parquet"
    target.write_bytes(b"old")

    def crashing_writer() -> None:
        with atomic_path(target) as temp:
            temp.write_bytes(b"half")
            raise RuntimeError("writer crashed")

    with pytest.raises(RuntimeError, match="writer crashed"):
        crashing_writer()
    assert target.read_bytes() == b"old"
    assert _leftovers(tmp_path) == []


def test_atomic_path_failed_replace_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "x"

    def boom(src: object, dst: object) -> None:
        raise OSError("nope")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="nope"), atomic_path(target) as temp:
        temp.write_bytes(b"data")
    assert list(tmp_path.iterdir()) == []


def test_atomic_path_tolerates_writer_deleting_temp_on_failure(tmp_path: Path) -> None:
    def writer_that_cleans_up_itself() -> None:
        with atomic_path(tmp_path / "t") as temp:
            temp.unlink()
            raise ValueError("x")

    with pytest.raises(ValueError, match="x"):
        writer_that_cleans_up_itself()
    assert list(tmp_path.iterdir()) == []


def test_concurrent_writers_never_collide_and_readers_see_whole_files(tmp_path: Path) -> None:
    target = tmp_path / "shared.bin"
    payloads = [bytes([i]) * 200_000 for i in range(8)]
    atomic_write_bytes(target, payloads[0])
    errors: list[BaseException] = []
    stop = threading.Event()
    torn: list[int] = []

    def writer(payload: bytes) -> None:
        try:
            for _ in range(15):
                atomic_write_bytes(target, payload)
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    def reader() -> None:
        while not stop.is_set():
            data = target.read_bytes()
            if data not in payloads:  # pragma: no cover - would be the bug
                torn.append(len(data))

    reading = threading.Thread(target=reader)
    reading.start()
    writers = [threading.Thread(target=writer, args=(p,)) for p in payloads]
    for thread in writers:
        thread.start()
    for thread in writers:
        thread.join()
    stop.set()
    reading.join()
    assert errors == []
    assert torn == []
    assert target.read_bytes() in payloads
    assert _leftovers(tmp_path) == []


def test_directory_fsync_is_skipped_where_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_open = os.open

    def no_dir_open(path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if Path(path).is_dir():
            raise PermissionError("cannot open a directory here")
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", no_dir_open)
    atomic_write_bytes(tmp_path / "f", b"ok")
    assert (tmp_path / "f").read_bytes() == b"ok"


def test_directory_fsync_rejected_by_filesystem_is_tolerated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_fsync = os.fsync
    rejected: list[int] = []

    def fsync(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            rejected.append(fd)
            raise OSError("EINVAL")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    atomic_write_bytes(tmp_path / "new" / "f", b"ok")
    assert (tmp_path / "new" / "f").read_bytes() == b"ok"
    assert len(rejected) == 2  # the new shard's parent, then the rename's directory


def test_ensure_dir_tolerates_concurrent_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_dir = tmp_path / "race"
    real_mkdir = Path.mkdir

    def racing_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        real_mkdir(self)  # another writer wins the race
        raise FileExistsError(str(self))

    monkeypatch.setattr(Path, "mkdir", racing_mkdir)
    atomic_write_bytes(target_dir / "f", b"ok")
    assert (target_dir / "f").read_bytes() == b"ok"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (".file.json.0123456789abcdef.tmp", True),
        ("file.json", False),
        (".file.json.0123.tmp", False),
        ("x.0123456789abcdef.tmp", False),
    ],
)
def test_is_temp_name(name: str, expected: bool) -> None:
    assert is_temp_name(name) is expected


@pytest.mark.skipif(not hasattr(os, "O_ACCMODE"), reason="needs fcntl access-mode flags")
def test_atomic_path_fsyncs_through_a_writable_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Emulate Windows, where fsync (FlushFileBuffers) fails with EBADF on a read-only descriptor."""
    import errno  # noqa: PLC0415 - only this test needs it
    import fcntl  # noqa: PLC0415 - POSIX-only module, guarded by the skipif

    real_fsync = os.fsync
    file_syncs: list[int] = []

    def windows_like_fsync(fd: int) -> None:
        read_only = fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY
        if read_only:
            raise OSError(errno.EBADF, "Bad file descriptor")
        file_syncs.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", windows_like_fsync)
    target = tmp_path / "out.parquet"
    with atomic_path(target) as temp:
        temp.write_bytes(b"columns")
    assert target.read_bytes() == b"columns"
    assert len(file_syncs) == 1  # the file was synced; the read-only directory fsync was skipped
    assert _leftovers(tmp_path) == []
