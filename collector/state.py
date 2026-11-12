"""下载中间态的登记、崩溃恢复与跨进程互斥。

磁盘布局（下载中间态与归档严格分离）::

    staging/
      <bundle>/
        lock                       整个成果的跨进程文件锁
        manifest.gen-<n>.json      本代清单快照（只属于这一代）
        members/<name>.state.json  成员状态（实体标识 + 已校验前缀）
        members/<name>.part        片段数据文件
        members/<name>.part.tmp    崩溃遗留的未完成临时写入（可安全清理）
    archive/
      <bundle>/
        gen-<n>/                   只有整包全部校验通过才会出现
        current -> gen-<n>/        原子切换的当前版本指针

状态文件只登记“已校验前缀” verified_prefix：写入并 fsync 之后才推进，
因此已登记的每个字节都能追溯到清单中的实体（etag）。崩溃可能留下
verified_prefix 之外的未登记尾巴，恢复时先比对再决定复用或截断。
"""

import fcntl
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .model import (
    DiskFullError,
    StateCorruptError,
    atomic_write_json,
    read_json,
)

STATE_VERSION = 1

# 成员生命周期状态
STATE_PENDING = 'pending'        # 尚未开始或尚未拿到任何字节
STATE_DOWNLOADING = 'downloading'
STATE_PAUSED = 'paused'          # 停止后继续：现场完整，等待下次续传
STATE_VERIFIED = 'verified'      # 长度与整文件 sha256 均已通过
STATE_FAILED = 'failed'          # 摘要不符、实体变化等，保留证据待清理
STATE_DISK_FULL = 'disk_full'


@dataclass
class MemberState:
    name: str
    etag: str
    size: int
    sha256: str
    verified_prefix: int
    status: str
    last_error: str = ''
    bytes_written: int = 0

    def to_dict(self):
        return dict(
            version=STATE_VERSION,
            name=self.name,
            etag=self.etag,
            size=self.size,
            sha256=self.sha256,
            verified_prefix=self.verified_prefix,
            bytes_written=self.bytes_written,
            status=self.status,
            last_error=self.last_error,
        )

    @classmethod
    def from_dict(cls, payload):
        if not isinstance(payload, dict) or payload.get('version') != STATE_VERSION:
            raise StateCorruptError('状态文件版本无法识别')
        try:
            state = cls(
                name=str(payload['name']),
                etag=str(payload['etag']),
                size=int(payload['size']),
                sha256=str(payload['sha256']),
                verified_prefix=int(payload['verified_prefix']),
                status=str(payload['status']),
                last_error=str(payload.get('last_error', '')),
                bytes_written=int(payload.get('bytes_written', 0)),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise StateCorruptError(f'状态文件字段损坏：{error}') from error
        if not (0 <= state.verified_prefix <= state.bytes_written <= state.size):
            raise StateCorruptError(
                f'{state.name} 状态边界矛盾：'
                f'verified={state.verified_prefix} written={state.bytes_written} size={state.size}')
        return state


class WorkArea:
    """一个成果在下载目录中的工作区。"""

    def __init__(self, staging_root, bundle):
        self.root = Path(staging_root) / bundle
        self.bundle = bundle
        self.member_dir = self.root / 'members'
        self.lock_path = self.root / 'lock'

    def ensure(self):
        self.member_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path.touch(exist_ok=True)
        return self

    def manifest_path(self, generation):
        return self.root / f'manifest.gen-{generation}.json'

    def state_path(self, name):
        return self.member_dir / f'{name}.state.json'

    def part_path(self, name):
        return self.member_dir / f'{name}.part'

    def load_state(self, name):
        path = self.state_path(name)
        if not path.exists():
            return None
        try:
            return MemberState.from_dict(read_json(path))
        except StateCorruptError:
            raise
        except OSError as error:
            raise StateCorruptError(f'{name} 状态文件不可读：{error}') from error

    def save_state(self, state):
        atomic_write_json(self.state_path(state.name), state.to_dict())

    def remove_member(self, name):
        """删除某个成员的全部中间产物（状态、片段、临时尾巴）。"""

        for path in (self.state_path(name), self.part_path(name),
                     self.member_dir / f'{name}.part.tmp'):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def stale_tmp_parts(self):
        return sorted(self.member_dir.glob('*.part.tmp'))

    @contextmanager
    def lock(self, blocking=True):
        """整个成果一把锁：多个下载进程碰到同一附件时串行化。"""

        self.ensure()
        handle = open(self.lock_path, 'a+')
        try:
            operation = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
            try:
                fcntl.flock(handle.fileno(), operation)
            except BlockingIOError:
                yield False
                return
            yield True
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


def open_part(work_area, name):
    """打开片段文件并返回 (句柄, 磁盘字节数)。句柄调用方负责关闭。"""

    path = work_area.part_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, 'a+b')
    handle.seek(0, os.SEEK_END)
    return handle, handle.tell()


def fsync_part(handle):
    handle.flush()
    os.fsync(handle.fileno())


def truncate_part(handle, length):
    handle.truncate(length)
    handle.flush()
    os.fsync(handle.fileno())


def translate_disk_error(error, name):
    """把写入类错误归一化为磁盘不足；其它 OSError 原样抛出。"""

    if isinstance(error, OSError) and error.errno in (28, 27):
        return DiskFullError(f'磁盘空间不足，{name} 的现场已保留，释放空间后可续传')
    return error
