"""单文件范围下载协议。

协议要点（对应交接场景里的每一条教训）：

1. 每个请求都带 ``Range`` 与 ``If-Range``，响应必须核对实体标识（ETag）。
2. 服务端忽略 Range 回 200 时，绝不把全量响应接到旧片段后面：
   已校验前缀为零则整体重下，前缀非零直接报错保留现场。
3. 416 是范围不被接受，明确失败，绝不计为成功。
4. 崩溃后已写入但未登记的尾巴不会被盲目信任：续传时用远端字节逐块比对，
   一致才纳入已校验前缀，发现差异就在首个不一致处截断重下。
5. 只有整文件长度与 sha256 全部通过，成员才算 verified。
"""

import os
import shutil

from .model import (
    DigestMismatchError,
    DiskFullError,
    DownloadError,
    EntityChangedError,
    RangeIgnoredError,
    RangeRejectedError,
    StateCorruptError,
    sha256_file,
)
from .state import (
    STATE_DISK_FULL,
    STATE_DOWNLOADING,
    STATE_FAILED,
    STATE_PAUSED,
    STATE_VERIFIED,
    MemberState,
    fsync_part,
    open_part,
    translate_disk_error,
    truncate_part,
)
from .transport import StreamClient


class PausedDownload(DownloadError):
    """收到停止信号（如 Ctrl-C），现场完整保留，可随时继续。"""

    code = 'paused'


def parse_content_range(header, name):
    """解析 ``bytes start-end/total``，非法格式按协议错误处理。"""

    if not header or not header.startswith('bytes '):
        raise DownloadError(f'{name} 的 206 响应缺少合法 Content-Range: {header!r}')
    span, _, total = header[len('bytes '):].partition('/')
    start_text, _, end_text = span.partition('-')
    try:
        start, end, total = int(start_text), int(end_text), int(total)
    except ValueError as error:
        raise DownloadError(f'{name} 的 Content-Range 无法解析：{header!r}') from error
    if end < start:
        raise DownloadError(f'{name} 的 Content-Range 区间反转：{header!r}')
    return start, end, total


def require_etag(observed, expected, name):
    if observed is None or observed != expected:
        raise EntityChangedError(name, expected, observed or '<缺失>')


class DownloadHooks:
    """测试与演示用钩子。默认什么都不做。

    after_chunk_fsync 在“数据已 fsync、状态尚未推进”这个精确时刻触发，
    演示中的进程被杀死，正好留下写了但未登记的片段进度；
    after_chunk_commit 在状态登记完成后触发，用于测试的停止/暂停。
    """

    def after_chunk_fsync(self, name, offset, chunk):
        return None

    def after_chunk_commit(self, name, offset, chunk):
        return None


class KillProcessHooks(DownloadHooks):
    """演示用：写满指定字节后模拟进程崩溃（不执行任何 finally 登记）。

    kill_after_bytes 针对文件绝对偏移；skip_below 用于跳过尾巴比对阶段
    已经存在的旧字节，只在真正新写入达到阈值时崩溃。
    """

    def __init__(self, kill_after_bytes, skip_below=0):
        self.kill_after_bytes = kill_after_bytes
        self.skip_below = skip_below

    def after_chunk_fsync(self, name, offset, chunk):
        # chunk 为空表示仅完成了未登记尾巴的比对，不算新写入，不在此处崩溃。
        if not chunk:
            return
        if (self.kill_after_bytes is not None
                and offset >= self.kill_after_bytes
                and offset > self.skip_below):
            os._exit(9)


class MemberDownloader:
    def __init__(self, work_area, client=None, chunk_size=64 * 1024,
                 reserve_free_bytes=0, hooks=None):
        self.work_area = work_area
        self.client = client or StreamClient()
        self.chunk_size = chunk_size
        self.reserve_free_bytes = reserve_free_bytes
        self.hooks = hooks or DownloadHooks()

    # ---- 入口 ----------------------------------------------------------

    def download(self, member):
        """下载或续传单个成员，返回最终 MemberState。"""

        state = self._prepare(member)
        if state.status == STATE_VERIFIED and state.verified_prefix == member.size:
            return state
        self._check_free_space(member, state)
        try:
            self._run(member, state)
        except KeyboardInterrupt:
            error = PausedDownload(f'{member.name} 被停止，已保留到 {state.verified_prefix} 字节')
            self._mark(member, state, STATE_PAUSED, str(error))
            raise error
        except PausedDownload:
            self._mark(member, state, STATE_PAUSED, '收到停止信号，现场已保留')
            raise
        except DiskFullError as error:
            self._mark(member, state, STATE_DISK_FULL, str(error))
            raise
        except DownloadError as error:
            self._mark(member, state, STATE_FAILED, str(error))
            raise
        return state

    # ---- 准备与恢复 -----------------------------------------------------

    def _prepare(self, member):
        state = self.work_area.load_state(member.name)
        if state is None:
            # 没有状态却有片段：无法证明它属于哪个实体，隔离而不是复用。
            orphan = self.work_area.part_path(member.name)
            if orphan.exists():
                quarantine = orphan.parent / f'{orphan.name}.orphan'
                shutil.move(str(orphan), str(quarantine))
                raise StateCorruptError(
                    f'{member.name} 存在无状态片段，已隔离为 {quarantine.name}，'
                    '无法证明其所属实体，需要重新下载')
            state = MemberState(
                name=member.name, etag=member.etag, size=member.size,
                sha256=member.sha256, verified_prefix=0,
                status=STATE_DOWNLOADING, bytes_written=0)
            self.work_area.save_state(state)
            return state

        if state.etag != member.etag:
            raise EntityChangedError(member.name, member.etag, state.etag)
        if state.size != member.size or state.sha256 != member.sha256:
            raise StateCorruptError(
                f'{member.name} 的本地状态与本代清单不一致，片段不能跨代复用')
        if state.status == STATE_VERIFIED:
            if state.verified_prefix != member.size:
                raise StateCorruptError(f'{member.name} 标记 verified 但前缀不完整')
            return state
        part = self.work_area.part_path(member.name)
        on_disk = part.stat().st_size if part.exists() else 0
        if on_disk < state.verified_prefix:
            raise StateCorruptError(
                f'{member.name} 片段短于已校验前缀（{on_disk} < {state.verified_prefix}）')
        state.status = STATE_DOWNLOADING
        state.last_error = ''
        state.bytes_written = on_disk
        self.work_area.save_state(state)
        return state

    def _check_free_space(self, member, state):
        if self.reserve_free_bytes <= 0:
            return
        usage = shutil.disk_usage(self.work_area.member_dir)
        remaining = member.size - state.verified_prefix
        if usage.free < remaining + self.reserve_free_bytes:
            error = DiskFullError(
                f'可用空间 {usage.free} 字节，至少还需 {remaining} 字节'
                f'（另预留 {self.reserve_free_bytes}），{member.name} 未启动写入')
            self._mark(member, state, STATE_DISK_FULL, str(error))
            raise error

    # ---- 协议主体 -------------------------------------------------------

    def _run(self, member, state):
        headers = {
            'Range': f'bytes={state.verified_prefix}-',
            'If-Range': member.etag,
        }
        with self.client.open(member.url, headers) as (status, response_headers, stream):
            observed_etag = response_headers.get('ETag')
            if status == 416:
                # 范围不被接受：可能是远端实体变了，也可能是范围越界，都不是成功。
                raise RangeRejectedError(
                    f'{member.name} 收到 416（Content-Range: '
                    f'{response_headers.get("Content-Range", "<无>")}），'
                    f'本地已校验 {state.verified_prefix} 字节，需重新核对实体')
            if status == 200:
                # 先核对实体：实体变了就明确报告，不被“忽略 Range”掩盖。
                require_etag(observed_etag, member.etag, member.name)
                # 服务端忽略了 Range。严禁把全量响应接到旧文件后面。
                if state.verified_prefix:
                    raise RangeIgnoredError(
                        f'{member.name} 已有 {state.verified_prefix} 字节已校验前缀，'
                        '服务端忽略 Range 返回 200，拒绝拼接，现场已保留')
                self._consume_full(member, state, stream, response_headers)
                self._finalize(member, state)
                return
            if status != 206:
                raise DownloadError(f'{member.name} 收到未预期状态码 {status}')

            require_etag(observed_etag, member.etag, member.name)
            start, end, total = parse_content_range(
                response_headers.get('Content-Range'), member.name)
            if start != state.verified_prefix:
                raise DownloadError(
                    f'{member.name} 响应起始 {start} 与请求前缀 {state.verified_prefix} 不符')
            if total != member.size:
                raise EntityChangedError(
                    member.name, f'length={member.size}', f'length={total}')
            if end >= total:
                raise DownloadError(
                    f'{member.name} Content-Range 结束位 {end} 越界（total={total}）')
            self._consume_partial(member, state, stream)
            self._finalize(member, state)

    def _consume_full(self, member, state, stream, response_headers):
        """200 全量响应：清空旧片段从头接收，长度以清单为准，超长即报错。"""

        declared = response_headers.get('Content-Length')
        if declared is not None and int(declared) != member.size:
            raise EntityChangedError(
                member.name, f'length={member.size}', f'length={declared}')
        handle, _ = open_part(self.work_area, member.name)
        try:
            truncate_part(handle, 0)
            written = 0
            while written < member.size:
                chunk = stream.read(min(self.chunk_size, member.size - written))
                if not chunk:
                    break
                handle.write(chunk)
                fsync_part(handle)
                written += len(chunk)
                state.bytes_written = written
                self.hooks.after_chunk_fsync(member.name, written, chunk)
                self._commit(state, verified=written)
                self.hooks.after_chunk_commit(member.name, written, chunk)
            if written != member.size:
                raise DownloadError(
                    f'{member.name} 传输提前结束：{written}/{member.size} 字节')
            if stream.read(1):
                raise DownloadError(f'{member.name} 响应体超过清单长度 {member.size}')
        except OSError as error:
            raise translate_disk_error(error, member.name) from error
        finally:
            handle.close()

    def _consume_partial(self, member, state, stream):
        """处理 206 流：先核对未登记尾巴，再续传剩余字节。"""

        handle, on_disk = open_part(self.work_area, member.name)
        try:
            offset = state.verified_prefix
            if on_disk > offset:
                # 崩溃留下的未登记尾巴：与远端字节逐块比对，一致才恢复。
                handle.seek(offset)
                tail_length = on_disk - offset
                compared = 0
                while compared < tail_length:
                    chunk = stream.read(min(self.chunk_size, tail_length - compared))
                    if not chunk:
                        raise DownloadError(
                            f'{member.name} 续传响应在尾巴比对阶段提前结束')
                    local = handle.read(len(chunk))
                    base = offset + compared
                    compared += len(chunk)
                    if local == chunk:
                        continue
                    # 定位首个不一致字节，此前一致的部分仍然有效。
                    divergence = base
                    for index in range(min(len(local), len(chunk))):
                        if local[index] != chunk[index]:
                            divergence = base + index
                            break
                    suffix = chunk[divergence - base:]
                    truncate_part(handle, divergence)
                    handle.seek(divergence)
                    handle.write(suffix)
                    fsync_part(handle)
                    new_offset = divergence + len(suffix)
                    state.bytes_written = handle.tell()
                    self._commit(state, verified=new_offset)
                    self.hooks.after_chunk_commit(member.name, new_offset, b'')
                    self._pump_remainder(member, state, stream, handle, new_offset)
                    return
                # 整个未登记尾巴与远端一致，纳入已校验前缀。
                offset = on_disk
                state.bytes_written = on_disk
                self.hooks.after_chunk_fsync(member.name, offset, b'')
                self._commit(state, verified=offset)
                self.hooks.after_chunk_commit(member.name, offset, b'')
            self._pump_remainder(member, state, stream, handle, offset)
        except OSError as error:
            raise translate_disk_error(error, member.name) from error
        finally:
            handle.close()

    def _pump_remainder(self, member, state, stream, handle, offset):
        """把响应流的剩余字节追加到片段，每块 fsync 后才推进已校验前缀。"""

        handle.seek(offset)
        while offset < member.size:
            chunk = stream.read(min(self.chunk_size, member.size - offset))
            if not chunk:
                raise DownloadError(
                    f'{member.name} 传输提前结束：已校验 {offset}/{member.size} 字节')
            handle.write(chunk)
            fsync_part(handle)
            offset += len(chunk)
            state.bytes_written = offset
            self.hooks.after_chunk_fsync(member.name, offset, chunk)
            self._commit(state, verified=offset)
            self.hooks.after_chunk_commit(member.name, offset, chunk)
        if stream.read(1):
            raise DownloadError(f'{member.name} 响应体超过清单长度 {member.size}')

    # ---- 收尾 -----------------------------------------------------------

    def _finalize(self, member, state):
        part = self.work_area.part_path(member.name)
        actual_size = part.stat().st_size
        if actual_size != member.size:
            raise DigestMismatchError(
                member.name, f'长度 {actual_size} != 清单 {member.size}')
        digest = sha256_file(part)
        if digest != member.sha256:
            raise DigestMismatchError(
                member.name, f'sha256 {digest} != 清单 {member.sha256}')
        state.verified_prefix = member.size
        state.bytes_written = member.size
        self._mark(member, state, STATE_VERIFIED, '')

    def _commit(self, state, verified):
        state.verified_prefix = verified
        state.bytes_written = max(state.bytes_written, verified)
        state.status = STATE_DOWNLOADING
        self.work_area.save_state(state)

    def _mark(self, member, state, status, message):
        state.status = status
        state.last_error = message
        self.work_area.save_state(state)
