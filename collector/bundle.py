"""成果级编排：清单确认、成员下载、整包校验、归档发布与清理。

归档目录里只会出现“整个成果都已校验”的版本：任何一个成员未通过，
归档目录保持原样不动。下载中间态全部留在 staging，另目录存放。
"""

import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin

from .downloader import MemberDownloader
from .model import (
    DownloadError,
    Manifest,
    ManifestChangedError,
    Member,
    atomic_write_json,
    sha256_file,
)
from .state import (
    STATE_FAILED,
    STATE_VERIFIED,
    WorkArea,
)
from .transport import Client


def fetch_manifest(manifest_url, client=None, if_none_match=None):
    """拉取并解析成果清单。

    返回 (manifest, etag, unchanged)。unchanged 为 True 表示 304，
    清单在下载途中没有换版。
    """

    client = client or Client()
    headers = {}
    if if_none_match:
        headers['If-None-Match'] = if_none_match
    status, response_headers, raw = client.request(
        manifest_url, headers=headers)
    etag = response_headers.get('ETag')
    if status == 304 and if_none_match:
        return None, etag, True
    if status != 200:
        raise DownloadError(f'清单请求失败：HTTP {status}')
    try:
        payload = json.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DownloadError(f'清单内容无法解析：{error}') from error
    manifest = Manifest.from_dict(payload)
    if etag:
        object.__setattr__(manifest, 'etag', etag)
    base = manifest_url
    resolved = tuple(
        Member(name=item.name, size=item.size, sha256=item.sha256,
               etag=item.etag, url=urljoin(base, item.url or item.name))
        for item in manifest.members
    )
    return Manifest(bundle=manifest.bundle, generation=manifest.generation,
                    members=resolved, etag=manifest.etag), etag, False


@dataclass
class MemberReport:
    name: str
    status: str
    verified_prefix: int
    size: int
    etag: str
    last_error: str = ''
    bytes_written: int = 0

    @property
    def complete(self):
        return self.status == STATE_VERIFIED and self.verified_prefix == self.size


@dataclass
class BundleReport:
    bundle: str
    generation: int
    manifest_etag: str
    archived: bool
    archive_path: str = ''
    members: list = field(default_factory=list)
    error: str = ''
    error_code: str = ''


class BundleManager:
    def __init__(self, staging_root, archive_root, downloader_factory=None,
                 client=None):
        self.staging_root = Path(staging_root)
        self.archive_root = Path(archive_root)
        self.downloader_factory = downloader_factory or (
            lambda work_area: MemberDownloader(work_area))
        self.client = client or Client()

    # ---- 主流程 ---------------------------------------------------------

    def run(self, manifest_url, wait_lock=True):
        """确认清单 → 下载全部成员 → 整包校验后原子发布到归档。"""

        first = fetch_manifest(manifest_url, self.client)
        manifest = first[0]
        work_area = WorkArea(self.staging_root, manifest.bundle).ensure()
        with work_area.lock(blocking=wait_lock) as acquired:
            if not acquired:
                raise DownloadError(f'成果 {manifest.bundle} 正被其它进程处理，未取得锁')
            return self._run_locked(manifest_url, manifest, work_area)

    def _run_locked(self, manifest_url, manifest, work_area):
        snapshot = work_area.manifest_path(manifest.generation)
        atomic_write_json(snapshot, manifest.to_dict())
        self._quarantine_stale_snapshots(work_area, manifest.generation)

        reports = []
        failure = None
        for member in manifest.members:
            existing = work_area.load_state(member.name)
            if existing is not None and (
                    existing.etag != member.etag
                    or existing.size != member.size
                    or existing.sha256 != member.sha256):
                # 本地片段属于旧实体：先隔离再下载，绝不与新代内容混装。
                self._quarantine_member(work_area, member.name,
                                        f'gen-{manifest.generation}-previous-entity')
            downloader = self.downloader_factory(work_area)
            try:
                state = downloader.download(member)
                reports.append(MemberReport(
                    name=member.name, status=state.status,
                    verified_prefix=state.verified_prefix, size=member.size,
                    etag=member.etag, last_error=state.last_error,
                    bytes_written=state.bytes_written))
            except DownloadError as error:
                failure = error
                state = work_area.load_state(member.name)
                reports.append(MemberReport(
                    name=member.name,
                    status=state.status if state else STATE_FAILED,
                    verified_prefix=state.verified_prefix if state else 0,
                    size=member.size, etag=member.etag,
                    last_error=str(error),
                    bytes_written=state.bytes_written if state else 0))
                break

        # 下载途中远端清单可能已经换代，必须重新确认这一个成果的清单。
        refreshed, _, unchanged = fetch_manifest(
            manifest_url, self.client,
            if_none_match=manifest.etag or None)
        if not unchanged and refreshed is not None:
            changed = (refreshed.etag and refreshed.etag != manifest.etag) or (
                [item.to_dict() for item in refreshed.members]
                != [item.to_dict() for item in manifest.members])
            if changed:
                raise ManifestChangedError(
                    f'成果 {manifest.bundle} 清单已换代'
                    f'（{manifest.etag or "<无>"} → {refreshed.etag or "<无>"}），'
                    '本代中间产物隔离保留，请用新清单重新下载，禁止混装')
        all_verified = bool(reports) and all(report.complete for report in reports)
        archive_path = ''
        if all_verified and failure is None:
            archive_path = self._publish(work_area, manifest)
        return BundleReport(
            bundle=manifest.bundle, generation=manifest.generation,
            manifest_etag=manifest.etag,
            archived=all_verified and failure is None,
            archive_path=archive_path, members=reports,
            error=str(failure) if failure else '',
            error_code=getattr(failure, 'code', '') if failure else '')

    # ---- 代次隔离与归档 -------------------------------------------------

    def _quarantine_stale_snapshots(self, work_area, generation):
        """旧代清单快照整体移入隔离区，杜绝不同代附件混装。"""

        quarantine_root = self.staging_root / '_quarantine'
        for path in sorted(work_area.root.glob('manifest.gen-*.json')):
            gen_text = path.stem.split('gen-')[-1]
            if gen_text.isdigit() and int(gen_text) != generation:
                target_dir = quarantine_root / work_area.bundle / f'gen-{gen_text}'
                target_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(target_dir / path.name))

    def _quarantine_member(self, work_area, name, generation_label):
        target_dir = (self.staging_root / '_quarantine' / work_area.bundle
                      / generation_label / 'members')
        target_dir.mkdir(parents=True, exist_ok=True)
        for suffix in ('.state.json', '.part', '.part.tmp', '.part.orphan',
                       '.orphan'):
            source = work_area.member_dir / f'{name}{suffix}'
            if source.exists():
                shutil.move(str(source), str(target_dir / source.name))

    def _publish(self, work_area, manifest):
        """把已校验片段发布到归档：独立复核后复制，再原子切换版本指针。"""

        # 发布前对 staging 片段再做一次独立复核，不信任内存中的结论。
        for member in manifest.members:
            part = work_area.part_path(member.name)
            if not part.exists() or part.stat().st_size != member.size:
                raise DownloadError(f'{member.name} 片段缺失或长度不符，拒绝归档')
            if sha256_file(part) != member.sha256:
                raise DownloadError(f'{member.name} 发布前复核摘要不符，拒绝归档')

        bundle_dir = self.archive_root / manifest.bundle
        version_dir = bundle_dir / f'gen-{manifest.generation}'
        if version_dir.exists() and not self._version_matches(version_dir, manifest):
            raise DownloadError(f'归档版本目录 {version_dir} 已存在且内容冲突')
        if not version_dir.exists():
            staging_version = bundle_dir / f'.staging-gen-{manifest.generation}'
            if staging_version.exists():
                shutil.rmtree(staging_version)
            staging_version.mkdir(parents=True)
            for member in manifest.members:
                shutil.copy2(work_area.part_path(member.name),
                             staging_version / member.name)
            atomic_write_json(staging_version / 'manifest.json',
                              manifest.to_dict())
            # 对归档副本再算一次摘要：归档的是真正校验过的文件。
            for member in manifest.members:
                if sha256_file(staging_version / member.name) != member.sha256:
                    shutil.rmtree(staging_version)
                    raise DownloadError(f'{member.name} 归档副本复核失败')
            os.replace(staging_version, version_dir)
            self._fsync_parent(bundle_dir)

        pointer = bundle_dir / 'current'
        temporary = bundle_dir / 'current.tmp'
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
        os.symlink(f'gen-{manifest.generation}', temporary)
        os.replace(temporary, pointer)
        self._fsync_parent(bundle_dir)
        return str(version_dir)

    @staticmethod
    def _version_matches(version_dir, manifest):
        manifest_path = version_dir / 'manifest.json'
        if not manifest_path.exists():
            return False
        try:
            recorded = Manifest.load(manifest_path)
        except ValueError:
            return False
        if {item.name for item in recorded.members} != {
                item.name for item in manifest.members}:
            return False
        for member in manifest.members:
            path = version_dir / member.name
            if not path.exists() or path.stat().st_size != member.size:
                return False
            if sha256_file(path) != member.sha256:
                return False
        return True

    @staticmethod
    def _fsync_parent(directory):
        try:
            descriptor = os.open(directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


# ---- 状态检查与清理 ------------------------------------------------------


def inspect(staging_root, archive_root):
    """汇总 staging、隔离区与归档的可查询状态。"""

    staging_root = Path(staging_root)
    archive_root = Path(archive_root)
    result = dict(bundles=[], quarantine=[], archive=[])
    if not staging_root.exists():
        return result
    for bundle_dir in sorted(path for path in staging_root.iterdir()
                             if path.is_dir() and path.name != '_quarantine'):
        work_area = WorkArea(staging_root, bundle_dir.name)
        snapshots = sorted(path.name for path in bundle_dir.glob('manifest.gen-*.json'))
        members = []
        member_dir = bundle_dir / 'members'
        if member_dir.exists():
            for state_path in sorted(member_dir.glob('*.state.json')):
                try:
                    payload = json.loads(state_path.read_text(encoding='utf-8'))
                except (OSError, json.JSONDecodeError):
                    members.append(dict(name=state_path.name, status='state_corrupt'))
                    continue
                part = member_dir / f"{payload.get('name', '?')}.part"
                members.append(dict(
                    name=payload.get('name'),
                    status=payload.get('status'),
                    etag=payload.get('etag'),
                    verified_prefix=payload.get('verified_prefix'),
                    bytes_written=payload.get('bytes_written'),
                    size=payload.get('size'),
                    part_on_disk=part.stat().st_size if part.exists() else 0,
                    last_error=payload.get('last_error', '')))
            for tmp in sorted(member_dir.glob('*.part.tmp')):
                members.append(dict(name=tmp.name, status='uncommitted_tmp',
                                    bytes_written=tmp.stat().st_size))
            for orphan in sorted(member_dir.glob('*.part.orphan')):
                members.append(dict(name=orphan.name, status='orphan_part',
                                    bytes_written=orphan.stat().st_size))
        result['bundles'].append(dict(
            bundle=bundle_dir.name, snapshots=snapshots,
            locked=(work_area.lock_path.exists()), members=members))
    quarantine = staging_root / '_quarantine'
    if quarantine.exists():
        for path in sorted(quarantine.rglob('*')):
            if path.is_file():
                result['quarantine'].append(str(path))
    if archive_root.exists():
        for bundle_dir in sorted(path for path in archive_root.iterdir() if path.is_dir()):
            versions = sorted(path.name for path in bundle_dir.iterdir()
                              if path.is_dir() and path.name.startswith('gen-'))
            pointer = bundle_dir / 'current'
            result['archive'].append(dict(
                bundle=bundle_dir.name, versions=versions,
                current=os.readlink(pointer) if pointer.is_symlink() else None))
    return result


def cleanup(staging_root, archive_root, bundle=None, force=False):
    """安全清理。

    默认只清理崩溃遗留的未提交临时文件（.part.tmp），不动任何已登记进度；
    force=True 时删除指定成果（或全部）的整个 staging 工作区。
    归档目录永远不会被这个函数删除。
    """

    staging_root = Path(staging_root)
    removed = []
    if not staging_root.exists():
        return dict(removed=removed)
    targets = [WorkArea(staging_root, bundle).root] if bundle else [
        path for path in staging_root.iterdir() if path.is_dir()]
    for target in targets:
        if not target.exists():
            continue
        if force:
            for path in sorted(target.rglob('*')):
                if path.is_file() or path.is_symlink():
                    removed.append(str(path))
            shutil.rmtree(target, ignore_errors=True)
            continue
        for tmp in sorted(target.rglob('*.part.tmp')):
            tmp.unlink()
            removed.append(str(tmp))
    return dict(removed=removed)
