"""下载层的领域模型：成果清单、成员描述与错误类型。

清单是归档的唯一依据：每个成员都带有长度、sha256 摘要与远端实体标识
（ETag）。任何字节被接受前，都必须能回答“属于哪个实体”。
"""

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path


class DownloadError(RuntimeError):
    """下载层错误基类，CLI 会以非零码退出并保留可查询现场。"""

    code = 'download_error'


class EntityChangedError(DownloadError):
    """远端实体标识与清单不一致，或下载途中发生变化。"""

    code = 'entity_changed'

    def __init__(self, name, expected, observed):
        super().__init__(f'实体标识变化：{name} 清单记录 {expected}，远端返回 {observed}')
        self.name = name
        self.expected = expected
        self.observed = observed


class RangeRejectedError(DownloadError):
    """服务端返回 416，续传范围不被接受，不能当作成功。"""

    code = 'range_rejected'


class RangeIgnoredError(DownloadError):
    """服务端忽略 Range 直接回 200，禁止把全量响应拼到旧片段后。"""

    code = 'range_ignored'


class DigestMismatchError(DownloadError):
    """整文件长度或 sha256 摘要与清单不符。"""

    code = 'digest_mismatch'

    def __init__(self, name, detail):
        super().__init__(f'摘要校验失败：{name} {detail}')
        self.name = name


class DiskFullError(DownloadError):
    """写入或预留空间时磁盘不足，现场保留以便释放空间后续传。"""

    code = 'disk_full'


class ManifestChangedError(DownloadError):
    """下载途中成果清单整体换了一代，当前代的中间产物必须作废。"""

    code = 'manifest_changed'


class StateCorruptError(DownloadError):
    """本地状态文件无法解析或与片段互相矛盾。"""

    code = 'state_corrupt'


@dataclass(frozen=True)
class Member:
    """成果中的一个文件（主文件或附属文件）。"""

    name: str
    size: int
    sha256: str
    etag: str
    url: str = ''

    def to_dict(self):
        return dict(name=self.name, size=self.size, sha256=self.sha256,
                    etag=self.etag, url=self.url)

    @classmethod
    def from_dict(cls, payload):
        try:
            name = str(payload['name'])
            size = int(payload['size'])
            sha256 = str(payload['sha256']).lower()
            etag = str(payload['etag'])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f'成员描述缺少有效字段：{error}') from error
        if not name or name in ('.', '..') or '/' in name or '\\' in name:
            raise ValueError(f'非法成员名称：{name!r}')
        if size < 0:
            raise ValueError(f'成员长度不能为负：{name}')
        if len(sha256) != 64 or any(c not in '0123456789abcdef' for c in sha256):
            raise ValueError(f'成员摘要必须是 64 位十六进制 sha256：{name}')
        if not etag:
            raise ValueError(f'成员缺少远端实体标识：{name}')
        return cls(name=name, size=size, sha256=sha256, etag=etag,
                   url=str(payload.get('url', '')))


@dataclass(frozen=True)
class Manifest:
    """一份成果清单：主文件加附属文件，各自可独立校验。"""

    bundle: str
    generation: int
    members: tuple
    etag: str = ''

    def member(self, name):
        for item in self.members:
            if item.name == name:
                return item
        raise KeyError(name)

    def to_dict(self):
        return dict(bundle=self.bundle, generation=self.generation,
                    etag=self.etag,
                    members=[item.to_dict() for item in self.members])

    @classmethod
    def from_dict(cls, payload):
        if not isinstance(payload, dict):
            raise ValueError('清单必须是 JSON 对象')
        try:
            bundle = str(payload['bundle'])
            generation = int(payload['generation'])
            raw_members = payload['members']
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f'清单缺少有效字段：{error}') from error
        if not bundle:
            raise ValueError('清单缺少成果标识 bundle')
        if not isinstance(raw_members, list) or not raw_members:
            raise ValueError('清单必须至少包含一个成员')
        members = tuple(Member.from_dict(item) for item in raw_members)
        names = [item.name for item in members]
        if len(set(names)) != len(names):
            raise ValueError('清单成员名称重复')
        return cls(bundle=bundle, generation=generation, members=members,
                   etag=str(payload.get('etag', '')))

    @classmethod
    def load(cls, path):
        try:
            return cls.from_dict(json.loads(Path(path).read_text(encoding='utf-8')))
        except FileNotFoundError:
            raise
        except json.JSONDecodeError as error:
            raise ValueError(f'清单不是合法 JSON：{error}') from error


def sha256_file(path):
    """流式计算文件摘要，避免把大附件整体读入内存。"""

    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_json(path, payload):
    """同目录临时文件加原子改名，崩溃只会留下旧版本或 .tmp，不会出现半截状态。"""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    with open(temporary, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_dir(path.parent)


def read_json(path):
    with open(path, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def _fsync_dir(directory):
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
