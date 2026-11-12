import hashlib
import tempfile
import unittest
from pathlib import Path

from collector.bundle import BundleManager, cleanup, fetch_manifest, inspect
from collector.downloader import (
    DownloadHooks,
    MemberDownloader,
    PausedDownload,
    RangeIgnoredError,
    RangeRejectedError,
)
from collector.fixtureserver import (
    MODE_CORRUPT,
    MODE_FORCE_416,
    MODE_IGNORE_RANGE,
    load_seed,
    serve_fixtures,
)
from collector.model import (
    DigestMismatchError,
    DownloadError,
    EntityChangedError,
    Manifest,
    Member,
    sha256_file,
)
from collector.state import (
    STATE_DISK_FULL,
    STATE_PAUSED,
    STATE_VERIFIED,
    WorkArea,
)

ROOT = Path(__file__).resolve().parent.parent


class StopAfterBytes(DownloadHooks):
    """在登记到指定字节后中断本次下载，状态已落盘，等价于停止后继续。"""

    def __init__(self, limit):
        self.limit = limit
        self.stopped = False

    def after_chunk_commit(self, name, offset, chunk):
        if not self.stopped and offset >= self.limit:
            self.stopped = True
            raise _StopLoop()


class _StopLoop(RuntimeError):
    pass


class Harness(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.staging = root / 'staging'
        self.archive = root / 'archive'
        self.assets = load_seed(ROOT / 'fixtures' / 'sample.json')
        self.server_cm = serve_fixtures(self.assets)
        self.server = self.server_cm.__enter__()
        self.manifest, _, _ = fetch_manifest(self.server.manifest_url())

    def tearDown(self):
        self.server_cm.__exit__(None, None, None)
        self.temporary.cleanup()

    def member(self, name, **overrides):
        payload = self.manifest.member(name).to_dict()
        payload['url'] = self.server.file_url(name)
        payload.update(overrides)
        return Member.from_dict(payload)

    def work_area(self):
        return WorkArea(self.staging, self.manifest.bundle).ensure()

    def downloader(self, **kwargs):
        kwargs.setdefault('chunk_size', 2)
        return MemberDownloader(self.work_area(), **kwargs)

    def state_of(self, name):
        return self.work_area().load_state(name)

    def stop_at(self, name, limit):
        with self.assertRaises(_StopLoop):
            self.downloader(hooks=StopAfterBytes(limit)).download(self.member(name))
            pass


class ProtocolTests(Harness):
    def test_full_download_verifies_length_and_digest(self):
        state = self.downloader().download(self.member('note.txt'))
        self.assertEqual(state.status, STATE_VERIFIED)
        part = self.work_area().part_path('note.txt')
        self.assertEqual(part.read_bytes(), b'hello')
        self.assertEqual(sha256_file(part), self.manifest.member('note.txt').sha256)

    def test_resume_uses_range_requests(self):
        self.stop_at('map.bin', 2)
        self.assertEqual(self.state_of('map.bin').verified_prefix, 2)
        ranges = [call['range'] for call in self.server.state.calls
                  if call['path'] == '/files/map.bin' and call['range']]
        self.assertEqual(ranges, ['bytes=0-'])
        state = self.downloader().download(self.member('map.bin'))
        self.assertEqual(state.status, STATE_VERIFIED)
        ranges = [call['range'] for call in self.server.state.calls
                  if call['path'] == '/files/map.bin' and call['range']]
        self.assertEqual(ranges, ['bytes=0-', 'bytes=2-'])
        self.assertEqual(self.work_area().part_path('map.bin').read_bytes(), b'abcdef')

    def test_range_ignored_200_with_prefix_is_refused(self):
        self.stop_at('map.bin', 2)
        self.server.control(dict(action='set_mode', mode=MODE_IGNORE_RANGE))
        with self.assertRaises(RangeIgnoredError):
            self.downloader().download(self.member('map.bin'))
        # 旧片段不能被全量响应污染或截断
        self.assertEqual(self.work_area().part_path('map.bin').read_bytes(), b'ab')
        self.assertEqual(self.state_of('map.bin').status, 'failed')

    def test_range_ignored_200_without_prefix_restarts_cleanly(self):
        self.server.control(dict(action='set_mode', mode=MODE_IGNORE_RANGE))
        state = self.downloader().download(self.member('map.bin'))
        self.assertEqual(state.status, STATE_VERIFIED)
        self.assertEqual(self.work_area().part_path('map.bin').read_bytes(), b'abcdef')

    def test_416_is_never_success(self):
        self.stop_at('map.bin', 2)
        self.server.control(dict(action='set_mode', mode=MODE_FORCE_416))
        with self.assertRaises(RangeRejectedError):
            self.downloader().download(self.member('map.bin'))
        self.assertNotEqual(self.state_of('map.bin').status, STATE_VERIFIED)

    def test_unregistered_tail_is_compared_and_reused(self):
        # 已登记 2 字节，但磁盘上有 4 字节（写后“崩溃”、状态未推进）
        self.stop_at('map.bin', 4)
        # 手工回退状态，制造未登记尾巴
        state = self.state_of('map.bin')
        state.verified_prefix = 2
        self.work_area().save_state(state)
        self.assertEqual(self.work_area().part_path('map.bin').stat().st_size, 4)
        resumed = self.downloader().download(self.member('map.bin'))
        self.assertEqual(resumed.status, STATE_VERIFIED)
        self.assertEqual(resumed.verified_prefix, 6)
        self.assertEqual(self.work_area().part_path('map.bin').read_bytes(), b'abcdef')

    def test_unregistered_tail_with_divergence_truncates_at_first_bad_byte(self):
        self.stop_at('map.bin', 2)
        part = self.work_area().part_path('map.bin')
        with open(part, 'ab') as handle:
            handle.write(b'XY')  # 正确尾巴应是 cd
        state = self.downloader().download(self.member('map.bin'))
        self.assertEqual(state.status, STATE_VERIFIED)
        self.assertEqual(part.read_bytes(), b'abcdef')

    def test_etag_change_is_detected_on_resume(self):
        self.stop_at('map.bin', 2)
        self.server.control(dict(
            action='replace_asset', name='map.bin',
            etag='"a2"', data='YWJjWFl6'))
        # If-Range 不匹配时服务器按 RFC 回 200，前缀非零 -> 拒绝并报实体变化
        with self.assertRaises((EntityChangedError, RangeIgnoredError)):
            self.downloader().download(self.member('map.bin'))
        self.assertEqual(self.work_area().part_path('map.bin').read_bytes(), b'ab')

    def test_serving_entity_override_keeps_manifest_but_changes_etag(self):
        self.stop_at('map.bin', 2)
        self.server.control(dict(
            action='override_serving_entity', name='map.bin',
            etag='"a2"', data='YWJjWFl6'))
        # 清单仍声明 a1，但文件实体已是 a2：ETag 先于前缀检查被识别
        with self.assertRaises(EntityChangedError):
            self.downloader().download(self.member('map.bin'))
        self.server.control(dict(action='clear_overrides'))

    def test_416_preserves_verified_prefix_and_marks_failed(self):
        self.stop_at('map.bin', 4)
        self.server.control(dict(action='set_mode', mode=MODE_FORCE_416))
        with self.assertRaises(RangeRejectedError):
            self.downloader().download(self.member('map.bin'))
        state = self.state_of('map.bin')
        self.assertEqual(state.verified_prefix, 4)
        self.assertEqual(state.status, 'failed')

    def test_corrupt_bytes_fail_digest_and_never_publish(self):
        self.server.control(dict(action='set_mode', mode=MODE_CORRUPT))
        with self.assertRaises(DigestMismatchError):
            self.downloader().download(self.member('map.bin'))
        self.assertEqual(self.state_of('map.bin').status, 'failed')

    def test_orphan_part_without_state_is_quarantined(self):
        work_area = self.work_area()
        work_area.part_path('map.bin').write_bytes(b'abcdef')
        with self.assertRaises(Exception) as caught:
            self.downloader().download(self.member('map.bin'))
        self.assertEqual(caught.exception.code, 'state_corrupt')
        self.assertFalse(work_area.part_path('map.bin').exists())
        self.assertTrue((work_area.member_dir / 'map.bin.part.orphan').exists())

    def test_disk_full_precheck_blocks_write(self):
        with self.assertRaises(Exception) as caught:
            self.downloader(reserve_free_bytes=10 ** 12).download(
                self.member('map.bin'))
        self.assertEqual(caught.exception.code, 'disk_full')
        self.assertEqual(self.state_of('map.bin').status, STATE_DISK_FULL)

    def test_disk_full_during_write_is_normalized_and_resumable(self):
        import errno as errno_module

        class FullHandle:
            def __init__(self, real):
                self._real = real

            def write(self, data):
                raise OSError(errno_module.ENOSPC, 'No space left on device')

            def __getattr__(self, name):
                return getattr(self._real, name)

        downloader = self.downloader()
        from collector import downloader as module
        saved = module.open_part

        def fake_open_part(work_area, name):
            handle, length = saved(work_area, name)
            return FullHandle(handle), length

        module.open_part = fake_open_part
        try:
            with self.assertRaises(Exception) as caught:
                downloader.download(self.member('map.bin'))
            self.assertEqual(caught.exception.code, 'disk_full')
        finally:
            module.open_part = saved
        # 释放“空间”后可正常续传完成
        state = self.downloader().download(self.member('map.bin'))
        self.assertEqual(state.status, STATE_VERIFIED)

    def test_state_bound_to_entity_rejects_other_etag(self):
        self.stop_at('map.bin', 2)
        new_member = self.member('map.bin', etag='"a9"', sha256='00' * 32)
        with self.assertRaises(EntityChangedError):
            self.downloader().download(new_member)

    def test_verified_member_is_idempotent(self):
        self.downloader().download(self.member('note.txt'))
        state = self.downloader().download(self.member('note.txt'))
        self.assertEqual(state.status, STATE_VERIFIED)
        self.assertEqual(len([c for c in self.server.state.calls
                              if c['path'] == '/files/note.txt']), 1)

    def test_paused_state_survives_keyboard_interrupt(self):
        class InterruptHooks(DownloadHooks):
            def after_chunk_commit(self, name, offset, chunk):
                raise KeyboardInterrupt
        with self.assertRaises(PausedDownload):
            self.downloader(hooks=InterruptHooks()).download(self.member('map.bin'))
        self.assertEqual(self.state_of('map.bin').status, STATE_PAUSED)
        self.downloader().download(self.member('map.bin'))
        self.assertEqual(self.state_of('map.bin').status, STATE_VERIFIED)


class BundleTests(Harness):
    def _manager(self):
        def factory(work_area):
            return MemberDownloader(work_area, chunk_size=2)
        return BundleManager(self.staging, self.archive, downloader_factory=factory)

    def test_bundle_publishes_only_after_all_members_verified(self):
        report = self._manager().run(self.server.manifest_url())
        self.assertTrue(report.archived)
        current = (self.archive / self.manifest.bundle / 'current').resolve()
        self.assertTrue((current / 'map.bin').is_file())
        self.assertTrue((current / 'note.txt').is_file())
        self.assertEqual(Manifest.load(current / 'manifest.json').generation, 1)

    def test_failed_member_keeps_archive_empty(self):
        self.server.control(dict(action='set_mode', mode=MODE_CORRUPT))
        report = self._manager().run(self.server.manifest_url())
        self.assertFalse(report.archived)
        self.assertFalse(self.archive.exists())
        self.assertEqual(report.error_code, 'digest_mismatch')

    def test_manifest_generation_change_quarantines_old_work(self):
        self._manager().run(self.server.manifest_url())
        self.server.control(dict(
            action='replace_asset', name='map.bin',
            etag='"a2"', data='YWJjWFl6'))
        report = self._manager().run(self.server.manifest_url())
        self.assertTrue(report.archived)
        self.assertEqual(report.generation, 2)
        bundle_dir = self.archive / self.manifest.bundle
        self.assertEqual(bundle_dir.joinpath('current').resolve().name, 'gen-2')
        self.assertTrue((bundle_dir / 'gen-1').is_dir())
        new_digest = hashlib.sha256(b'abcXYz').hexdigest()
        self.assertEqual(sha256_file(bundle_dir / 'gen-2' / 'map.bin'), new_digest)
        # 旧代片段被隔离，没有和新代混装
        self.assertTrue((self.staging / '_quarantine').exists())

    def test_status_and_cleanup(self):
        self._manager().run(self.server.manifest_url())
        status = inspect(self.staging, self.archive)
        self.assertEqual(status['bundles'][0]['bundle'], self.manifest.bundle)
        self.assertTrue(status['archive'][0]['current'].startswith('gen-'))
        tmp = self.work_area().member_dir / 'x.part.tmp'
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(b'tmp')
        result = cleanup(self.staging, self.archive)
        self.assertTrue(all(path.endswith('.part.tmp') for path in result['removed']))
        cleanup(self.staging, self.archive, force=True)
        self.assertTrue(any(self.archive.rglob('map.bin')))

    def test_manifest_validation_rejects_bad_payload(self):
        with self.assertRaises(ValueError):
            Manifest.from_dict(dict(bundle='', generation=1, members=[]))
        with self.assertRaises(ValueError):
            Member.from_dict(dict(name='../x', size=1,
                                  sha256='00' * 32, etag='"e"'))
        with self.assertRaises(ValueError):
            Member.from_dict(dict(name='ok', size=1,
                                  sha256='not-hex', etag='"e"'))

    def test_nonblocking_run_fails_fast_when_locked(self):
        import fcntl
        work_area = WorkArea(self.staging, self.manifest.bundle).ensure()
        handle = open(work_area.lock_path)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            with self.assertRaises(DownloadError) as caught:
                self._manager().run(self.server.manifest_url(), wait_lock=False)
            self.assertIn('未取得锁', str(caught.exception))
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


if __name__ == '__main__':
    unittest.main()
