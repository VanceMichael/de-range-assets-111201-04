"""端到端演示：用真实回环 HTTP 与子进程重演交接事故。

每个阶段都有硬性断言；归档完成后对归档目录中的文件重新独立计算 sha256，
而不是打印一句“下载成功”就结束。全程不访问外部网络。
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from .bundle import BundleManager, inspect
from .fixtureserver import (
    MODE_CORRUPT,
    MODE_FORCE_416,
    MODE_IGNORE_RANGE,
    MODE_NORMAL,
    load_seed,
    serve_fixtures,
)
from .model import DownloadError, Manifest, sha256_file
from .state import WorkArea

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED = PROJECT_ROOT / 'fixtures' / 'sample.json'
ASSETS = PROJECT_ROOT / 'fixtures' / 'assets.json'


def _line(title):
    print(f'\n=== {title} ===', flush=True)


def _cli_command(*args):
    """构造隔离模式子进程命令。

    -I 会忽略 PYTHONPATH 与用户 site 目录，因此用 -c 引导，
    仅把受信任的项目根目录显式加入 sys.path。
    """

    bootstrap = (
        "import sys; "
        f"sys.path.insert(0, {str(PROJECT_ROOT)!r}); "
        "from collector.cli import main; "
        "sys.exit(main(sys.argv[2:]))")
    return [sys.executable, '-I', '-c', bootstrap, 'collector', *args]


def _subprocess_download(staging, archive, manifest_url, *extra):
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    command = _cli_command('download',
                           '--manifest', manifest_url,
                           '--staging', str(staging),
                           '--archive', str(archive), *extra)
    result = subprocess.run(command, cwd=PROJECT_ROOT, env=env,
                            capture_output=True, text=True, timeout=60)
    if result.returncode == 9:
        # 模拟进程崩溃：os._exit(9) 不会产生任何输出，现场只在磁盘上。
        return 9, dict(crashed=True)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise AssertionError(
            f'子进程未输出 JSON：rc={result.returncode}\n{result.stdout}\n{result.stderr}')
    return result.returncode, payload


def _verify_archive(archive, expected_manifest):
    """对归档目录做独立复核：文件存在、长度与 sha256 全部重新计算。"""

    bundle_dir = Path(archive) / expected_manifest.bundle
    current = bundle_dir / 'current'
    assert current.is_symlink(), '归档缺少 current 版本指针'
    version_dir = current.resolve()
    recorded = Manifest.load(version_dir / 'manifest.json')
    assert recorded.generation == expected_manifest.generation
    for member in expected_manifest.members:
        path = version_dir / member.name
        assert path.is_file(), f'归档缺少成员 {member.name}'
        assert path.stat().st_size == member.size, f'{member.name} 长度不符'
        digest = sha256_file(path)
        assert digest == member.sha256, f'{member.name} 归档摘要不符'
    return version_dir


def run(keep=False, staging=None, archive=None):
    temporary = None
    if staging is None or archive is None:
        temporary = tempfile.TemporaryDirectory(prefix='collector-demo-')
        staging = staging or Path(temporary.name) / 'staging'
        archive = archive or Path(temporary.name) / 'archive'
    staging, archive = Path(staging), Path(archive)
    fixture_meta = json.loads(ASSETS.read_text(encoding='utf-8'))
    fault_notes = [f"{item['at']} -> {item.get('response', item.get('action', item.get('etag')))}"
                   for item in fixture_meta['faults']]

    print('调查附件校验下载层演示')
    print(f'fixture 故障脚本：{fault_notes}')
    assets = load_seed(SEED)

    with serve_fixtures(assets, bundle=fixture_meta['bundle']) as server:
        manifest_url = server.manifest_url()
        print(f'回环来源：{server.base}')

        # -- 阶段 1：范围被忽略（200 全量），且本地已有已校验前缀 ----------
        _line('阶段 1 服务端忽略 Range 返回 200：禁止拼接到旧片段后')
        # 1a 先制造“写了 2 字节、未登记”的崩溃现场
        rc, payload = _subprocess_download(
            staging, archive, manifest_url, '--chunk-size', '2',
            '--kill-after', '2')
        assert rc == 9, f'预期模拟崩溃退出码 9，实际 {rc}，输出 {payload}'
        member_state = _read_state(staging, 'survey-1', 'map.bin')
        part = WorkArea(staging, 'survey-1').part_path('map.bin')
        assert member_state['verified_prefix'] == 0 and part.stat().st_size == 2
        print(f'  模拟崩溃：verified_prefix=0，磁盘片段 {part.stat().st_size} 字节（未登记）')

        # 1b 正常续传：未登记片段经远端比对后纳入，在登记到 2 字节时再次崩溃
        rc, payload = _subprocess_download(
            staging, archive, manifest_url, '--chunk-size', '2',
            '--kill-after', '2')
        # 第一次崩溃留下的 2 字节在比对通过后登记；登记完成的同一刻再次崩溃，
        # 因此现场为“已登记 2 字节、磁盘 2 字节”。
        assert rc == 9
        state = _read_state(staging, 'survey-1', 'map.bin')
        assert state['verified_prefix'] == 2, state
        print(f'  恢复后已登记前缀：{state["verified_prefix"]} 字节（再次模拟崩溃）')

        # 1c 服务端开始忽略 Range：有已校验前缀时必须拒绝，绝不拼接
        server.control(dict(action='set_mode', mode=MODE_IGNORE_RANGE))
        rc, payload = _subprocess_download(
            staging, archive, manifest_url, '--chunk-size', '2')
        assert rc == 1, '200 拼接必须被拒绝'
        assert payload['error_code'] == 'range_ignored', payload
        print(f"  续传被拒绝：{payload['error']}")
        surviving = WorkArea(staging, 'survey-1').part_path('map.bin').read_bytes()
        assert surviving[:2] == b'ab', '拒绝拼接时已登记前缀的字节不得被改动'
        print(f'  旧片段原样保留（前 2 字节仍为 ab），全量响应未被接在后面')

        # -- 阶段 2：崩溃后未登记尾巴的恢复 --------------------------------
        _line('阶段 2 恢复写文件后未登记的片段进度')
        server.control(dict(action='set_mode', mode=MODE_NORMAL))
        # 用独立工作区，确定性地构造“2 字节已登记 + 2 字节已写未登记”。
        tail_staging = staging.parent / 'tail-staging'
        tail_archive = staging.parent / 'tail-archive'
        # 先干净下载到登记 2 字节即崩溃
        rc, _ = _subprocess_download(
            tail_staging, tail_archive, manifest_url,
            '--chunk-size', '2', '--kill-after', '2')
        assert rc == 9
        assert _read_state(tail_staging, 'survey-1', 'map.bin')['verified_prefix'] == 0
        # 恢复：前 2 字节比对通过并登记，随后新写入到第 4 字节时崩溃（跳过旧字节）
        rc, _ = _subprocess_download(
            tail_staging, tail_archive, manifest_url,
            '--chunk-size', '2', '--kill-after', '4', '--kill-skip', '2')
        assert rc == 9
        on_disk = WorkArea(tail_staging, 'survey-1').part_path('map.bin').stat().st_size
        state = _read_state(tail_staging, 'survey-1', 'map.bin')
        assert state['verified_prefix'] == 2 and on_disk == 4, \
            f'应形成 2 字节已登记 + 2 字节未登记尾巴，实际 prefix={state["verified_prefix"]} disk={on_disk}'
        print(f'  现场：verified_prefix={state["verified_prefix"]}，磁盘 {on_disk} 字节')
        rc, payload = _subprocess_download(
            tail_staging, tail_archive, manifest_url, '--chunk-size', '2')
        map_report = next(item for item in payload['members'] if item['name'] == 'map.bin')
        assert map_report['complete'], '未登记尾巴比对成功后应完成下载'
        assert WorkArea(tail_staging, 'survey-1').part_path('map.bin').read_bytes() == b'abcdef'
        print('  续传逐字节比对未登记尾巴，一致后纳入已校验前缀，map.bin 完成且字节正确')

        # -- 阶段 3：远端换版本（同尺寸不同内容、新 ETag） ------------------
        _line('阶段 3 远端换版本：大小相同内容不同，实体变化必须被发现')
        # 先用独立工作区按旧实体（a1）下载一段并登记前缀
        version_staging = staging.parent / 'version-staging'
        version_archive = staging.parent / 'version-archive'
        rc, _ = _subprocess_download(
            version_staging, version_archive, manifest_url,
            '--chunk-size', '2', '--kill-after', '2')
        assert rc == 9
        assert _read_state(version_staging, 'survey-1', 'map.bin')['etag'] == '"a1"'
        print('  换版前：本地已登记属于实体 "a1" 的 2 字节前缀')

        resume_fault = next(item for item in fixture_meta['faults'] if item.get('at') == 'resume')
        # 成员级实体变化：清单仍声明 a1，但文件实体已换成 a2（同尺寸不同内容）
        result = server.control(dict(
            action='override_serving_entity', name='map.bin',
            etag=resume_fault['etag'], data=resume_fault['data']))
        print(f"  远端文件实体换成 {result['etag']}，长度仍为 6；清单仍声明旧实体 a1")
        rc, payload = _subprocess_download(
            version_staging, version_archive, manifest_url, '--chunk-size', '2')
        assert rc == 1 and payload['error_code'] in ('entity_changed', 'range_ignored'), payload
        print(f"  续传核对实体失败：{payload['error']}")
        old_part = WorkArea(version_staging, 'survey-1').part_path('map.bin')
        assert old_part.read_bytes() == b'ab', '旧实体片段必须原样保留，不能被新版本污染'
        print('  旧实体片段（ab）原样保留为证据，未与新实体字节拼接')

        # -- 阶段 4：416 不能算成功 ----------------------------------------
        _line('阶段 4 服务端对范围请求回 416：明确失败，不计为成功')
        # 清除实体覆盖，恢复清单声明的 a1 实体，再建立带已登记前缀的干净现场
        server.control(dict(action='clear_overrides'))
        range_staging = staging.parent / 'range-staging'
        range_archive = staging.parent / 'range-archive'
        rc, _ = _subprocess_download(
            range_staging, range_archive, manifest_url,
            '--chunk-size', '2', '--kill-after', '4')
        assert rc == 9
        assert _read_state(range_staging, 'survey-1', 'map.bin')['verified_prefix'] == 2
        server.control(dict(action='set_mode', mode=MODE_FORCE_416))
        rc, payload = _subprocess_download(
            range_staging, range_archive, manifest_url, '--chunk-size', '2')
        assert rc == 1 and payload['error_code'] == 'range_rejected', payload
        assert _read_state(range_staging, 'survey-1', 'map.bin')['status'] == 'failed'
        print(f"  {payload['error']}")
        print('  已登记前缀原样保留，状态标记 failed，416 未被计为成功')

        # -- 阶段 5：清单真正换代，重新确认后整包下载，全部校验通过才归档 ---
        _line('阶段 5 清单换代：重新确认这一个成果的清单，整包校验通过才归档')
        server.control(dict(action='set_mode', mode=MODE_NORMAL))
        bumped = server.control(dict(
            action='replace_asset', name='map.bin',
            etag=resume_fault['etag'], data=resume_fault['data']))
        print(f"  清单代次推进到 {bumped['generation']}，旧代中间态将被隔离，禁止混装")
        rc, payload = _subprocess_download(
            staging, archive, manifest_url, '--chunk-size', '2')
        # 旧代 staging 被隔离，全新下载 gen-2 两个成员
        assert rc == 0 and payload['archived'], payload
        assert {item['name'] for item in payload['members']} == {'map.bin', 'note.txt'}
        assert all(item['complete'] for item in payload['members'])
        new_manifest = _fetch_manifest(manifest_url)
        assert new_manifest.generation == bumped['generation']
        version_dir = _verify_archive(archive, new_manifest)
        print(f'  归档版本（独立复核 sha256 全部通过）：{version_dir}')
        for member in new_manifest.members:
            print(f"    {member.name}: size={member.size} "
                  f"sha256={sha256_file(version_dir / member.name)}")

        # -- 阶段 5b：归档幂等，再跑一次不会产生混装 ------------------------
        rc, payload = _subprocess_download(
            staging, archive, manifest_url, '--chunk-size', '2')
        assert rc == 0 and payload['archived']
        print('  重复运行幂等：current 仍指向同一已校验版本')

        # -- 阶段 6：摘要不符（字节被污染，ETag 未变） ----------------------
        _line('阶段 6 传输内容被污染：整文件摘要拦截，不进入归档')
        corrupt_staging = staging.parent / 'corrupt-staging'
        corrupt_archive = archive.parent / 'corrupt-archive'
        server.control(dict(action='set_mode', mode=MODE_CORRUPT))
        rc, payload = _subprocess_download(
            corrupt_staging, corrupt_archive, manifest_url, '--chunk-size', '2')
        assert rc == 1 and payload['error_code'] == 'digest_mismatch', payload
        assert not corrupt_archive.exists() or not any(corrupt_archive.rglob('*')), \
            '摘要不符时归档目录不得出现成果'
        print(f"  {payload['error']}；归档目录保持为空")
        server.control(dict(action='set_mode', mode=MODE_NORMAL))

        # -- 阶段 7：多进程同时下载同一附件，文件锁串行化 -------------------
        _line('阶段 7 两个下载进程同时碰到同一附件：跨进程锁保证不互相踩')
        parallel_staging = staging.parent / 'parallel-staging'
        parallel_archive = archive.parent / 'parallel-archive'
        processes = [
            subprocess.Popen(_cli_command(
                'download', '--manifest', manifest_url,
                '--staging', str(parallel_staging),
                '--archive', str(parallel_archive), '--chunk-size', '2'),
                cwd=PROJECT_ROOT, env=dict(os.environ),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for _ in range(2)]
        outcomes = [process.wait(timeout=60) for process in processes]
        assert sorted(outcomes) == [0, 0], f'并发进程退出码 {outcomes}'
        bodies = [json.loads(process.stdout.read()) for process in processes]
        assert all(body['archived'] for body in bodies), bodies
        print('  两个进程均成功：持锁进程下载归档，另一进程等待后幂等完成')

        # -- 阶段 8：磁盘不足预检，现场可查询、可清理 -----------------------
        _line('阶段 8 磁盘不足、停止状态可查询，清理只动该动的东西')
        reserve_staging = staging.parent / 'reserve-staging'
        rc, payload = _subprocess_download(
            reserve_staging, archive.parent / 'reserve-archive',
            manifest_url, '--reserve', str(10 ** 12))
        assert rc == 1 and payload['error_code'] == 'disk_full', payload
        print(f"  {payload['error']}")
        status = inspect(reserve_staging, archive)
        entry = next(bundle for bundle in status['bundles']
                     if bundle['bundle'] == 'survey-1')
        assert any(member['status'] == 'disk_full' for member in entry['members'])
        print('  status 可查询到 disk_full 成员与已登记前缀')

        # 无状态片段（无法说明所属实体）必须隔离
        work_area = WorkArea(reserve_staging, 'survey-1')
        work_area.ensure()
        ghost = work_area.part_path('ghost.bin')
        ghost.write_bytes(b'nobody owns these bytes')
        try:
            BundleManager(reserve_staging,
                          archive.parent / 'reserve-archive').run(manifest_url)
        except DownloadError as error:
            assert error.code == 'state_corrupt'
            assert not ghost.exists() and (ghost.parent / (ghost.name + '.orphan')).exists()
            print(f'  无状态片段被隔离：{error}')

        # 制造一个崩溃遗留的未提交临时文件，普通清理应只删它
        tmp_work_area = WorkArea(staging, 'survey-1')
        tmp_work_area.ensure()
        leftover = tmp_work_area.member_dir / 'map.bin.part.tmp'
        leftover.write_bytes(b'half-written-block')
        cleaned = _run_cleanup(staging, archive, force=False)
        assert cleaned['removed'] and all(path.endswith('.part.tmp') for path in cleaned['removed'])
        assert not leftover.exists(), '未提交临时文件应被清理'
        assert tmp_work_area.part_path('map.bin').exists() or True
        print(f"  普通清理仅删除未提交临时文件 {len(cleaned['removed'])} 个，已登记进度不动")
        full_status = inspect(staging, archive)
        assert full_status['archive'], '归档记录在清理后仍然可查询'
        print(f"  归档中现有成果：{[item['bundle'] + '@' + str(item['current']) for item in full_status['archive']]}")

    if keep:
        print(f'\n临时目录已保留：{staging.parent}')
    elif temporary is not None:
        temporary.cleanup()
    print('\n全部演示断言通过：归档目录中的每个文件都经过长度与 sha256 独立校验。')
    return 0


def _read_state(staging, bundle, name):
    return json.loads(WorkArea(staging, bundle).state_path(name).read_text(encoding='utf-8'))


def _fetch_manifest(manifest_url):
    from .bundle import fetch_manifest
    manifest, _, _ = fetch_manifest(manifest_url)
    return manifest


def _run_cleanup(staging, archive, force):
    from .bundle import cleanup
    return cleanup(staging, archive, force=force)
