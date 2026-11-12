"""命令行入口：download / status / cleanup / serve-demo / demo。"""

import argparse
import json
import sys

from .bundle import BundleManager, cleanup, inspect
from .downloader import DownloadHooks, KillProcessHooks, MemberDownloader
from .fixtureserver import (
    MODE_CORRUPT,
    MODE_FORCE_416,
    MODE_IGNORE_RANGE,
    MODE_NORMAL,
    load_seed,
    serve_fixtures,
)
from .model import DownloadError


def _print(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _download_hooks(arguments):
    if arguments.kill_after is not None:
        return KillProcessHooks(arguments.kill_after,
                                skip_below=arguments.kill_skip)
    return DownloadHooks()


def command_download(arguments):
    factory = None
    if arguments.kill_after is not None or arguments.chunk_size or arguments.reserve:
        def factory(work_area):
            return MemberDownloader(
                work_area, chunk_size=arguments.chunk_size or 64 * 1024,
                reserve_free_bytes=arguments.reserve,
                hooks=_download_hooks(arguments))
    manager = BundleManager(arguments.staging, arguments.archive,
                            downloader_factory=factory)
    report = manager.run(arguments.manifest, wait_lock=not arguments.no_wait)
    _print(_report_dict(report))
    if report.error_code:
        return 1
    return 0 if report.archived else 2


def _report_dict(report):
    return dict(
        bundle=report.bundle, generation=report.generation,
        manifest_etag=report.manifest_etag, archived=report.archived,
        archive_path=report.archive_path,
        error_code=report.error_code, error=report.error,
        members=[dict(name=item.name, status=item.status,
                      verified_prefix=item.verified_prefix, size=item.size,
                      bytes_written=item.bytes_written, etag=item.etag,
                      last_error=item.last_error, complete=item.complete)
                 for item in report.members])


def command_status(arguments):
    _print(inspect(arguments.staging, arguments.archive))
    return 0


def command_cleanup(arguments):
    _print(cleanup(arguments.staging, arguments.archive,
                   bundle=arguments.bundle, force=arguments.force))
    return 0


def command_serve_demo(arguments):
    assets = load_seed(arguments.seed)
    with serve_fixtures(assets, bundle=arguments.bundle) as handle:
        print(f'演示资源服务：{handle.base}', flush=True)
        print(f'清单地址：{handle.manifest_url()}', flush=True)
        print(f'切换模式：ignore_range={MODE_IGNORE_RANGE}, '
              f'force_416={MODE_FORCE_416}, corrupt={MODE_CORRUPT}, '
              f'normal={MODE_NORMAL}', flush=True)
        print('按 Ctrl-C 停止', flush=True)
        try:
            import time
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog='collector', description='调查附件校验断点下载层')
    sub = parser.add_subparsers(dest='command', required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--staging', default='var/staging',
                        help='下载中间态目录（默认 var/staging）')
    common.add_argument('--archive', default='var/archive',
                        help='归档目录，只放整包校验通过的版本（默认 var/archive）')

    download = sub.add_parser('download', parents=[common],
                              help='按清单下载一个成果并发布到归档')
    download.add_argument('--manifest', required=True, help='成果清单 URL')
    download.add_argument('--no-wait', action='store_true',
                          help='成果被其它进程锁定时直接失败，不等待')
    download.add_argument('--chunk-size', type=int, default=0,
                          help='覆盖每块字节数（演示/测试用）')
    download.add_argument('--reserve', type=int, default=0,
                          help='写入前要求的额外预留空间（字节）')
    download.add_argument('--kill-after', type=int, default=None,
                          help='演示用：写入指定字节后模拟进程崩溃')
    download.add_argument('--kill-skip', type=int, default=0,
                          help='演示用：该偏移及之前不触发崩溃（跳过尾巴比对）')
    download.set_defaults(func=command_download)

    status = sub.add_parser('status', parents=[common], help='查询下载与归档状态')
    status.set_defaults(func=command_status)

    clean = sub.add_parser('cleanup', parents=[common], help='安全清理中间态')
    clean.add_argument('--bundle', help='只清理指定成果；缺省处理全部成果')
    clean.add_argument('--force', action='store_true',
                       help='删除整个工作区（归档目录永不删除）；缺省只清未提交临时文件')
    clean.set_defaults(func=command_cleanup)

    serve = sub.add_parser('serve-demo', help='启动本地合成资源响应服务器')
    serve.add_argument('--seed', default='fixtures/sample.json')
    serve.add_argument('--bundle', default='survey-1')
    serve.add_argument('--port', type=int, default=0,
                       help='保留参数；当前使用系统分配的回环端口')
    serve.set_defaults(func=command_serve_demo)

    demo = sub.add_parser('demo', help='端到端故障演示（回环 HTTP + 子进程）')
    demo.add_argument('--staging', default=None)
    demo.add_argument('--archive', default=None)
    demo.add_argument('--keep', action='store_true', help='保留临时目录便于人工检查')
    demo.set_defaults(func=lambda args: run_demo(args))
    return parser


def main(argv=None):
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        return arguments.func(arguments)
    except DownloadError as error:
        _print(dict(ok=False, error_code=getattr(error, 'code', 'download_error'),
                    error=str(error)))
        return 1


# 演示编排放在独立模块，避免 CLI 导入循环
def run_demo(arguments):
    from .demo import run
    return run(keep=arguments.keep,
               staging=arguments.staging, archive=arguments.archive)


if __name__ == '__main__':
    sys.exit(main())
