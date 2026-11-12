# 调查附件·校验断点下载

面向合作资料接收与归档人员的 Python 采集组件。下载层保证：**进入归档目录的每个文件，
其长度与 sha256 摘要都经过独立校验**；下载中间态另目录存放，崩溃、断连、远端换版都
可查询、可恢复、可安全清理。输入样本均为本地合成数据，回环域名只表示来源身份，不联网。

## 安装与运行

使用 Python 3.11，仅依赖标准库。从项目根目录执行：

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
```

`demo` 启动真实的回环 HTTP 来源，用子进程重演八类交接事故，每次使用独立临时目录并在
结束后清理；归档完成后会对归档文件**重新计算 sha256**，而不是打印“下载成功”即结束。

## 下载层命令

```bash
# 按成果清单下载（主文件 + 附属文件），整包全部校验通过才进入归档
python3 -m collector download --manifest http://127.0.0.1:8080/manifest.json \
    --staging var/staging --archive var/archive

# 查询每个成员的实体标识、已校验前缀、磁盘片段与失败原因
python3 -m collector status --staging var/staging --archive var/archive

# 普通清理：只删崩溃遗留的未提交临时文件（*.part.tmp），已登记进度不动
python3 -m collector cleanup --staging var/staging --archive var/archive
# 强制清理指定成果的整个工作区；归档目录永远不会被删除
python3 -m collector cleanup --bundle survey-1 --force --staging var/staging --archive var/archive

# 单独启动本地合成资源服务器，供手工演练范围忽略、416、换版与字节污染
python3 -m collector serve-demo
```

路径相对于运行命令的工作目录解析；`var/` 已在 `.gitignore` 中忽略，不会进入版本库。

## 协议保证（对应交接事故）

- 每个请求都带 `Range` 与 `If-Range`，并核对响应实体标识（ETag）、`Content-Range` 与长度。
- 服务端忽略 `Range` 回 200 时，**绝不把全量响应接到旧片段后**：有已校验前缀即拒绝并
  保留现场；无前缀才清空重下。
- 416 视为范围不被接受，明确失败并保留前缀，**不计为成功**。
- 崩溃留下“已写但未登记”的尾巴时，续传先用远端字节逐块比对：一致才纳入已校验前缀，
  首个不一致处截断重下。没有状态文件的片段无法证明所属实体，一律隔离为 `.orphan`。
- 远端实体（ETag）变化在续传时即被识别；清单整体换代时重新确认这一个成果的清单，
  旧代中间态移入 `staging/_quarantine`，不同代附件不会混装。
- 磁盘不足（写入 ENOSPC 或预检不达标）标记 `disk_full`，释放空间后可直接续传。
- 多个下载进程碰到同一附件时，由成果级文件锁串行化；后到者等待并幂等完成。

## 磁盘布局

```
var/staging/<bundle>/
  lock                       成果级跨进程文件锁
  manifest.gen-<n>.json      本代清单快照
  members/<name>.state.json  实体标识 + 已校验前缀（fsync 后才推进）
  members/<name>.part        片段数据
  members/<name>.part.tmp    崩溃遗留的未提交临时写入（普通清理删除）
var/staging/_quarantine/     旧代或无主片段的隔离区
var/archive/<bundle>/
  gen-<n>/                   仅当整包所有成员校验通过才出现
  current -> gen-<n>/        原子切换的当前版本指针
```

成员状态取值：`downloading / paused / verified / failed / disk_full`。
`verified_prefix` 之前的每个字节都能追溯到状态文件登记的实体（ETag）。

## 原有本地凭据功能（保留兼容）

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

`collector/domain.py` 负责来源格式与领域校验；`storage.py` 在单事务内保存原始对象、
导入凭据与当前记录；旧的 `transport.replay_server` 脚本回放工具保持不变。

## 可复现运行

宿主机或容器均不需要外部网络。仓库内不含真实账号或人员资料；不要把生产凭据写入样本或日志。
