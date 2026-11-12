# 本地运行与归档操作说明

先运行单元测试，再执行演示命令。演示从干净临时目录发起真实回环请求并处理响应，
不需要外部站点；归档发布前会对 staging 片段和归档副本各做一次独立 sha256 复核。

## 日常操作

1. `download`：按成果清单下载主文件与附属文件。整包所有成员通过长度与摘要校验后，
   才会复制到 `archive/<bundle>/gen-<n>/` 并原子切换 `current` 指针；任一成员失败，
   归档目录保持原样。
2. `status`：只读检查，不补采、不删除。输出每个成员的状态、ETag、已校验前缀、
   磁盘片段长度与最近错误，以及隔离区和归档版本。
3. `cleanup`：
   - 不带 `--force`：仅删除 `*.part.tmp` 等崩溃遗留的未提交临时文件，已登记进度不动；
   - 带 `--force`：删除指定成果（或全部）的整个 staging 工作区；
   - 任何情况下都不会删除 `archive/` 下的已归档版本。

## 状态处置手册

- `paused`：收到停止信号（如 Ctrl-C），现场完整，重新执行同一 `download` 即可续传。
- `disk_full`：写入前预检不达标或写入返回 ENOSPC。释放空间后重新执行 `download`，
  已校验前缀继续生效。
- `failed / range_ignored`：服务端忽略 Range 回 200 且本地已有前缀。程序拒绝拼接并
  保留现场；确认服务端恢复范围支持后重试，必要时用 `cleanup --force` 重下该成果。
- `failed / range_rejected`：服务端回 416。这不是成功，需先核对远端实体与范围，
  再决定续传或重下。
- `failed / entity_changed` 或 `manifest_changed`：远端换了实体或清单换代。旧代片段已
  隔离在 `staging/_quarantine`，用新清单重新下载，禁止把两代附件混装。
- `failed / digest_mismatch`：整文件长度或 sha256 与清单不符，文件不会进入归档。
  排查传输污染后重下；现场保留用于追责。
- `state_corrupt`：状态文件损坏，或出现无状态片段（已隔离为 `.orphan`）。无主片段无法
  证明所属实体，不得复用，应重下。

## 本地响应服务器（演练用）

`serve-demo` 与 `demo` 使用的回环服务器实现真实 Range/206 语义，并通过 `POST /__control__`
在运行中切换：`ignore_range`（忽略 Range 回 200）、`force_416`、`corrupt`（污染首字节、
ETag 不变以触发摘要不符）、`override_serving_entity`（清单不变、实体换 ETag）、
`replace_asset`（同尺寸换内容并推进清单代次）。每条请求的 Range 与 If-Range 都有记录，
可据此核对调用顺序。

数据文件路径均相对项目根目录；演示子进程以 `python -I` 隔离模式运行，仅显式信任项目根，
屏蔽用户 site-packages 与环境变量中的模块注入面。
