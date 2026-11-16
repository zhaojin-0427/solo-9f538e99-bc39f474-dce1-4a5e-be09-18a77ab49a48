# 潮位补采台（tide-collector）

海洋站数据员补采单日潮位 CSV 的小系统：网页填写源 URL，一键启动/继续采集；
断网或进程重启后从已确认字节续传，整份到齐且通过 CSV 校验才替换网页中的
可用观测，失败时仍提供上一份完整数据。

## 组成

| 目录 | 说明 |
|---|---|
| `app/` | Flask + SQLite 采集网页（默认 8080 端口） |
| `source/` | 本地示例潮位源（默认 9000 端口），强 ETag + Range，可切换版本/断流/忽略 Range/错误 206 |
| `tests/` | 单元测试与端到端自测 |
| `docker-compose.yml` | 一键拉起 app + source |

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 采集网页：http://localhost:8080 （表单已预填示例源 `http://source:9000/tide.csv`）
- 示例源控制台：http://localhost:9000 （切换版本、断流字节数、忽略 Range、错误 206）

采集数据（SQLite 断点 + .part 文件）持久化在命名卷 `appdata`，
`docker compose restart app` 后任务可从断点继续。

## 本地运行（无 Docker）

```bash
pip install -r app/requirements.txt -r source/requirements.txt
python3 source/source_app.py &                 # 9000 端口
APP_PORT=8080 SAMPLE_SOURCE_URL=http://127.0.0.1:9000/tide.csv python3 app/app.py
```

## 自测

```bash
python3 tests/unit_test.py   # CSV 校验 / Content-Range 解析
python3 tests/e2e_test.py    # 拉起 app+source，跑 8 组端到端场景
```

## 采集与续传规则（collector.py 的不变式）

1. `.part` 文件只追加；每写一块执行 `write → fsync → 提交 SQLite 断点`，
   断点（`confirmed_bytes`）与磁盘字节共同推进。
2. 进程启动时恢复：`confirmed = min(磁盘字节, 断点)`，文件截到 `confirmed`，
   断点同步改回该值 —— 二者一起恢复，不跳字节；`.part` 丢失则整体清零重收。
3. 同版续传：请求带 `Range: bytes=N-` 与 `If-Range: <etag>`，不重取前缀。
4. 换版（HEAD 预检 ETag 变化、206 上 ETag 不符、416）或续传返回 200
   （源忽略 Range）→ 舍弃旧前缀整体重收，绝不混合两份文件。
5. 206 必须核对：`Content-Range` 起点 == 断点、总长与已知一致、
   `Content-Length` 与区间长度一致；任何异常响应不写盘、不推进断点。
6. 整份到齐（`confirmed == total`）且通过 CSV 校验才在一个事务里
   `INSERT OR REPLACE` 发布观测；校验失败或下载中断时旧观测保持可读。
7. 连接提前断开按“应得字节 vs 实得字节”核对（读取前关闭 urllib3 的
   `enforce_content_length`，自己判定短读），已写字节安全保留，任务置为
   `interrupted` 等待继续。

## CSV 校验规则（csvvalid.py）

- 两列：UTC 时刻（ISO 8601，须带时区，`Z` 或偏移均可，归一到 UTC）+ 潮位；
- 潮位须为有限数值（拒绝 NaN/inf/非数值）；
- 时刻严格递增（等值或倒退均拒绝）；
- 首行可为表头（自动识别，无表头时首行不丢）。

## 示例源开关（source/，控制台在 http://localhost:9000）

- **版本 1 / 2**：两份不同但合法的潮位 CSV（144 / 150 行），ETag 随内容变；
- **版本 3**：损坏版（NaN、时刻倒退），用于演示校验失败不替换观测；
- **断流**：每次响应送出 N 字节后提前关闭连接（Content-Length 未满足）；
- **忽略 Range**：对续传请求一律回 200 全量；
- **错误 206**：`Content-Range` 起点故意错位，用于演示客户端核对；
- `/stats` 返回每次请求的方法/Range/状态/实际送出字节，供核对续传行为。

## 已知限制

- 源必须提供强 ETag 与 Content-Length 才能安全续传；无 Content-Length 的
  200 响应按“干净读完即完整”处理（提前断开无法识别），示例源始终带长度。
- 会话内不自动重试：断流后任务置 `interrupted`，需再次点击“启动/继续”
  （符合补采场景人工确认的习惯）。
- 使用 Flask 开发服务器（threaded），面向单站 QC 演示；生产部署应换
  WSGI 服务器并加访问控制。
- 每块写盘都 fsync，吞吐优先的场景可调大块大小（`collector.CHUNK`）。
