# 临床试验数据离线导出

FastAPI + PostgreSQL。选择试验号后，从**同一数据库快照**用**两条独立连接并行**
导出患者与检验记录，打包 ZIP 下载。基于 MVCC 快照，**不锁源表**；支持即时取消、
失败隔离与故障演示。

## 启动

```bash
docker compose up --build
```

浏览器打开 <http://localhost:8000> 。首次启动时 `db/init.sql` 自动灌入合成数据
（3 个试验 T-100/T-200/T-300，各 2 万患者，每人 5 条检验，共 30 万条）。

## 工作原理

### 同一快照 + 独立连接并行读取（exporter.py）

1. **keeper 连接**：`BEGIN ISOLATION LEVEL REPEATABLE READ` 后
   `SELECT pg_export_snapshot()` 导出快照，该事务在整个导出期间保持打开；
2. **两条 reader 连接**：各自 `BEGIN ISOLATION LEVEL REPEATABLE READ` 并
   `SET TRANSACTION SNAPSHOT '<快照号>'`，然后并行读取：
   - patients 连接：`SELECT patient_id, trial_id FROM patients WHERE trial_id=$1`
   - labs 连接：`labs JOIN patients`（同一快照内的入选患者及其检验）；
3. 快照基于 MVCC，不取表锁，源端写入（试验归属/检验值更正）**不被阻塞**，
   导出也看不到导出开始后的更正；
4. 两份 CSV 都完整写出后才打包 ZIP，状态置 `ready` 才开放下载。

### 状态机与取消

```
running ─┬─► ready       （两份 CSV 完整 + ZIP 打包成功）
         ├─► failed      （任一读取失败，如读连接断开）
         └─► cancelled   （用户取消 / 被新批次顶替）
```

- 状态迁移**只允许从 `running` 出发**：已被取消的批次，其迟到任务无法把它
  改写成 ready/failed，更影响不到新批次（每批状态与产物目录完全独立）；
- **取消即时受理**：HTTP 层同步把状态翻为 `cancelled` 后立即返回（不等待慢
  查询），随后后台任务 `pg_cancel_backend` 终止服务端慢查询，并兜底断开仍
  存活的客户端连接；`_run` 的 `finally` 释放本批全部 3 条连接；
- 取消/失败的批次删除全部产物，下载接口返回 409；只有 `ready` 可下载；
- 开始新批次会自动取消仍在运行的旧批次。

### 演示开关

| 功能 | 入口 | 说明 |
|---|---|---|
| 慢读 | 开始导出时设「模拟慢查询秒数」 | 读事务内先 `pg_sleep(n)`，演示取消不等慢查询 |
| 跨表事务更正 | 「启动更正写入」 | 每 0.2s 在**单事务**内更正随机患者的试验归属+全部检验值 |
| 读连接断开 | 勾选「故障注入」 | N 秒后 `pg_terminate_backend` + 客户端断开 labs 读连接 → 批次 failed |
| 连接观察 | 页面第 4 区 / `GET /api/connections` | 实时查看 export-*/mutator 后端连接与事务年龄 |

## API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/trials` | 试验列表及患者数 |
| POST | `/api/batches` | 开始批次 `{trial_id, slow_seconds, kill_reader_after}` |
| GET | `/api/batches/current` · `/api/batches/{id}` | 批次状态 |
| POST | `/api/batches/{id}/cancel` | 取消（即时受理） |
| GET | `/api/batches/{id}/download` | 下载 ZIP（仅 ready） |
| POST | `/api/mutator/start` · `/stop`，GET `/api/mutator` | 源端写入模拟 |
| GET | `/api/connections` | 导出/更正相关数据库后端 |

## 本地开发（不用 Docker）

```bash
pip install -r app/requirements.txt
# 准备 PostgreSQL 并执行 db/init.sql 后：
cd app
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/clinical \
  uvicorn main:app --port 8000
```

## 目录

```
app/main.py      FastAPI 路由
app/exporter.py  批次管理：快照导出、并行读取、取消、状态机
app/mutator.py   源端跨表事务更正模拟
app/static/      浏览器页面
db/init.sql      建表 + 合成数据
docker-compose.yml / app/Dockerfile
```
