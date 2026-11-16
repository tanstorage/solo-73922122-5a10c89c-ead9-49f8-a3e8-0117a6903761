# 临床试验数据平台：离线导出 + 差分隐私带噪查询

FastAPI + PostgreSQL，双实例部署。两大功能：

1. **离线导出**：选择试验号后，从**同一数据库快照**用**两条独立连接并行**导出患者与
   检验记录，打包 ZIP 下载。基于 MVCC 快照，**不锁源表**；支持即时取消、失败隔离与
   故障演示。
2. **带噪声检验分布查询**（新增）：研究数据管理员向合作方提供加噪的检验分布。
   选择试验、填写全局查询号与 ε（0.1 / 0.2 / 0.5），返回三个桶的带噪计数值；
   每患者 ε 预算上限 1.0（跨试验累计不重置），查询号全局幂等。

## 启动

```bash
docker compose up --build
```

- 实例 1：<http://localhost:8001> （导出页 `/index.html`，查询页 `/query.html`）
- 实例 2：<http://localhost:8002> （同上，页面右上角显示当前实例名）

首次启动时 `db/init.sql` 自动灌入合成数据（3 个试验 T-100/T-200/T-300，各 2 万患者，
每人 5 条检验，共 30 万条），并为全部患者预置 ε 预算账本（余额 0）。

## 带噪声检验分布查询

### 规则

- 输入：试验号 + 全局查询号 + ε（仅允许 0.1 / 0.2 / 0.5）；
- 对每名**有检验**的患者按**平均测值**分桶：负数 / 0～100（含端点） / 超过 100；
- 三个桶各自独立加入 **Laplace(0, 1/ε)** 噪声后返回；
- **此入口不返回患者明细或真实计数**（真实计数只在服务端瞬时计算，不落库、不外发）；
- 参与患者名单与统计值取自**同一已提交快照**（单条 SQL 语句的语句级快照）；
- 每患者累计 ε 上限 **1.0**，换试验**不重置**；任一参与者余额不足则**整次拒绝**，
  不扣任何人、不指明患者身份；
- 查询号全局唯一：**同号同参数**重试返回首个已存结果（不重复扣费、不重新加噪，
  响应丢失 / 实例重启 / 源数据变化后亦然）；**同号异参数**返回 409。

### 一致性设计（app/dpquery.py）

一次查询 = 一个事务，顺序如下：

1. （可选演示）`pg_sleep` 模拟慢统计 —— 此时尚未持有任何锁；
2. **一条 SQL** 同时算出三个桶的真实计数与参与者名单（同一已提交快照），不取任何锁
   —— 因此慢统计期间源端更正与其他查询都不被阻塞；
3. `SELECT ... FOR UPDATE` 锁定本次参与者的账本行（行级锁、按 patient_id 排序防死锁），
   在锁内逐人核对余额；任一不足 → 同事务写入「拒绝」记录并提交，**不扣任何人**；
   不相交人群锁定不相交的行，互不等待；
4. 余额充足 → 条件 UPSERT 扣费（`WHERE spent + ε ≤ 1.0`，行数不符即整体回滚），
   叠加 `CHECK (spent ≤ 1.0)` 约束 —— 双实例并发查询重叠人群也**不会超支**；
5. 三个桶加噪后与扣费在**同一事务**提交：落库失败两者均不留；
6. 并发同号由 `queries` 主键裁决：落败方事务整体回滚（扣费不留），改返回已存结果。

账本与结果全部存于 PostgreSQL，应用实例无状态 —— 重启后幂等语义不变。

### API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/queries` | 提交查询 `{query_id, trial_id, epsilon, slow_seconds?}` → 200 `{status, buckets?}` / 409 异参数 / 422 非法 ε / 404 未知试验 / 503 锁超时 |
| GET | `/api/queries` | 最近 20 条查询（仅参数与带噪结果） |
| GET | `/api/queries/{query_id}` | 按查询号取回已存结果（响应丢失后恢复用） |
| GET | `/api/instance` | 当前实例名（双实例演示用） |

`status=completed` 时 `buckets` 含 `negative / zero_to_100 / over_100` 三个带噪值；
`status=rejected` 时只有通用原因 `insufficient_budget`，不含任何患者信息。
响应中 `replayed=true` 表示返回的是首个已存结果（本次未扣费、未加噪）。

## 离线导出（原有功能）

### 同一快照 + 独立连接并行读取（exporter.py）

1. **keeper 连接**：`BEGIN ISOLATION LEVEL REPEATABLE READ` 后
   `SELECT pg_export_snapshot()` 导出快照，该事务在整个导出期间保持打开；
2. **两条 reader 连接**：各自 `BEGIN ISOLATION LEVEL REPEATABLE READ` 并
   `SET TRANSACTION SNAPSHOT '<快照号>'`，然后并行读取 patients / labs；
3. 快照基于 MVCC，不取表锁，源端写入**不被阻塞**，导出也看不到导出开始后的更正；
4. 两份 CSV 都完整写出后才打包 ZIP，状态置 `ready` 才开放下载。

### 状态机与取消

```
running ─┬─► ready       （两份 CSV 完整 + ZIP 打包成功）
         ├─► failed      （任一读取失败，如读连接断开）
         └─► cancelled   （用户取消 / 被新批次顶替）
```

- 状态迁移**只允许从 `running` 出发**；每批状态与产物目录完全独立；
- **取消即时受理**：HTTP 层同步把状态翻为 `cancelled` 后立即返回，随后后台
  `pg_cancel_backend` 终止服务端慢查询，并兜底断开仍存活的客户端连接；
- 取消/失败的批次删除全部产物，下载接口返回 409；只有 `ready` 可下载；
- 开始新批次会自动取消仍在运行的旧批次。

### 演示开关

| 功能 | 入口 | 说明 |
|---|---|---|
| 慢读 | 开始导出时设「模拟慢查询秒数」 | 读事务内先 `pg_sleep(n)`，演示取消不等慢查询 |
| 跨表事务更正 | 「启动更正写入」 | 每 0.2s 在**单事务**内更正随机患者的试验归属+全部检验值 |
| 读连接断开 | 勾选「故障注入」 | N 秒后断开 labs 读连接 → 批次 failed |
| 连接观察 | 页面第 4 区 / `GET /api/connections` | 实时查看 export-*/mutator 后端连接与事务年龄 |
| 慢统计 | 查询页「模拟慢统计秒数」 | 统计前 `pg_sleep(n)`，演示慢统计不持锁、不阻塞他人 |

### 导出 API 摘要

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
app/main.py      FastAPI 路由（导出 + 带噪查询）
app/exporter.py  批次管理：快照导出、并行读取、取消、状态机
app/dpquery.py   带噪查询：直方图、ε 预算账本、幂等、Laplace 加噪
app/mutator.py   源端跨表事务更正模拟
app/static/      浏览器页面（index.html 导出 / query.html 带噪查询）
db/init.sql      建表 + 合成数据 + ε 预算账本 + 查询结果表
docker-compose.yml / app/Dockerfile   （db + 双实例 app1/app2）
```
