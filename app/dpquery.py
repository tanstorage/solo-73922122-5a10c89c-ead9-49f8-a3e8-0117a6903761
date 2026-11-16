"""带噪声检验分布查询：Laplace 机制 + 每患者 ε 预算账本 + 全局查询号幂等。

设计要点（对应需求）：

- 直方图与参与者名单取自**同一条 SQL 语句**：READ COMMITTED 下单条语句
  看到同一个已提交快照，因此「参与患者与统计取自同一已提交快照」；
- 直方图计算期间**不持有任何锁**：源端更正（patients/labs 更新）与
  不相交人群查询都无需等待慢统计结束；锁只在扣费阶段短暂持有，
  且只锁定本次参与者的 privacy_ledger 行（行级锁，不相交即不冲突）；
- 每患者累计 ε 上限 1.0，跨试验不重置：扣费前在锁内逐人核对余额，
  任一参与者不足则整次拒绝（不扣任何人、不返回身份）；条件 UPSERT +
  CHECK 约束是并发下的最终防线，双实例重叠人群也不会超支；
- 扣费与带噪结果在**同一事务**提交：落库失败两者均不留；
- 全局查询号幂等：queries 表主键即查询号。同号同参数返回首个已存结果
  （不重复扣费、不重新加噪，重启/响应丢失/源数据变化后亦然）；
  同号异参数拒绝；并发同号由主键唯一约束裁决，落败方回滚后返回已存结果。
"""

from __future__ import annotations

import math
import random
from decimal import Decimal

import asyncpg

EPSILON_CHOICES = (Decimal("0.1"), Decimal("0.2"), Decimal("0.5"))
BUDGET_CAP = Decimal("1.0")

REASON_INSUFFICIENT_BUDGET = "insufficient_budget"

# 一条语句同时产出三个桶的真实计数与参与者名单（同一已提交快照）。
# 只统计「有检验」的患者：按平均测值归入 负数 / 0~100（含端点） / 超过100。
HISTOGRAM_SQL = """
WITH per_patient AS (
    SELECT p.patient_id, AVG(l.value) AS avg_value
    FROM patients p
    JOIN labs l ON l.patient_id = p.patient_id
    WHERE p.trial_id = $1
    GROUP BY p.patient_id
)
SELECT count(*) FILTER (WHERE avg_value < 0)                       AS bucket_negative,
       count(*) FILTER (WHERE avg_value >= 0 AND avg_value <= 100) AS bucket_zero_to_100,
       count(*) FILTER (WHERE avg_value > 100)                     AS bucket_over_100,
       array_agg(patient_id ORDER BY patient_id)                   AS participant_ids
FROM per_patient
"""

# 锁定本次参与者的账本行（行级锁；ORDER BY 保证加锁顺序一致，避免死锁）。
# READ COMMITTED 下 FOR UPDATE 会等待并发扣费提交后读到最新余额。
LOCK_SQL = """
SELECT patient_id, spent
FROM privacy_ledger
WHERE patient_id = ANY($1::bigint[])
ORDER BY patient_id
FOR UPDATE
"""

# 条件扣费：余额够才扣；RETURNING 行数必须等于参与者数，否则整体回滚。
# 与 CHECK(spent <= 1.0) 一起保证任何并发交错下都不会超支。
DEDUCT_SQL = """
INSERT INTO privacy_ledger (patient_id, spent)
SELECT pid, $2::numeric FROM unnest($1::bigint[]) AS pid
ON CONFLICT (patient_id) DO UPDATE
SET spent = privacy_ledger.spent + $2::numeric,
    updated_at = now()
WHERE privacy_ledger.spent + $2::numeric <= 1.0
RETURNING patient_id
"""

INSERT_QUERY_SQL = """
INSERT INTO queries (query_id, trial_id, epsilon, status,
                     bucket_negative, bucket_zero_to_100, bucket_over_100,
                     reason)
VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8)
"""

SELECT_QUERY_SQL = "SELECT * FROM queries WHERE query_id = $1"

_rng = random.SystemRandom()


def laplace(scale: float) -> float:
    """Laplace(0, scale) 采样（逆变换法，SystemRandom 熵源）。"""
    u = 0.0
    while u == 0.0:  # 排除 log(0)
        u = _rng.random()
    u -= 0.5
    return -scale * math.copysign(1.0, u) * math.log1p(-2.0 * abs(u))


class ParamMismatch(Exception):
    """查询号已存在但参数（试验号或 ε）不同。"""


class _BudgetRace(Exception):
    """极端并发下条件扣费未覆盖全部参与者（防御路径，正常不会触发）。"""


# 扣费阶段锁等待上限：正常情况下临界区只有毫秒级，10s 绰绰有余；
# 超时即整体回滚（查询号不消耗），由上层转成 503，避免病态锁等待拖死请求。
LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '10s'"


def outcome_from_row(row: asyncpg.Record, trial_id: str, epsilon: Decimal) -> dict:
    """把已存查询记录转成响应；同号异参数抛 ParamMismatch。"""
    if row["trial_id"] != trial_id or row["epsilon"] != epsilon:
        raise ParamMismatch(
            f"query_id {row['query_id']!r} 已被使用，参数为 "
            f"trial_id={row['trial_id']!r}, epsilon={row['epsilon']}"
        )
    outcome = {
        "query_id": row["query_id"],
        "trial_id": row["trial_id"],
        "epsilon": float(row["epsilon"]),
        "status": row["status"],
        "reason": row["reason"],
        "buckets": None,
        "created_at": row["created_at"].isoformat(),
    }
    if row["status"] == "completed":
        outcome["buckets"] = {
            "negative": row["bucket_negative"],
            "zero_to_100": row["bucket_zero_to_100"],
            "over_100": row["bucket_over_100"],
        }
    return outcome


async def _insert_outcome(
    conn: asyncpg.Connection,
    query_id: str,
    trial_id: str,
    epsilon: Decimal,
    status: str,
    buckets: dict | None,
    reason: str | None,
) -> None:
    await conn.execute(
        INSERT_QUERY_SQL,
        query_id,
        trial_id,
        epsilon,
        status,
        buckets["negative"] if buckets else None,
        buckets["zero_to_100"] if buckets else None,
        buckets["over_100"] if buckets else None,
        reason,
    )


async def _attempt(
    conn: asyncpg.Connection,
    query_id: str,
    trial_id: str,
    epsilon: Decimal,
    slow_seconds: float,
) -> dict:
    """在单个事务内完成：慢统计（无锁）→ 锁账核对 → 扣费 + 落库带噪结果。"""
    async with conn.transaction():
        await conn.execute(LOCK_TIMEOUT_SQL)
        if slow_seconds > 0:
            # 模拟慢统计：此时尚未持有任何锁，不阻塞源端更正与其他查询
            await conn.fetchval("SELECT pg_sleep($1)", slow_seconds)

        # 1) 同一已提交快照：三个桶的真实计数 + 参与者名单
        hist = await conn.fetchrow(HISTOGRAM_SQL, trial_id)
        ids: list[int] = list(hist["participant_ids"] or [])
        true_counts = (
            hist["bucket_negative"],
            hist["bucket_zero_to_100"],
            hist["bucket_over_100"],
        )

        # 2) 锁定参与者账本行并逐人核对余额（锁内检查，并发安全）
        if ids:
            rows = await conn.fetch(LOCK_SQL, ids)
            spent = {r["patient_id"]: r["spent"] for r in rows}
            insufficient = any(
                spent.get(pid, Decimal(0)) + epsilon > BUDGET_CAP for pid in ids
            )
        else:
            insufficient = False

        if insufficient:
            # 任一参与者余额不足：整次拒绝，不扣任何人，不记录身份
            await _insert_outcome(
                conn, query_id, trial_id, epsilon,
                "rejected", None, REASON_INSUFFICIENT_BUDGET,
            )
            return {
                "query_id": query_id,
                "trial_id": trial_id,
                "epsilon": float(epsilon),
                "status": "rejected",
                "reason": REASON_INSUFFICIENT_BUDGET,
                "buckets": None,
            }

        # 3) 扣费（条件 UPSERT；行数不符说明极端竞争，整体回滚改记拒绝）
        if ids:
            charged = await conn.fetch(DEDUCT_SQL, ids, epsilon)
            if len(charged) != len(ids):
                raise _BudgetRace()

        # 4) 三个桶各自独立加 Laplace(0, 1/ε) 噪声，与扣费同事务落库
        scale = 1.0 / float(epsilon)
        buckets = {
            "negative": true_counts[0] + laplace(scale),
            "zero_to_100": true_counts[1] + laplace(scale),
            "over_100": true_counts[2] + laplace(scale),
        }
        await _insert_outcome(
            conn, query_id, trial_id, epsilon, "completed", buckets, None
        )
        return {
            "query_id": query_id,
            "trial_id": trial_id,
            "epsilon": float(epsilon),
            "status": "completed",
            "reason": None,
            "buckets": buckets,
        }


async def execute(
    pool: asyncpg.Pool,
    query_id: str,
    trial_id: str,
    epsilon: Decimal,
    slow_seconds: float = 0.0,
) -> dict:
    """执行带噪查询；并发同号落败或防御性回滚时返回首个已存结果。

    返回的 dict 含 replayed 标记：False=本次新执行，True=返回已存结果。
    """
    async with pool.acquire() as conn:
        try:
            outcome = await _attempt(conn, query_id, trial_id, epsilon, slow_seconds)
            outcome["replayed"] = False
            return outcome
        except asyncpg.UniqueViolationError:
            # 同号并发：对方已提交，本事务（含扣费）整体回滚，回落到已存结果
            pass
        except _BudgetRace:
            # 防御路径：扣费未完整生效已回滚，改为记录「预算不足」拒绝
            try:
                async with conn.transaction():
                    await _insert_outcome(
                        conn, query_id, trial_id, epsilon,
                        "rejected", None, REASON_INSUFFICIENT_BUDGET,
                    )
            except asyncpg.UniqueViolationError:
                pass
        row = await conn.fetchrow(SELECT_QUERY_SQL, query_id)
        if row is None:  # 理论上不可达：唯一冲突意味着对方已提交
            raise RuntimeError("query outcome lost after concurrency conflict")
        outcome = outcome_from_row(row, trial_id, epsilon)
        outcome["replayed"] = True
        return outcome
