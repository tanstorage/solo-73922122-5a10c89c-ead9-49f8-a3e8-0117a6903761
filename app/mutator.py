"""源端写入模拟器：跨表事务更正。

循环执行「单事务内同时修改患者试验归属 + 该患者全部检验值」，
用于演示：
1. 导出快照不受源端写入影响（MVCC，不锁表、不阻塞写入）；
2. 同一快照导出的两张 CSV 互相一致（检验集恰好是快照中入选患者的检验）。
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque

import asyncpg


class Mutator:
    def __init__(self, dsn: str, interval: float = 0.2):
        self.dsn = dsn
        self.interval = interval
        self.running = False
        self.corrections = 0
        self.recent: deque[dict] = deque(maxlen=50)
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self.running = True
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self.running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        conn = await asyncpg.connect(
            self.dsn, server_settings={"application_name": "mutator"}
        )
        try:
            row = await conn.fetchrow(
                "SELECT min(patient_id) AS lo, max(patient_id) AS hi FROM patients"
            )
            lo, hi = row["lo"], row["hi"]
            trials = [
                r["trial_id"]
                for r in await conn.fetch(
                    "SELECT DISTINCT trial_id FROM patients ORDER BY 1"
                )
            ]
            while True:
                pid = random.randint(lo, hi)
                async with conn.transaction():  # 跨表事务：归属 + 检验值一起提交
                    old = await conn.fetchval(
                        "SELECT trial_id FROM patients WHERE patient_id = $1 FOR UPDATE",
                        pid,
                    )
                    if old is not None:
                        new = random.choice([t for t in trials if t != old])
                        delta = round(random.uniform(-5, 5), 2)
                        await conn.execute(
                            "UPDATE patients SET trial_id = $2 WHERE patient_id = $1",
                            pid, new,
                        )
                        await conn.execute(
                            "UPDATE labs SET value = round(value + $2::numeric, 2) "
                            "WHERE patient_id = $1",
                            pid, delta,
                        )
                        self.corrections += 1
                        self.recent.append(
                            {
                                "seq": self.corrections,
                                "patient_id": pid,
                                "from": old,
                                "to": new,
                                "delta": delta,
                                "at": time.time(),
                            }
                        )
                await asyncio.sleep(self.interval)
        finally:
            await conn.close()

    def status(self) -> dict:
        alive = self._task is not None and not self._task.done()
        return {
            "running": self.running and alive,
            "corrections": self.corrections,
            "recent": list(self.recent)[-10:],
        }
