"""FastAPI 入口：临床试验数据离线导出。

浏览器入口：GET / ；API 见 /docs。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from exporter import READY, BatchManager
from mutator import Mutator

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/clinical"
)
EXPORT_DIR = os.environ.get("EXPORT_DIR", "./exports")

manager = BatchManager(DATABASE_URL, EXPORT_DIR)
mutator = Mutator(DATABASE_URL)
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    manager.prepare_dir()
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    yield
    await mutator.stop()
    await manager.shutdown()
    await pool.close()


app = FastAPI(title="临床试验数据离线导出", lifespan=lifespan)


class StartRequest(BaseModel):
    trial_id: str
    slow_seconds: float = Field(0, ge=0, le=120, description="模拟慢查询秒数")
    kill_reader_after: float | None = Field(
        None, ge=0.5, le=120, description="N 秒后断开 labs 读连接（故障注入）"
    )


@app.get("/api/trials")
async def trials():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT trial_id, count(*) AS patients FROM patients "
            "GROUP BY trial_id ORDER BY trial_id"
        )
    return [{"trial_id": r["trial_id"], "patients": r["patients"]} for r in rows]


@app.post("/api/batches", status_code=201)
async def start_batch(req: StartRequest):
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM patients WHERE trial_id = $1 LIMIT 1", req.trial_id
        )
    if not exists:
        raise HTTPException(404, f"unknown trial_id: {req.trial_id!r}")
    # 若仍有运行中的批次，start() 内部会先取消它再开新批次
    batch = await manager.start(req.trial_id, req.slow_seconds, req.kill_reader_after)
    return batch.public()


@app.get("/api/batches")
async def list_batches():
    items = sorted(manager.batches.values(), key=lambda b: b.created_at, reverse=True)
    return [b.public() for b in items[:20]]


@app.get("/api/batches/current")
async def current_batch():
    batch = manager.current()
    return batch.public() if batch is not None else {"status": "idle"}


@app.get("/api/batches/{batch_id}")
async def get_batch(batch_id: str):
    batch = manager.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    return batch.public()


@app.post("/api/batches/{batch_id}/cancel")
async def cancel_batch(batch_id: str):
    """取消须及时受理：立即返回 cancelled，不等待慢查询结束。"""
    batch = await manager.cancel(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    return batch.public()


@app.get("/api/batches/{batch_id}/download")
async def download_batch(batch_id: str):
    batch = manager.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    if batch.status != READY or batch.zip_path is None or not batch.zip_path.exists():
        raise HTTPException(409, f"batch not ready for download (status={batch.status})")
    return FileResponse(
        batch.zip_path, filename=batch.zip_path.name, media_type="application/zip"
    )


@app.post("/api/mutator/start")
async def mutator_start():
    await mutator.start()
    return mutator.status()


@app.post("/api/mutator/stop")
async def mutator_stop():
    await mutator.stop()
    return mutator.status()


@app.get("/api/mutator")
async def mutator_status():
    return mutator.status()


@app.get("/api/connections")
async def connections():
    """导出/更正相关的数据库后端连接（用于观察连接释放与无锁状态）。"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pid, application_name, state, wait_event_type,
                   (now() - xact_start)::text AS xact_age
            FROM pg_stat_activity
            WHERE application_name LIKE 'export-%' OR application_name = 'mutator'
            ORDER BY pid
            """
        )
    return [dict(r) for r in rows]


static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
