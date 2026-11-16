"""批次导出管理器。

核心机制：PostgreSQL 可导出快照（pg_export_snapshot / SET TRANSACTION SNAPSHOT）。

- 一条 keeper 连接开启 REPEATABLE READ 事务并导出快照，整个导出期间保持打开；
- 两条相互独立的 reader 连接各自开启 REPEATABLE READ 事务并
  SET TRANSACTION SNAPSHOT 到同一快照，然后并行读取 patients / labs；
- 快照基于 MVCC，不取任何表锁，源端写入（试验归属/检验值更正）不会被阻塞；
- 取消：状态立即置为 cancelled（HTTP 即时受理），随后异步
  pg_cancel_backend 终止后端慢查询，连接在后台释放，调用方不等待；
- 状态迁移只允许 running -> ready/failed/cancelled，因此迟到的旧批次任务
  无法改写已取消批次或新批次的状态；每批产物在独立目录，失败/取消即删除。
"""

from __future__ import annotations

import asyncio
import csv
import shutil
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import asyncpg

RUNNING = "running"
READY = "ready"
FAILED = "failed"
CANCELLED = "cancelled"

PATIENTS_SQL = (
    "SELECT patient_id, trial_id FROM patients "
    "WHERE trial_id = $1 ORDER BY patient_id"
)
LABS_SQL = (
    "SELECT l.lab_id, l.patient_id, l.value "
    "FROM labs l JOIN patients p ON p.patient_id = l.patient_id "
    "WHERE p.trial_id = $1 ORDER BY l.lab_id"
)


class BatchCancelled(Exception):
    """批次在执行途中被取消（内部控制流异常）。"""


@dataclass
class Batch:
    id: str
    trial_id: str
    slow_seconds: float = 0.0
    kill_reader_after: float | None = None
    status: str = RUNNING
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    patients_rows: int = 0
    labs_rows: int = 0
    directory: Path | None = None
    zip_path: Path | None = None
    conns: list[asyncpg.Connection] = field(default_factory=list)
    pids: list[int] = field(default_factory=list)
    task: asyncio.Task | None = None
    watchdog: asyncio.Task | None = None

    @property
    def connections_open(self) -> int:
        return sum(1 for c in self.conns if not c.is_closed())

    def public(self) -> dict:
        return {
            "id": self.id,
            "trial_id": self.trial_id,
            "status": self.status,
            "error": self.error,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "patients_rows": self.patients_rows,
            "labs_rows": self.labs_rows,
            "slow_seconds": self.slow_seconds,
            "kill_reader_after": self.kill_reader_after,
            "connections_open": self.connections_open,
            "backend_pids": list(self.pids),
            "download_url": (
                f"/api/batches/{self.id}/download" if self.status == READY else None
            ),
        }


class BatchManager:
    def __init__(self, dsn: str, export_dir: str):
        self.dsn = dsn
        self.export_dir = Path(export_dir)
        self.batches: dict[str, Batch] = {}
        self.current_id: str | None = None
        self._lock = asyncio.Lock()

    def prepare_dir(self) -> None:
        if self.export_dir.exists():
            shutil.rmtree(self.export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def current(self) -> Batch | None:
        return self.batches.get(self.current_id) if self.current_id else None

    # ------------------------------------------------------------------ API

    async def start(
        self,
        trial_id: str,
        slow_seconds: float = 0.0,
        kill_reader_after: float | None = None,
    ) -> Batch:
        async with self._lock:
            cur = self.current()
            if cur is not None and cur.status == RUNNING:
                # 开始新批次前先取消仍在运行的旧批次
                self._cancel_locked(cur)
            batch = Batch(
                id=uuid.uuid4().hex,
                trial_id=trial_id,
                slow_seconds=slow_seconds,
                kill_reader_after=kill_reader_after,
            )
            batch.directory = self.export_dir / batch.id
            batch.directory.mkdir(parents=True, exist_ok=True)
            self.batches[batch.id] = batch
            self.current_id = batch.id
            batch.task = asyncio.create_task(self._run(batch))
            return batch

    async def cancel(self, batch_id: str) -> Batch | None:
        batch = self.batches.get(batch_id)
        if batch is None:
            return None
        async with self._lock:
            self._cancel_locked(batch)
        return batch

    async def shutdown(self) -> None:
        async with self._lock:
            for batch in self.batches.values():
                if batch.status == RUNNING:
                    self._cancel_locked(batch)
        tasks = [b.task for b in self.batches.values() if b.task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------- cancel

    def _cancel_locked(self, batch: Batch) -> bool:
        """立即受理取消：同步翻转状态，异步终止后端查询。

        不等待慢查询结束 —— 调用方（HTTP 层）马上拿到 cancelled 状态；
        连接清理由 _cancel_backends 与 _run 的 finally 在后台完成。
        """
        if batch.status != RUNNING:
            return False
        batch.status = CANCELLED
        batch.finished_at = time.time()
        if batch.watchdog is not None:
            batch.watchdog.cancel()
        asyncio.create_task(self._cancel_backends(batch))
        return True

    async def _cancel_backends(self, batch: Batch) -> None:
        pids = list(batch.pids)
        try:
            ctrl = await asyncpg.connect(
                self.dsn,
                server_settings={"application_name": f"export-ctrl-{batch.id[:8]}"},
            )
            try:
                for pid in pids:
                    # 取消后端正在执行的查询（pg_sleep / 大查询都会被立即打断）
                    await ctrl.execute("SELECT pg_cancel_backend($1)", pid)
            finally:
                await ctrl.close()
        except Exception:
            pass
        # 兜底：短暂宽限后仍存活的连接直接断开，确保本批连接一定被释放
        await asyncio.sleep(1.0)
        for conn in list(batch.conns):
            if not conn.is_closed():
                conn.terminate()

    # ---------------------------------------------------------------- run

    async def _run(self, batch: Batch) -> None:
        keeper: asyncpg.Connection | None = None
        readers: list[asyncpg.Connection] = []
        try:
            # 1) keeper 事务导出快照，整个导出期间保持打开
            keeper = await self._connect(batch, "keeper")
            await keeper.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            snapshot = await keeper.fetchval("SELECT pg_export_snapshot()")
            self._check_cancelled(batch)

            # 2) 两条独立 reader 连接导入同一快照
            for role in ("patients", "labs"):
                conn = await self._connect(batch, role)
                await conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                await conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")
                readers.append(conn)
            self._check_cancelled(batch)

            # 故障注入：演示读连接被断开
            if batch.kill_reader_after is not None:
                batch.watchdog = asyncio.create_task(
                    self._kill_reader(batch, readers[1])
                )

            # 3) 两表并行读取（各自独立连接、同一快照）
            patients_csv = batch.directory / "patients.csv"
            labs_csv = batch.directory / "labs.csv"
            batch.patients_rows, batch.labs_rows = await asyncio.gather(
                self._read_table(
                    batch, readers[0], PATIENTS_SQL,
                    ("patient_id", "trial_id"), patients_csv,
                ),
                self._read_table(
                    batch, readers[1], LABS_SQL,
                    ("lab_id", "patient_id", "value"), labs_csv,
                ),
            )
            self._check_cancelled(batch)

            # 4) 两份 CSV 均完整才打包 ZIP
            zip_path = batch.directory / f"export_{batch.trial_id}_{batch.id[:8]}.zip"
            await asyncio.to_thread(self._make_zip, zip_path, patients_csv, labs_csv)
            self._check_cancelled(batch)

            if not self._finish(batch, READY):
                raise BatchCancelled()  # 打包期间被取消，丢弃产物
            batch.zip_path = zip_path
        except BatchCancelled:
            self._finish(batch, CANCELLED)
            self._discard(batch)
        except Exception as exc:  # 任一读取失败 -> 本批失败，不开放下载
            self._finish(batch, FAILED, error=f"{type(exc).__name__}: {exc}")
            self._discard(batch)
        finally:
            if batch.watchdog is not None:
                batch.watchdog.cancel()
            await self._release(batch, keeper, readers)

    async def _read_table(self, batch, conn, sql, header, path) -> int:
        if batch.slow_seconds > 0:
            # 模拟慢查询：pg_sleep 与正式查询处于同一快照事务，可被取消
            await conn.fetchval("SELECT pg_sleep($1)", batch.slow_seconds)
        self._check_cancelled(batch)
        rows = await conn.fetch(sql, batch.trial_id)
        self._check_cancelled(batch)
        await asyncio.to_thread(self._write_csv, path, header, rows)
        return len(rows)

    # ------------------------------------------------------------ helpers

    async def _connect(self, batch: Batch, role: str) -> asyncpg.Connection:
        conn = await asyncpg.connect(
            self.dsn,
            server_settings={
                "application_name": f"export-{role}-{batch.id[:8]}",
                "jit": "off",
            },
        )
        batch.conns.append(conn)
        batch.pids.append(conn.get_server_pid())
        return conn

    async def _kill_reader(self, batch: Batch, reader: asyncpg.Connection) -> None:
        """故障注入：N 秒后断开 labs 读连接。

        双保险：先 pg_terminate_backend 断服务端后端（正常环境生效），
        再客户端 terminate() —— 无论服务端信号是否可达，应用都会立即
        观察到读连接断开。
        """
        try:
            await asyncio.sleep(batch.kill_reader_after)
            pid = reader.get_server_pid()
            try:
                ctrl = await asyncpg.connect(
                    self.dsn,
                    server_settings={
                        "application_name": f"export-killer-{batch.id[:8]}"
                    },
                )
                try:
                    await ctrl.execute("SELECT pg_terminate_backend($1)", pid)
                finally:
                    await ctrl.close()
            except Exception:
                pass
            reader.terminate()
        except (asyncio.CancelledError, Exception):
            pass

    async def _release(self, batch, keeper, readers) -> None:
        """释放本批全部连接（keeper + 两条 reader）。"""
        for conn in [keeper, *readers]:
            if conn is None:
                continue
            try:
                await asyncio.wait_for(conn.close(), timeout=5)
            except Exception:
                try:
                    conn.terminate()
                except Exception:
                    pass
        batch.conns.clear()

    @staticmethod
    def _check_cancelled(batch: Batch) -> None:
        if batch.status != RUNNING:
            raise BatchCancelled()

    @staticmethod
    def _finish(batch: Batch, status: str, error: str | None = None) -> bool:
        """只允许 running -> 终态 的迁移。

        批次已被取消（或已结束）时返回 False —— 迟到的旧任务不得改写状态。
        """
        if batch.status != RUNNING:
            return False
        batch.status = status
        batch.error = error
        batch.finished_at = time.time()
        return True

    def _discard(self, batch: Batch) -> None:
        """取消/失败的批次不保留任何可下载产物。"""
        if batch.directory is not None and batch.directory.exists():
            shutil.rmtree(batch.directory, ignore_errors=True)
        batch.zip_path = None

    @staticmethod
    def _write_csv(path, header, rows) -> None:
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in rows:
                writer.writerow([row[i] for i in range(len(header))])

    @staticmethod
    def _make_zip(zip_path, patients_csv, labs_csv) -> None:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(patients_csv, "patients.csv")
            zf.write(labs_csv, "labs.csv")
