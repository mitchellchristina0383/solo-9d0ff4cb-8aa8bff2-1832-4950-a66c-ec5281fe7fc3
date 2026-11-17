"""模拟写入后端。

每个后端实例：
  * 把收到的写入落到自己的 SQLite（/data/writes.db），持久化保存；
  * 每个**数据卷**在库内 meta 表保存终身身份 volume_id 与代次 generation：
      - volume_id 只在卷首次初始化时生成，随卷走、不随容器走；
      - 进程每次启动（或在演练中重新挂载卷）generation += 1；
      - 另有仅存于进程内的 instance_id，区分"同一进程"与"换了进程"。
    代理据此区分：同卷重启（volume_id 不变）与误挂空卷/异卷替换（volume_id 变了）。
  * 支持慢回执：POST /writes?delay=秒，先落库再延迟回执，模拟"已送达、未回执"；
  * 支持健康故障注入：POST /control/fault on/off，故障期间 /healthz 503、写入 500；
  * 写入栅栏：请求头 X-Expected-Volume-Id 与本卷不符时 409 拒绝且**绝不落库**，
    防止"代理探测之后、实际投递之前节点被换成空卷"导致写入落进新卷；
  * /writes/<id> 供代理在切流后仍向"原送达节点/原绑定卷"查询回执；
    404 响应同样带本卷身份，避免代理把新卷的"查无记录"当成旧写入的结论；
  * /control/simulate-replace 与 /control/simulate-restore 仅供演练：
    前者备份旧库后初始化一个空的新卷（新 volume_id），后者从备份恢复原卷。
"""
from __future__ import annotations

import asyncio
import glob
import os
import shutil
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

NODE_NAME = os.environ.get("NODE_NAME", "backend")
DB_PATH = os.environ.get("BACKEND_DB", "/data/writes.db")

# 故障开关是进程内瞬态，不需要落库。
_fault_on = False
# 正在慢回执中的请求 id -> asyncio.Event，用于 /writes/{id} 等待回执完成。
_pending: dict[str, asyncio.Event] = {}

# 进程级实例身份：只活在内存里，进程重启即换；卷身份在库里持久化。
_instance_id = uuid.uuid4().hex
# 当前挂载卷的身份，由 init_db 载入/初始化。
_volume_id: str = ""
_generation: int = 0


def _identity() -> dict[str, Any]:
    return {
        "node": NODE_NAME,
        "instance_id": _instance_id,
        "volume_id": _volume_id,
        "generation": _generation,
    }


async def init_db() -> None:
    """打开（或初始化）当前挂载的数据卷，读取/建立卷身份并令代次 +1。"""
    global _volume_id, _generation
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS writes (
                id           TEXT PRIMARY KEY,
                payload      TEXT NOT NULL,
                client_ip    TEXT,
                received_at  REAL NOT NULL,
                acked_at     REAL,
                delay        REAL NOT NULL DEFAULT 0
            )
            """
        )
        # 卷身份表：每个数据卷一行，随卷持久化；换空卷即没有这行/值不同。
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key    TEXT PRIMARY KEY,
                value  TEXT NOT NULL
            )
            """
        )
        cur = await db.execute("SELECT value FROM meta WHERE key = 'volume_id'")
        row = await cur.fetchone()
        if row is None:
            # 全新（空）卷：发放终身卷身份，代次从 1 开始。
            _volume_id = uuid.uuid4().hex
            _generation = 1
            await db.execute(
                "INSERT INTO meta (key, value) VALUES ('volume_id', ?)", (_volume_id,)
            )
            await db.execute(
                "INSERT INTO meta (key, value) VALUES ('generation', '1')"
            )
        else:
            _volume_id = row[0]
            cur = await db.execute("SELECT value FROM meta WHERE key = 'generation'")
            grow = await cur.fetchone()
            _generation = (int(grow[0]) if grow else 0) + 1
            await db.execute(
                "UPDATE meta SET value = ? WHERE key = 'generation'", (str(_generation),)
            )
        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title=f"mock-write-backend-{NODE_NAME}", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    # 无论健康与否都返回卷身份：代理在故障态也要能对账"挂着的是哪个卷"。
    if _fault_on:
        return JSONResponse(status_code=503,
                            content={**_identity(), "healthy": False})
    return JSONResponse(status_code=200,
                        content={**_identity(), "healthy": True})


@app.post("/control/fault")
async def control_fault(request: Request) -> dict[str, Any]:
    """注入/解除健康故障。body: {"on": true|false}"""
    global _fault_on
    body = await request.json()
    _fault_on = bool(body.get("on", True))
    return {**_identity(), "fault": _fault_on}


@app.post("/control/simulate-replace")
async def simulate_replace(request: Request) -> JSONResponse:
    """演练：模拟值班工程师误挂空数据卷替换容器（节点名/地址不变）。

    先把当前卷备份到同目录，再删除库文件并初始化一个全新的空卷
    （新 volume_id、generation 重新计），旧写入随之"消失"。
    """
    old_identity = _identity()
    # WAL 先收敛再复制，保证备份是完整快照。
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    backup = f"{DB_PATH}.backup-{int(time.time() * 1000)}"
    shutil.copy2(DB_PATH, backup)
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(DB_PATH + suffix)
        except FileNotFoundError:
            pass
    _pending.clear()
    await init_db()
    return JSONResponse(content={
        "event": "simulated_empty_volume_replace",
        "old": old_identity,
        "new": _identity(),
        "backup": os.path.basename(backup),
    })


@app.post("/control/simulate-restore")
async def simulate_restore(request: Request) -> JSONResponse:
    """演练：把最近一次备份的原卷恢复回来（原 volume_id 回来，generation 继续递增）。"""
    backups = sorted(glob.glob(f"{DB_PATH}.backup-*"))
    if not backups:
        return JSONResponse(status_code=404,
                            content={"error": "no volume backup found to restore"})
    backup = backups[-1]
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(DB_PATH + suffix)
        except FileNotFoundError:
            pass
    shutil.copy2(backup, DB_PATH)
    _pending.clear()
    await init_db()
    return JSONResponse(content={
        "event": "simulated_original_volume_restored",
        "restored_from": os.path.basename(backup),
        "current": _identity(),
    })


@app.post("/writes")
async def create_write(request: Request) -> JSONResponse:
    # 卷栅栏：代理投递前会带上它认定的卷身份；节点已被换卷时直接拒绝，绝不落库。
    expected_volume = request.headers.get("x-expected-volume-id", "").strip()
    if expected_volume and expected_volume != _volume_id:
        return JSONResponse(status_code=409, content={
            "error": "volume fence mismatch: write rejected, nothing persisted",
            "expected_volume": expected_volume,
            "actual_volume": _volume_id,
            **_identity(),
        })

    if _fault_on:
        # 故障期间拒绝写入（代理探测会先失败，正常不会走到这里）。
        return JSONResponse(
            status_code=500,
            content={"node": NODE_NAME, "error": "node in fault state, write rejected"},
        )

    delay = float(request.query_params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    write_id = str(body.get("id") or uuid.uuid4())
    payload = str(body.get("payload", ""))
    now = time.time()

    # 先落库，再慢回执：即使回执期间连接被切，写入也已经在本节点持久化。
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO writes (id, payload, client_ip, received_at, delay) "
            "VALUES (?, ?, ?, ?, ?)",
            (write_id, payload, request.client.host if request.client else None, now, delay),
        )
        await db.commit()

    if delay > 0:
        done = asyncio.Event()
        _pending[write_id] = done
        try:
            await asyncio.sleep(delay)
        finally:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
                )
                await db.commit()
            _pending.pop(write_id, None)
            done.set()
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
            )
            await db.commit()

    return JSONResponse(
        status_code=201,
        content={
            "id": write_id,
            "received_at": now,
            "delay": delay,
            "status": "acked",
            **_identity(),
        },
    )


@app.get("/writes/{write_id}")
async def get_write(write_id: str, wait: bool = False) -> JSONResponse:
    """查询单条写入的回执状态。

    wait=true 时，如果该写入仍在慢回执中，最多等待 60 秒，供代理向原送达节点取回执。
    无论找到与否都带上**当前卷身份**：代理据此判断"查无记录"是不是原卷给出的结论。
    """
    if wait and write_id in _pending:
        try:
            await asyncio.wait_for(_pending[write_id].wait(), timeout=60)
        except asyncio.TimeoutError:
            pass

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        row = await cur.fetchone()
    if row is None:
        return JSONResponse(
            status_code=404,
            content={"id": write_id, "found": False, **_identity()},
        )
    data = dict(row)
    data["found"] = True
    data["status"] = "acked" if data["acked_at"] is not None else "pending"
    data.update(_identity())
    return JSONResponse(status_code=200, content=data)


@app.get("/records")
async def records() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, payload, client_ip, received_at, acked_at, delay "
            "FROM writes ORDER BY received_at"
        )
        rows = await cur.fetchall()
    return {"count": len(rows), "records": [dict(r) for r in rows], **_identity()}
