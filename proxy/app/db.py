"""代理侧 SQLite 持久层：节点配置、当前路由、卷身份台账、写入记录、切流事件。

写入记录一旦落库即代表"已送达某节点的某个数据卷"，回执状态只能就地向原节点、
**原绑定卷**查询更新，任何状态机代码都不会把同一条写入再投到另一个节点或另一个卷。

卷身份模型（修复"误挂空卷替换"故障的核心）：
  * 后端每个数据卷在自己的 SQLite 里存终身 volume_id，进程启动令 generation +1；
  * nodes 表记录每个节点**当前挂载卷**与节点状态（OK / QUARANTINED）；
  * node_volumes 台账记录在该节点上见过的所有卷，trusted=1 表示首次见到的原卷；
  * 对账时 volume_id 变化：命中 trusted 卷 = 原卷恢复（解除隔离）；
    未见过的卷 = 异卷/空卷替换（隔离，不接新写入、不可切流）。
"""
from __future__ import annotations

import os
import time
from typing import Any

import aiosqlite

DB_PATH = os.environ.get("PROXY_DB", "/data/proxy.db")

# 写入状态机
PENDING = "PENDING"        # 已送达/投递中，尚未拿到回执
ACKED = "ACKED"            # 已拿到原节点成功回执
TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"  # 写入已落库但回执超时，结果不确定
FAILED = "FAILED"          # 节点明确拒绝（连接前失败/5xx/卷栅栏拦截），未送达

# 节点状态机
OK = "OK"                          # 挂载的是已知（可信）卷
QUARANTINED = "QUARANTINED"        # 挂载了未见过的异卷/空卷，冻结新写入与切流

# 单连接 + 单写锁，SQLite 写入串行化，避免 lock 竞争。
_conn: aiosqlite.Connection | None = None
_write_lock: Any = None


async def init_db() -> None:
    global _conn, _write_lock
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _conn = aiosqlite.connect(DB_PATH)
    await _conn
    _write_lock = asyncio_lock()
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            name        TEXT PRIMARY KEY,
            base_url    TEXT NOT NULL,
            created_at  REAL NOT NULL,
            state       TEXT NOT NULL DEFAULT 'OK',
            current_volume_id  TEXT,
            current_generation INTEGER,
            last_instance_id   TEXT,
            identity_updated_at REAL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            active      TEXT NOT NULL,
            updated_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            id           TEXT PRIMARY KEY,
            target_node  TEXT NOT NULL,
            volume_id    TEXT,
            payload      TEXT NOT NULL,
            delay_hint   REAL NOT NULL DEFAULT 0,
            status       TEXT NOT NULL,
            status_detail TEXT NOT NULL DEFAULT '',
            created_at   REAL NOT NULL,
            updated_at   REAL NOT NULL,
            acked_at     REAL
        )
        """
    )
    # 节点上出现过的数据卷台账：trusted=1 是首次见到的原卷，其余为替换上来的异卷。
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS node_volumes (
            node        TEXT NOT NULL,
            volume_id   TEXT NOT NULL,
            first_seen  REAL NOT NULL,
            trusted     INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (node, volume_id)
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL NOT NULL,
            kind        TEXT NOT NULL,
            message     TEXT NOT NULL
        )
        """
    )
    # 对升级前已存在的旧库做平滑加列。
    await _ensure_column("nodes", "state", f"TEXT NOT NULL DEFAULT '{OK}'")
    await _ensure_column("nodes", "current_volume_id", "TEXT")
    await _ensure_column("nodes", "current_generation", "INTEGER")
    await _ensure_column("nodes", "last_instance_id", "TEXT")
    await _ensure_column("nodes", "identity_updated_at", "REAL")
    await _ensure_column("writes", "volume_id", "TEXT")
    await _conn.commit()


async def _ensure_column(table: str, column: str, decl: str) -> None:
    cur = await _conn.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in await cur.fetchall()}
    if column not in cols:
        await _conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def asyncio_lock() -> Any:
    import asyncio

    return asyncio.Lock()


async def register_node(name: str, base_url: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT OR IGNORE INTO nodes (name, base_url, created_at, state) "
            "VALUES (?, ?, ?, ?)",
            (name, base_url, time.time(), OK),
        )
        # 已存在则只更新地址（compose 重建容器场景）；绝不重置隔离状态与卷身份。
        await _conn.execute(
            "UPDATE nodes SET base_url = ? WHERE name = ?", (base_url, name)
        )
        cur = await _conn.execute("SELECT COUNT(*) AS c FROM route")
        row = await cur.fetchone()
        if row["c"] == 0:
            await _conn.execute(
                "INSERT INTO route (id, active, updated_at) VALUES (1, ?, ?)",
                (name, time.time()),
            )
        await _conn.commit()


async def get_active() -> str:
    cur = await _conn.execute("SELECT active FROM route WHERE id = 1")
    row = await cur.fetchone()
    return row["active"]


async def set_active(name: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE route SET active = ?, updated_at = ? WHERE id = 1",
            (name, time.time()),
        )
        await _conn.commit()


async def list_nodes() -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT name, base_url, state, current_volume_id, current_generation, "
        "last_instance_id, identity_updated_at FROM nodes ORDER BY name"
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_node(name: str) -> dict[str, Any] | None:
    cur = await _conn.execute(
        "SELECT name, base_url, state, current_volume_id, current_generation, "
        "last_instance_id, identity_updated_at FROM nodes WHERE name = ?",
        (name,),
    )
    row = await cur.fetchone()
    return dict(row) if row else None


async def node_url(name: str) -> str | None:
    cur = await _conn.execute("SELECT base_url FROM nodes WHERE name = ?", (name,))
    row = await cur.fetchone()
    return row["base_url"] if row else None


async def list_node_volumes(name: str) -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT volume_id, first_seen, trusted FROM node_volumes "
        "WHERE node = ? ORDER BY first_seen",
        (name,),
    )
    return [dict(r) for r in await cur.fetchall()]


async def reconcile_identity(
    name: str, volume_id: str, generation: int, instance_id: str, now: float
) -> dict[str, Any] | None:
    """用一次最新观测到的卷身份对账节点状态。

    返回变化描述（kind 见下），无变化返回 None。调用方据此写事件日志。
      first_seen          首次观测：登记为可信原卷
      same_volume_restart 卷未换、进程重启（generation/instance 变化），状态不变
      volume_replace      换上未见过的异卷/空卷：隔离
      volume_recovered    已知可信卷回来了：解除隔离
    """
    async with _write_lock:
        cur = await _conn.execute("SELECT * FROM nodes WHERE name = ?", (name,))
        node_row = await cur.fetchone()
        if node_row is None:
            return None
        node = dict(node_row)
        cur = await _conn.execute(
            "SELECT trusted FROM node_volumes WHERE node = ? AND volume_id = ?",
            (name, volume_id),
        )
        vol_row = await cur.fetchone()
        trusted = bool(vol_row["trusted"]) if vol_row else False

        previous_volume = node["current_volume_id"]
        previous_instance = node["last_instance_id"]
        previous_generation = node["current_generation"]
        previous_state = node["state"]

        if previous_volume is None:
            kind = "first_seen"
            new_state = OK
            await _conn.execute(
                "INSERT OR IGNORE INTO node_volumes (node, volume_id, first_seen, trusted) "
                "VALUES (?, ?, ?, 1)",
                (name, volume_id, now),
            )
        elif volume_id == previous_volume:
            # 同一个卷：可能只是进程重启。隔离态由"异卷"产生，同卷观测不得解除它。
            restarted = (
                instance_id != previous_instance or generation != previous_generation
            )
            kind = "same_volume_restart" if restarted else "unchanged"
            new_state = previous_state
        elif trusted:
            kind = "volume_recovered"
            new_state = OK
        else:
            kind = "volume_replace"
            new_state = QUARANTINED
            await _conn.execute(
                "INSERT OR IGNORE INTO node_volumes (node, volume_id, first_seen, trusted) "
                "VALUES (?, ?, ?, 0)",
                (name, volume_id, now),
            )

        await _conn.execute(
            "UPDATE nodes SET state = ?, current_volume_id = ?, current_generation = ?, "
            "last_instance_id = ?, identity_updated_at = ? WHERE name = ?",
            (new_state, volume_id, generation, instance_id, now, name),
        )
        await _conn.commit()
        return {
            "kind": kind,
            "node": name,
            "state": new_state,
            "previous_state": previous_state,
            "volume_id": volume_id,
            "previous_volume_id": previous_volume,
            "generation": generation,
            "instance_id": instance_id,
            "restarted": kind == "same_volume_restart",
        }


async def insert_write(
    write_id: str, target_node: str, volume_id: str | None,
    payload: str, delay_hint: float,
) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO writes (id, target_node, volume_id, payload, delay_hint, status, "
            "status_detail, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                write_id,
                target_node,
                volume_id,
                payload,
                delay_hint,
                PENDING,
                "delivered, awaiting ack",
                time.time(),
                time.time(),
            ),
        )
        await _conn.commit()


async def update_write_status(
    write_id: str, status: str, detail: str, acked: bool = False
) -> None:
    async with _write_lock:
        if acked:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
                "acked_at = ? WHERE id = ?",
                (status, detail, time.time(), time.time(), write_id),
            )
        else:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ? "
                "WHERE id = ?",
                (status, detail, time.time(), write_id),
            )
        await _conn.commit()


async def get_write(write_id: str) -> dict[str, Any] | None:
    cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def list_writes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT * FROM writes ORDER BY created_at DESC LIMIT 200")
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def add_event(kind: str, message: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO events (ts, kind, message) VALUES (?, ?, ?)",
            (time.time(), kind, message),
        )
        await _conn.commit()


async def list_events(limit: int = 30) -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT ts, kind, message FROM events ORDER BY id DESC LIMIT ?", (limit,)
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]
