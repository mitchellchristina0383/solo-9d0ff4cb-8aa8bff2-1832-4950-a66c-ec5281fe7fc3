"""FastAPI 反向代理 + 写入切流控制台。

切流安全语义（本文件的所有代码路径都遵守）：
  1. 每个写入请求在发出前快照"当前现役节点 + 其当前数据卷身份"，并把节点名与
     volume_id 一并持久化到 writes 行；之后无论怎么切流/换卷，这个请求只与该节点
     的**原绑定卷**通信，绝不重投到另一节点或另一卷。
  2. 切流只改 route 表，对已经发往旧节点、尚未回执的在途请求不做任何取消动作；
     回执仍从原节点读取（也可事后用 /api/writes/{id}/refresh 向原节点/原卷查询）。
  3. 切流前先探测目标节点 /healthz，探测失败则保留旧路由并返回 409；
     目标节点处于隔离态（挂了异卷/空卷）同样 409，不得成为切流目标。
  4. 读超时（写入大概率已在后端落库，只是回执没回来）标记为 TIMEOUT_UNCERTAIN，
     返回 504 并在页面提示"结果不确定"，代理不做任何自动补投/重试。

卷身份与隔离语义（修复"误挂空卷替换"故障）：
  5. 后端每个数据卷有终身 volume_id（存在后端 SQLite 里），进程重启 generation+1；
     代理后台周期对账：volume_id 变成未登记的异卷/空卷 -> 节点 QUARANTINED：
     健康检查仍 200 也不接新写入（503）、不可切流（409），但旧写入的归属与确认
     状态原样保留；已登记的原卷重新挂回 -> 自动解除隔离。
  6. 写入三重栅栏，防止"探测之后、实际投递之前换卷"：
       a. 投递前再探一次 /healthz 取卷身份快照，取不到身份直接失败（fail-closed）；
       b. 请求头带 X-Expected-Volume-Id，后端发现卷不符即 409 拒绝且不落库；
       c. 响应回来再复核 ack 中的卷身份，不符不确认回执。
  7. 回执核实必须由原绑定卷作答：异卷返回的 404 只说明"新卷没有这条记录"，
     绝不作为旧写入的核实结论，原状态保留，等原卷恢复后再核实。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import db

NODE_A = os.environ.get("NODE_A_NAME", "A")
NODE_B = os.environ.get("NODE_B_NAME", "B")
URL_A = os.environ.get("BACKEND_A_URL", "http://127.0.0.1:8001")
URL_B = os.environ.get("BACKEND_B_URL", "http://127.0.0.1:8002")

# 连接失败 = 节点不可达、请求未送达；读超时 = 请求已发出、回执未归。
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "3.0"))
READ_TIMEOUT = float(os.environ.get("WRITER_READ_TIMEOUT", "30.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "2.5"))
# 卷身份后台对账周期（秒）。
IDENTITY_POLL_INTERVAL = float(os.environ.get("IDENTITY_POLL_INTERVAL", "2.0"))

# node -> 在途写入 id 集合（仅内存态，进程重启后以 DB 中 PENDING 行作为排空依据）。
_inflight: dict[str, set[str]] = {}
_inflight_lock = asyncio.Lock()


# ---------------------------------------------------------------- 健康探测 / 卷对账

async def probe_node(base_url: str) -> tuple[bool, str, dict[str, Any] | None]:
    """返回 (是否健康, 说明, 卷身份)。探测本身绝不触发任何写入。

    503（故障注入）响应体里同样带卷身份，因此故障态也能对账"挂的是哪个卷"。
    """
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/healthz", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return False, f"probe request failed: {type(exc).__name__}: {exc}", None
    try:
        data = resp.json()
    except Exception:
        data = {}
    identity = None
    if data.get("volume_id"):
        identity = {
            "volume_id": data["volume_id"],
            "generation": data.get("generation"),
            "instance_id": data.get("instance_id"),
        }
    if resp.status_code == 200:
        return True, "healthy", identity
    return False, f"healthz returned HTTP {resp.status_code}: {resp.text[:200]}", identity


async def observe_node(name: str, base_url: str) -> dict[str, Any] | None:
    """探测一次并把卷身份对账入库；返回变化描述（无变化/取不到身份时 None）。"""
    _, _, identity = await probe_node(base_url)
    if identity is None:
        return None
    return await db.reconcile_identity(
        name,
        identity["volume_id"],
        identity["generation"] or 0,
        identity["instance_id"] or "",
        time.time(),
    )


_EVENT_KIND = {
    "first_seen": "VOLUME_FIRST_SEEN",
    "same_volume_restart": "VOLUME_RESTART",
    "volume_replace": "VOLUME_REPLACE",
    "volume_recovered": "VOLUME_RECOVERED",
}


async def log_identity_change(change: dict[str, Any]) -> None:
    kind = _EVENT_KIND.get(change["kind"])
    if kind is None:
        return
    short = lambda v: (v or "")[:8]
    if change["kind"] == "same_volume_restart":
        msg = (f"node {change['node']} same volume {short(change['volume_id'])} "
               f"restarted (generation={change['generation']}); bindings intact")
    elif change["kind"] == "first_seen":
        msg = (f"node {change['node']} first seen with volume "
               f"{short(change['volume_id'])}, registered as trusted original volume")
    elif change["kind"] == "volume_replace":
        msg = (f"node {change['node']} volume CHANGED "
               f"{short(change['previous_volume_id'])} -> "
               f"{short(change['volume_id'])} (generation={change['generation']}); "
               "foreign/empty volume -> QUARANTINED: no new writes, not a switch target; "
               "old writes keep original ownership and ack status")
    else:
        msg = (f"node {change['node']} trusted volume {short(change['volume_id'])} "
               "restored -> quarantine lifted; delivery/cutover/verification resume")
    await db.add_event(kind, msg)


async def identity_monitor() -> None:
    """后台对账循环：节点被换卷后最迟一个周期内被识别并隔离。"""
    while True:
        await asyncio.sleep(IDENTITY_POLL_INTERVAL)
        try:
            for node in await db.list_nodes():
                change = await observe_node(node["name"], node["base_url"])
                if change:
                    await log_identity_change(change)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 对账失败不能打死代理进程
            await db.add_event("MONITOR_ERROR", f"identity monitor error: {exc!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    await db.register_node(NODE_A, URL_A)
    await db.register_node(NODE_B, URL_B)
    _inflight.setdefault(NODE_A, set())
    _inflight.setdefault(NODE_B, set())
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT,
                              write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        # 显式关闭 httpx 默认的连接重试，保证"一次投递"语义。
        limits=httpx.Limits(max_connections=100),
    )
    # 启动先对账一轮：若节点已被换卷，无需等下一个周期即隔离。
    for node in await db.list_nodes():
        change = await observe_node(node["name"], node["base_url"])
        if change:
            await log_identity_change(change)
    app.state.monitor_task = asyncio.create_task(identity_monitor())
    await db.add_event("BOOT", f"proxy started, active={await db.get_active()}")
    try:
        yield
    finally:
        app.state.monitor_task.cancel()
        try:
            await app.state.monitor_task
        except asyncio.CancelledError:
            pass
        await app.state.http.aclose()


app = FastAPI(title="write-cutover-proxy", lifespan=lifespan)


# ---------------------------------------------------------------- 切流

@app.post("/api/switch")
async def switch(request: Request) -> JSONResponse:
    body = await request.json()
    target = str(body.get("target", "")).strip()
    nodes = {n["name"]: n for n in await db.list_nodes()}
    if target not in nodes:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"unknown target node: {target!r}",
                     "known_nodes": sorted(nodes)},
        )

    active = await db.get_active()
    if target == active:
        return JSONResponse(content={"ok": True, "noop": True, "active": active,
                                     "message": "target is already active"})

    target_row = nodes[target]
    if target_row["state"] == db.QUARANTINED:
        # 异卷/空卷节点健康也没用：不得成为切流目标。
        reason = (f"target {target} is QUARANTINED: mounted foreign/empty volume "
                  f"{(target_row['current_volume_id'] or '')[:8]}; cutover refused")
        await db.add_event("SWITCH_BLOCKED", f"{active} -> {target} blocked: {reason}")
        return JSONResponse(status_code=409,
                            content={"ok": False, "active": active, "blocked": True,
                                     "quarantined": True, "reason": reason,
                                     "message": "kept old route"})

    # 探测前先即时对账一次，堵住"后台周期未到 + 探测前刚换卷"的窗口。
    await observe_node(target, target_row["base_url"])
    target_row = await db.get_node(target)
    healthy, detail, identity = await probe_node(target_row["base_url"])
    if not healthy:
        # 探测失败：保持旧路由，不动 route 表。
        await db.add_event(
            "SWITCH_BLOCKED",
            f"{active} -> {target} blocked, target unhealthy: {detail}",
        )
        return JSONResponse(
            status_code=409,
            content={"ok": False, "active": active, "blocked": True,
                     "reason": f"target {target} probe failed: {detail}",
                     "message": "kept old route"},
        )
    if target_row["state"] == db.QUARANTINED or (
        identity and target_row["current_volume_id"]
        and identity["volume_id"] != target_row["current_volume_id"]
    ):
        reason = (f"target {target} volume identity changed at probe time "
                  f"(expected {(target_row['current_volume_id'] or '')[:8]}, "
                  f"got {(identity or {}).get('volume_id', '?')[:8]}); cutover refused")
        await db.add_event("SWITCH_BLOCKED", f"{active} -> {target} blocked: {reason}")
        return JSONResponse(status_code=409,
                            content={"ok": False, "active": active, "blocked": True,
                                     "quarantined": True, "reason": reason,
                                     "message": "kept old route"})

    async with _inflight_lock:
        inflight_snapshot = {k: len(v) for k, v in _inflight.items()}
        await db.set_active(target)
    await db.add_event(
        "SWITCH_OK",
        f"{active} -> {target} ok; inflight at cutover={inflight_snapshot}; "
        "new requests now route to target, in-flight requests stay on old node",
    )
    return JSONResponse(content={
        "ok": True, "active": target, "previous": active,
        "inflight_at_cutover": inflight_snapshot,
        "message": "switch done; in-flight writes continue against their original node",
    })


# ---------------------------------------------------------------- 写入代理

async def _track(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).add(write_id)


async def _untrack(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).discard(write_id)


@app.post("/api/writes")
async def proxy_write(request: Request) -> JSONResponse:
    params = request.query_params
    delay = float(params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"payload": body}

    # 路由在投递前快照，切流不影响本请求。
    active = await db.get_active()
    node_row = await db.get_node(active)
    base_url = node_row["base_url"] if node_row else await db.node_url(active)

    # 闸门 0：隔离节点（异卷/空卷）不得接收新写入——即使它 /healthz 是 200。
    if node_row and node_row["state"] == db.QUARANTINED:
        detail = (f"node {active} QUARANTINED (foreign/empty volume "
                  f"{(node_row['current_volume_id'] or '')[:8]}); write refused, "
                  "nothing delivered; switch traffic to a healthy standby node instead")
        await db.add_event("WRITE_BLOCKED", detail)
        return JSONResponse(status_code=503,
                            content={"node": active, "blocked": True,
                                     "quarantined": True, "status": "BLOCKED",
                                     "detail": detail})

    # 闸门 1：投递前即时探测取卷身份快照；取不到身份 fail-closed，绝不盲投。
    healthy, probe_detail, identity = await probe_node(base_url)
    if identity is None:
        detail = (f"cannot establish volume identity of {active} before delivery "
                  f"({probe_detail}); write refused (fail-closed), nothing delivered")
        await db.add_event("WRITE_BLOCKED", detail)
        return JSONResponse(status_code=503,
                            content={"node": active, "blocked": True,
                                     "status": "BLOCKED", "detail": detail})
    expected_volume = identity["volume_id"]
    # 探测与投递之间的二次库内校验：监控任务可能刚把该节点隔离。
    node_row = await db.get_node(active)
    if node_row and (node_row["state"] == db.QUARANTINED
                     or node_row["current_volume_id"] != expected_volume):
        detail = (f"node {active} volume changed between probe and delivery; "
                  "write refused, nothing delivered")
        await db.add_event("WRITE_BLOCKED", detail)
        return JSONResponse(status_code=503,
                            content={"node": active, "blocked": True,
                                     "quarantined": True, "status": "BLOCKED",
                                     "detail": detail})

    write_id = str(body.get("id") or uuid.uuid4())
    body["id"] = write_id
    payload = str(body.get("payload", ""))

    # 绑定"节点 + 卷身份"后落库；旧写入的归属此后不再随实例/卷变化而改变。
    await db.insert_write(write_id, active, expected_volume, payload, delay)
    await _track(active, write_id)
    client: httpx.AsyncClient = app.state.http
    try:
        # 闸门 2：把卷身份带给后端；若后端此刻已被换卷，它会 409 拒绝且不落库。
        resp = await client.post(
            f"{base_url}/writes", params={"delay": delay}, json=body,
            headers={"X-Expected-Volume-Id": expected_volume},
        )
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout,
            httpx.WriteTimeout) as exc:
        # 连接层面失败：请求没有被后端受理，标记 FAILED，不向任何节点补投。
        detail = f"connect to {active} failed: {type(exc).__name__}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "detail": detail})
    except httpx.ReadTimeout:
        # 读超时：请求已送达，后端可能已落库，回执未归。结果不确定，禁止补投。
        detail = (f"read timeout after {READ_TIMEOUT}s waiting ack from {active} "
                  f"(volume {expected_volume[:8]}); write may already be persisted "
                  "there, result UNCERTAIN; no automatic re-delivery to another node")
        await db.update_write_status(write_id, db.TIMEOUT_UNCERTAIN, detail)
        await db.add_event("WRITE_TIMEOUT_UNCERTAIN",
                           f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=504,
                            content={"id": write_id, "node": active,
                                     "status": db.TIMEOUT_UNCERTAIN,
                                     "uncertain": True, "detail": detail})
    except httpx.HTTPError as exc:
        detail = f"unexpected transport error against {active}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "detail": detail})
    finally:
        await _untrack(active, write_id)

    # 后端卷栅栏拦截：典型于"探测后、投递前被换卷"。写入没进新卷，也不补投。
    if resp.status_code == 409:
        try:
            fence = resp.json()
        except Exception:
            fence = {}
        detail = (f"volume fence rejected by {active}: expected "
                  f"{expected_volume[:8]}, actual volume "
                  f"{str(fence.get('actual_volume', '?'))[:8]}; "
                  "write not persisted anywhere, no re-delivery")
        await db.update_write_status(write_id, db.FAILED, detail)
        await db.add_event("WRITE_FENCED", f"{write_id}: {detail}")
        return JSONResponse(status_code=409,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "fenced": True,
                                     "detail": detail})

    if resp.status_code in (200, 201):
        try:
            ack = resp.json()
        except Exception:
            ack = {}
        # 闸门 3：响应复核——回执必须来自投递前快照的那个卷，否则不确认。
        if ack.get("volume_id") and ack["volume_id"] != expected_volume:
            detail = (f"ack from unexpected volume: expected {expected_volume[:8]}, "
                      f"got {str(ack.get('volume_id'))[:8]}; not marking acked")
            await db.update_write_status(write_id, db.FAILED, detail)
            await db.add_event("WRITE_FENCED", f"{write_id}: {detail}")
            return JSONResponse(status_code=409,
                                content={"id": write_id, "node": active,
                                         "status": db.FAILED, "fenced": True,
                                         "detail": detail})
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked by {active} volume {expected_volume[:8]} "
            f"(backend status={ack.get('status', 'acked')})",
            acked=True,
        )
        await db.add_event("WRITE_ACKED", f"{write_id} acked by {active}")
        return JSONResponse(status_code=201,
                            content={"id": write_id, "node": active,
                                     "status": db.ACKED, "backend": ack})

    # 后端明确拒绝（如故障态 500）：未受理，FAILED；同样不补投。
    detail = f"{active} rejected with HTTP {resp.status_code}: {resp.text[:300]}"
    await db.update_write_status(write_id, db.FAILED, detail)
    await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
    return JSONResponse(
        status_code=502,
        content={"id": write_id, "node": active, "status": db.FAILED, "detail": detail},
    )


@app.get("/api/writes/{write_id}/refresh")
async def refresh_write(write_id: str) -> JSONResponse:
    """向"原送达节点 + 原绑定卷"查询该写入的最新回执状态——切流/换卷后规则不变。"""
    row = await db.get_write(write_id)
    if row is None:
        return JSONResponse(status_code=404,
                            content={"error": f"unknown write {write_id}"})
    node = row["target_node"]
    bound_volume = row["volume_id"]
    base_url = await db.node_url(node)
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/writes/{write_id}",
            timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=10.0,
                                  write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        )
    except httpx.RequestError as exc:
        detail = f"refresh query to original node {node} failed: {type(exc).__name__}"
        await db.update_write_status(write_id, row["status"],
                                     f"{row['status_detail']} | {detail}")
        refreshed = await db.get_write(write_id)
        return JSONResponse(status_code=200,
                            content={"refreshed": False, "write": refreshed,
                                     "note": detail})

    try:
        data = resp.json()
    except Exception:
        data = {}
    observed_volume = data.get("volume_id")

    # 核心修复：应答方不是原绑定卷时，它的 404 不能作为核实结论。
    if bound_volume and observed_volume and observed_volume != bound_volume:
        note = (f"node {node} now serves foreign volume {observed_volume[:8]} "
                f"(bound original volume {bound_volume[:8]}); its "
                f"HTTP {resp.status_code} / 'no record' is NOT a verdict for write "
                f"{write_id}: original ownership and status preserved, retry after "
                "the original volume is restored")
        await db.add_event(
            "VERIFY_BLOCKED",
            f"{write_id}: foreign volume {observed_volume[:8]} cannot verify; "
            f"status stays {row['status']}",
        )
        return JSONResponse(content={
            "refreshed": False, "blocked": True, "reason": "foreign_volume",
            "write": row, "observed_volume_id": observed_volume,
            "bound_volume_id": bound_volume, "note": note,
        })

    if resp.status_code == 404:
        note = (f"original node {node} (volume {(observed_volume or bound_volume or '?')[:8]}) "
                f"has no row yet; keep status {row['status']} (do not re-deliver)")
        return JSONResponse(content={"refreshed": False, "write": row, "note": note})

    if data.get("status") == "acked" and row["status"] != db.ACKED:
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked confirmed later by original node {node} volume "
            f"{(bound_volume or observed_volume or '?')[:8]}", acked=True
        )
        await db.add_event("WRITE_RESOLVED",
                           f"{write_id} confirmed acked by original volume on node {node}")
    refreshed = await db.get_write(write_id)
    return JSONResponse(content={"refreshed": True, "write": refreshed,
                                 "backend_view": data})


# ---------------------------------------------------------------- 状态/排空

@app.get("/api/status")
async def status() -> JSONResponse:
    active = await db.get_active()
    node_rows = await db.list_nodes()
    probe_results = await asyncio.gather(
        *(probe_node(n["base_url"]) for n in node_rows)
    )
    health: dict[str, dict[str, Any]] = {}
    for n, (ok, detail, identity) in zip(node_rows, probe_results):
        health[n["name"]] = {
            "healthy": ok, "detail": detail, "base_url": n["base_url"],
            "state": n["state"], "quarantined": n["state"] == db.QUARANTINED,
            "volume_id": n["current_volume_id"],
            "generation": n["current_generation"],
            "instance_id": n["last_instance_id"],
            "observed_identity": identity,
            "volumes": await db.list_node_volumes(n["name"]),
        }
    writes = await db.list_writes()

    async with _inflight_lock:
        inflight_counts = {k: len(v) for k, v in _inflight.items()}
        inflight_ids = {k: sorted(v) for k, v in _inflight.items()}

    # 排空视角：已切走的旧节点上仍未拿到回执的写入。
    per_node: dict[str, dict[str, Any]] = {}
    for w in writes:
        slot = per_node.setdefault(
            w["target_node"], {"total": 0, "pending": 0, "acked": 0,
                               "timeout_uncertain": 0, "failed": 0}
        )
        slot["total"] += 1
        slot[w["status"].lower()] = slot.get(w["status"].lower(), 0) + 1
    for name in health:
        per_node.setdefault(name, {"total": 0, "pending": 0, "acked": 0,
                                   "timeout_uncertain": 0, "failed": 0})
    for name, slot in per_node.items():
        slot["inflight_now"] = inflight_counts.get(name, 0)
        slot["inflight_ids"] = inflight_ids.get(name, [])
        slot["is_active"] = name == active
        slot["state"] = health[name]["state"]
        slot["quarantined"] = health[name]["quarantined"]
        slot["volume_id"] = health[name]["volume_id"]
        slot["draining"] = name != active and (
            slot["inflight_now"] > 0 or slot["pending"] > 0
        )

    return JSONResponse(content={
        "ts": time.time(),
        "active": active,
        "active_quarantined": health.get(active, {}).get("quarantined", False),
        "nodes": health,
        "drainage": per_node,
        "writes": writes,
        "events": await db.list_events(),
        "timeouts": {"connect": CONNECT_TIMEOUT, "read": READ_TIMEOUT,
                     "probe": PROBE_TIMEOUT, "identity_poll": IDENTITY_POLL_INTERVAL},
    })


@app.get("/api/nodes/{name}/records")
async def node_records(name: str) -> JSONResponse:
    """透传某后端自己保存的写入记录（响应含该实例当前卷身份）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.get(
            f"{base_url}/records", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    return JSONResponse(content=resp.json())


@app.post("/api/nodes/{name}/fault")
async def node_fault(name: str, request: Request) -> JSONResponse:
    """给某后端注入/解除健康故障（透传后端控制接口）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    body = await request.json()
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/fault", json={"on": bool(body.get("on", True))},
            timeout=httpx.Timeout(PROBE_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    await db.add_event("FAULT_INJECTION",
                       f"node {name} fault on={bool(body.get('on', True))}")
    return JSONResponse(content=resp.json())


@app.post("/api/nodes/{name}/simulate-replace")
async def node_simulate_replace(name: str) -> JSONResponse:
    """演练：让该后端模拟"误挂空数据卷替换"（节点名/地址不变、健康检查仍通过）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/simulate-replace",
            timeout=httpx.Timeout(PROBE_TIMEOUT * 4),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    data = resp.json()
    change = await observe_node(name, base_url)
    if change:
        await log_identity_change(change)
    await db.add_event(
        "DRILL_REPLACE",
        f"node {name} simulated empty-volume replacement: "
        f"{str(data.get('old', {}).get('volume_id', '?'))[:8]} -> "
        f"{str(data.get('new', {}).get('volume_id', '?'))[:8]}",
    )
    return JSONResponse(content={"drill": "replace", "backend": data,
                                 "node": await db.get_node(name)})


@app.post("/api/nodes/{name}/simulate-restore")
async def node_simulate_restore(name: str) -> JSONResponse:
    """演练：让该后端从最近备份恢复原数据卷（验证隔离解除与旧回执核实恢复）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/simulate-restore",
            timeout=httpx.Timeout(PROBE_TIMEOUT * 4),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    if resp.status_code != 200:
        return JSONResponse(status_code=resp.status_code, content=resp.json())
    data = resp.json()
    change = await observe_node(name, base_url)
    if change:
        await log_identity_change(change)
    await db.add_event(
        "DRILL_RESTORE",
        f"node {name} simulated original-volume restore: "
        f"now on volume {str(data.get('current', {}).get('volume_id', '?'))[:8]}",
    )
    return JSONResponse(content={"drill": "restore", "backend": data,
                                 "node": await db.get_node(name)})


# ---------------------------------------------------------------- 控制台页面

@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)
