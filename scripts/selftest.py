#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好出现一次；
  7. 误挂空数据卷替换（节点名/地址不变、健康检查仍 200）：
     代理经卷身份对账识别并隔离该节点；旧写入归属/确认状态保留；
     新卷的"查无记录"不能作为核实结论；隔离节点不接新写入、不可切流；
  8. 原卷恢复：隔离自动解除，旧写入可继续投递/切流/核实旧回执；
  9. 同卷重启：volume_id 不变、generation+1，不触发隔离，路由与核实照常；
 10. 后端卷栅栏：X-Expected-Volume-Id 不符即 409 且不落库（防探测后换卷）；
 11. 在途写入遇换卷：在途状态不被改写、不补投，新写入被隔离闸门拒绝；
 12. 卷台账与持久化：可信原卷/异卷均登记，原卷数据在备份中未被破坏。

用法:
  python3 -I scripts/selftest.py [--python /path/to/venv/python]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30,
         headers: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id:
                if w["status"] == expect:
                    return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


def wait_node_state(proxy: str, node: str, expect_state: str,
                    expect_volume: str | None = None, timeout: float = 12) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        last = s["nodes"][node]
        if last["state"] == expect_state and (
            expect_volume is None or last["volume_id"] == expect_volume
        ):
            return last
        time.sleep(0.25)
    raise AssertionError(
        f"node {node} never reached state {expect_state}; last={json.dumps(last)[:300]}"
    )


def get_node(proxy: str, node: str) -> dict:
    _, s = http("GET", f"{proxy}/api/status")
    return s["nodes"][node]


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None

    def run(self):
        self.result = http("POST", f"{self.proxy}/api/writes?delay={self.delay}",
                           {"payload": self.payload}, timeout=30)
        if self.result:
            self.write_id = self.result[1].get("id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: dict[str, subprocess.Popen] = {}
    ports = {"proxy": 18000, "A": 18001, "B": 18002}
    proxy = f"http://127.0.0.1:{ports['proxy']}"
    backend_envs = {
        "A": {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")},
        "B": {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")},
    }

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs[kind] = p

    def kill(kind: str) -> None:
        p = procs.pop(kind, None)
        if p is None:
            return
        p.terminate()
        try:
            p.wait(timeout=8)
        except subprocess.TimeoutExpired:
            p.kill()

    def restart_backend(kind: str) -> None:
        # procs 以节点名（A/B）为键。
        kill(kind)
        time.sleep(0.4)
        spawn(kind, ROOT / "backend", backend_envs[kind], ports[kind])
        assert wait_ready(f"http://127.0.0.1:{ports[kind]}/healthz"), \
            f"backend {kind} did not come back"

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn("A", ROOT / "backend", backend_envs["A"], ports["A"])
        spawn("B", ROOT / "backend", backend_envs["B"], ports["B"])
        ok_a = wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
        ok_b = wait_ready(f"http://127.0.0.1:{ports['B']}/healthz")
        check(ok_a and ok_b, "两个后端启动并健康", f"A={ok_a} B={ok_b}")

        spawn("proxy", ROOT / "proxy", {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")
        check(st["nodes"]["A"]["state"] == "OK"
              and bool(st["nodes"]["A"]["volume_id"]),
              "卷身份已建立，节点初始状态 OK")
        original_volume_a = st["nodes"]["A"]["volume_id"]
        original_volume_b = st["nodes"]["B"]["volume_id"]

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")
        check(rec_a["volume_id"] == original_volume_a, "后端记录携带卷身份")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        # 等代理把 PENDING 行落库（请求已送达 A、回执未归）
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A", f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        # 故障态的 503 响应仍带卷身份，不得被误判成换卷隔离。
        check(get_node(proxy, "A")["state"] == "OK",
              "健康故障注入不改变卷身份与节点状态（未误隔离）")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id = tout["id"]
        timeout_node = tout["node"]
        _, s3 = http("GET", f"{proxy}/api/status")
        row = next(w for w in s3["writes"] if w["id"] == timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")

        # 直接打开后端 SQLite 核对持久化（后端各自保存写入的硬性要求）
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")

        # ============== 卷身份/隔离/恢复（本次故障修复的新增场景） ==============

        print("\n== 场景 7：A 误挂空数据卷替换（名址不变、健康仍 200）-> 识别并隔离 ==")
        # 现役先回到 A，并准备三类旧写入：已回执 / 超时未决 / 稍后用于在途观察。
        if http("GET", f"{proxy}/api/status")[1]["active"] != "A":
            code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
            check(code == 200, "切回 A 作为故障演练的现役节点")
        code, old_acked = http("POST", f"{proxy}/api/writes?delay=0",
                               {"payload": "pre-replace-acked"})
        check(code == 201, "替换前 A 上再写一条并拿到回执",
              f"id={old_acked.get('id','')[:8]}")
        old_acked_id = old_acked["id"]
        # 一条超时未决写入（卷里其实已落库），用于验证"新卷查无记录不作结论"。
        code, old_tout = http("POST", f"{proxy}/api/writes?delay=5",
                              {"payload": "pre-replace-uncertain"}, timeout=15)
        check(code == 504 and old_tout["status"] == "TIMEOUT_UNCERTAIN",
              "替换前制造一条 TIMEOUT_UNCERTAIN 写入")
        old_tout_id = old_tout["id"]
        # 等它在旧卷上的慢回执落定（代理虽超时，后端仍会写 acked_at），
        # 这样随后备份/恢复的原卷快照里它是已回执状态，恢复后可直接核实为 ACKED。
        deadline = time.time() + 8
        while time.time() < deadline:
            _, direct = http("GET", f"http://127.0.0.1:{ports['A']}/writes/{old_tout_id}")
            if direct.get("status") == "acked":
                break
            time.sleep(0.3)
        check(direct.get("status") == "acked"
              and direct.get("volume_id") == original_volume_a,
              "换卷前：超时写入已在原卷完成慢回执（仅代理侧仍标记不确定）")

        # 在途慢写入：换卷发生在它投递成功之后、回执之前。
        inflight = SlowPost(proxy, 4.0, "inflight-at-replace")
        inflight.start()
        deadline = time.time() + 5
        inflight_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for wrow in s["writes"]:
                if wrow["status"] == "PENDING":
                    inflight_id = wrow["id"]
                    break
            if inflight_id:
                break
            time.sleep(0.1)
        check(bool(inflight_id), "在途写入已投递并进入 PENDING")

        print("   触发后端 A 模拟空卷替换（先备份旧卷，再初始化新空卷）……")
        code, rep = http("POST", f"{proxy}/api/nodes/A/simulate-replace", {})
        check(code == 200 and rep["backend"]["new"]["volume_id"]
              != rep["backend"]["old"]["volume_id"],
              "后端 A 已换成新空卷（volume_id 改变，节点名/地址不变）")
        new_volume_a = rep["backend"]["new"]["volume_id"]
        check(rep["backend"]["new"]["generation"] == 1, "新空卷代次从 1 重新计")

        node_a = wait_node_state(proxy, "A", "QUARANTINED", new_volume_a)
        check(node_a["quarantined"] is True and node_a["healthy"] is True,
              "代理经卷身份对账识别替换：健康仍 200 但状态为 QUARANTINED")
        _, s7 = http("GET", f"{proxy}/api/status")
        check(any(e["kind"] == "VOLUME_REPLACE" for e in s7["events"]),
              "事件日志记录 VOLUME_REPLACE")
        check(s7["active_quarantined"] is True, "现役节点隔离状态在 /api/status 暴露")
        vols_a = {v["volume_id"]: v["trusted"] for v in node_a["volumes"]}
        check(vols_a.get(original_volume_a) == 1 and vols_a.get(new_volume_a) == 0,
              "卷台账同时保留可信原卷与异卷（异卷 trusted=0）")

        # 7.1 旧写入的归属与确认状态原样保留，不被新实例"接管"或改写。
        w_acked = next(w for w in s7["writes"] if w["id"] == old_acked_id)
        check(w_acked["target_node"] == "A" and w_acked["status"] == "ACKED"
              and w_acked["volume_id"] == original_volume_a,
              "旧已回执写入：仍绑定 A/原卷，ACKED 状态保留")
        w_tout = next(w for w in s7["writes"] if w["id"] == old_tout_id)
        check(w_tout["status"] == "TIMEOUT_UNCERTAIN"
              and w_tout["volume_id"] == original_volume_a,
              "旧超时未决写入：归属与不确定状态保留，未被新实例记录覆盖")
        w_inf = next(w for w in s7["writes"] if w["id"] == inflight_id)
        check(w_inf["target_node"] == "A" and w_inf["volume_id"] == original_volume_a,
              "在途写入仍绑定原卷，不向新卷/另一节点补投")

        # 7.2 新卷确实没有旧写入；但它的"查无记录"不能作为核实结论。
        _, rec_a_new = http("GET", f"{proxy}/api/nodes/A/records")
        check(rec_a_new["volume_id"] == new_volume_a
              and not any(r["id"] == old_acked_id for r in rec_a_new["records"]),
              "新空卷上没有任何旧写入（故障现象复现）")
        code, blocked_verify = http("GET", f"{proxy}/api/writes/{old_tout_id}/refresh")
        check(code == 200 and blocked_verify.get("blocked") is True
              and blocked_verify["reason"] == "foreign_volume"
              and blocked_verify["write"]["status"] == "TIMEOUT_UNCERTAIN",
              "向旧实例核实回执：新卷 404/查无记录被拦截，不作结论，状态原样保留")
        code, blocked_verify2 = http("GET", f"{proxy}/api/writes/{old_acked_id}/refresh")
        check(blocked_verify2.get("blocked") is True,
              "已回执写入同样拒绝由异卷核实（只认真正的原绑定卷）")

        # 7.3 隔离节点不得接收新写入（健康检查通过也不行）。
        code, blocked_write = http("POST", f"{proxy}/api/writes?delay=0",
                                   {"payload": "must-not-land-on-new-volume"})
        check(code == 503 and blocked_write.get("quarantined") is True,
              "对隔离节点 A 的新写入被 503 拒绝", blocked_write.get("detail", "")[:80])
        _, rec_a_after = http("GET", f"{proxy}/api/nodes/A/records")
        check(all(r["payload"] != "must-not-land-on-new-volume"
                  for r in rec_a_after["records"]),
              "被拒写入没有落进新卷")

        # 7.4 隔离节点不得成为切流目标。
        code, blocked_switch = http("POST", f"{proxy}/api/switch", {"target": "A"})
        # 若 A 仍是现役则先切 B，再验证切回 A 被拒。
        if blocked_switch.get("noop"):
            code, swb = http("POST", f"{proxy}/api/switch", {"target": "B"})
            check(code == 200 and swb["active"] == "B",
                  "A 隔离期间可把流量切到健康备用节点 B")
            code, blocked_switch = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked_switch.get("quarantined") is True
              and blocked_switch.get("active") == "B",
              "切向隔离节点 A 被 409 拦截，路由保持在 B", blocked_switch.get("reason", "")[:80])

        # 7.5 备用 B 不受影响，可正常写入；在途写入收尾后状态不被新卷篡改。
        code, wb = http("POST", f"{proxy}/api/writes?delay=0",
                        {"payload": "while-a-quarantined"})
        check(code == 201 and wb["node"] == "B", "隔离期间备用节点 B 正常承接新写入")
        inflight.join(timeout=12)
        check(inflight.is_alive() is False, "在途写入请求已结束")
        _, s7b = http("GET", f"{proxy}/api/status")
        w_inf2 = next(w for w in s7b["writes"] if w["id"] == inflight_id)
        check(w_inf2["volume_id"] == original_volume_a and w_inf2["status"] in
              ("PENDING", "TIMEOUT_UNCERTAIN", "FAILED"),
              "在途写入未被改绑新卷（状态未被异卷确认）",
              f"status={w_inf2['status']}")

        print("\n== 场景 8：原卷恢复 -> 自动解除隔离，投递/切流/旧回执核实恢复 ==")
        # 先等旧卷上的慢回执全部结束，恢复后核实应直接得到 ACKED。
        time.sleep(3.5)
        code, res = http("POST", f"{proxy}/api/nodes/A/simulate-restore", {})
        check(code == 200 and res["backend"]["current"]["volume_id"]
              == original_volume_a, "后端 A 从备份恢复了原数据卷")
        node_a2 = wait_node_state(proxy, "A", "OK", original_volume_a)
        check(node_a2["quarantined"] is False, "原卷回来后代理自动解除隔离")
        _, s8 = http("GET", f"{proxy}/api/status")
        check(any(e["kind"] == "VOLUME_RECOVERED" for e in s8["events"]),
              "事件日志记录 VOLUME_RECOVERED")

        # 旧写入的回执核实恢复：超时写入由原卷确认 ACKED。
        code, ref_tout = http("GET", f"{proxy}/api/writes/{old_tout_id}/refresh")
        check(code == 200 and ref_tout["write"]["status"] == "ACKED"
              and ref_tout["write"]["volume_id"] == original_volume_a,
              "原卷恢复后核实旧回执成功，超时写入就地更正为 ACKED")
        _, rec_a_restored = http("GET", f"{proxy}/api/nodes/A/records")
        restored_ids = {r["id"] for r in rec_a_restored["records"]}
        check(old_acked_id in restored_ids and old_tout_id in restored_ids
              and rec_a_restored["volume_id"] == original_volume_a,
              "原卷上的历史写入完整回来（归属未变）")

        # 恢复后可继续投递与切流。
        code, sw_back = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw_back["active"] == "A", "恢复后切流回 A 成功")
        code, w_after = http("POST", f"{proxy}/api/writes?delay=0",
                             {"payload": "after-restore"})
        check(code == 201 and w_after["node"] == "A", "恢复后 A 重新承接新写入")
        code, ref_acked = http("GET", f"{proxy}/api/writes/{old_acked_id}/refresh")
        check(ref_acked["write"]["status"] == "ACKED",
              "恢复后旧已回执写入仍可正常核实")

        print("\n== 场景 9：同卷重启 -> volume_id 不变、generation+1，不隔离 ==")
        gen_before = get_node(proxy, "B")["generation"]
        restart_backend("B")
        # 必须等到代理后台对账到"代次 +1"，而不能只看状态仍为 OK（重启前后都是 OK）。
        deadline = time.time() + 12
        node_b = {}
        while time.time() < deadline:
            node_b = get_node(proxy, "B")
            if (node_b["volume_id"] == original_volume_b
                    and node_b["generation"] == gen_before + 1):
                break
            time.sleep(0.25)
        check(node_b.get("volume_id") == original_volume_b
              and node_b.get("generation") == gen_before + 1,
              "同卷重启：卷身份不变、代次 +1",
              f"gen {gen_before} -> {node_b.get('generation')}")
        _, s9 = http("GET", f"{proxy}/api/status")
        check(any(e["kind"] == "VOLUME_RESTART" for e in s9["events"]),
              "事件日志记录 VOLUME_RESTART 而非 REPLACE")
        code, wb2 = http("POST", f"{proxy}/api/writes?delay=0",
                         {"payload": "after-same-volume-restart"})
        check(code == 201, "同卷重启后写入投递正常")
        # B 上历史写入仍可核实。
        _, rec_b2 = http("GET", f"{proxy}/api/nodes/B/records")
        check(rec_b2["volume_id"] == original_volume_b
              and any(r["id"] == wb["id"] for r in rec_b2["records"]),
              "同卷重启后旧记录与卷身份都还在，回执核实不受影响")

        print("\n== 场景 10：后端卷栅栏 X-Expected-Volume-Id 不符即拒绝且不落库 ==")
        b_url = f"http://127.0.0.1:{ports['B']}"
        fence_payload = {"id": f"fence-{uuid_arg()}", "payload": "fence-test"}
        code, fence_resp = http(
            "POST", f"{b_url}/writes?delay=0", fence_payload,
            headers={"X-Expected-Volume-Id": "wrong-volume-id-xyz"},
        )
        check(code == 409 and fence_resp.get("actual_volume") == original_volume_b,
              "错误卷身份被后端 409 拦截", fence_resp.get("error", "")[:70])
        _, rec_b3 = http("GET", f"{proxy}/api/nodes/B/records")
        check(not any(r["id"] == fence_payload["id"] for r in rec_b3["records"]),
              "栅栏拦截的写入没有落库（新卷也不会收到它）")
        code, ok_resp = http(
            "POST", f"{b_url}/writes?delay=0",
            {"id": f"fence-ok-{uuid_arg()}", "payload": "fence-pass"},
            headers={"X-Expected-Volume-Id": original_volume_b},
        )
        check(code == 201 and ok_resp.get("volume_id") == original_volume_b,
              "正确卷身份的写入正常受理，响应回带卷身份")

        print("\n== 场景 11：代理投递前闸门——换卷窗口内不盲投（fail-closed） ==")
        # A 当前是可信原卷；再次演练换卷后立刻连续投递，全部必须被拒绝，
        # 且任何写入都不能出现在新卷上（场景 7 已验证 503，这里补数量与落库核对）。
        code, rep2 = http("POST", f"{proxy}/api/nodes/A/simulate-replace", {})
        wait_node_state(proxy, "A", "QUARANTINED",
                        rep2["backend"]["new"]["volume_id"])
        refused = 0
        for i in range(3):
            code, r = http("POST", f"{proxy}/api/writes?delay=0",
                           {"payload": f"window-write-{i}"})
            refused += 1 if code in (409, 503) else 0
        check(refused == 3, "换卷后连续 3 次写入全部被闸门拒绝（无一次盲投）")
        _, rec_a_new2 = http("GET", f"{proxy}/api/nodes/A/records")
        check(all(not str(r.get("payload", "")).startswith("window-write")
                  for r in rec_a_new2["records"]),
              "窗口内写入无一落入新卷")

        print("\n== 场景 12：恢复收尾 + 备份/持久化核对 ==")
        code, _ = http("POST", f"{proxy}/api/nodes/A/simulate-restore", {})
        wait_node_state(proxy, "A", "OK", original_volume_a)
        backups = sorted(work.glob("a.db.backup-*"))
        check(len(backups) >= 2, f"旧卷备份保留在数据目录（{len(backups)} 份）")
        if backups:
            con = sqlite3.connect(backups[-1])
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            vid = con.execute("SELECT value FROM meta WHERE key='volume_id'").fetchone()[0]
            con.close()
            check(n > 0 and vid == original_volume_a,
                  f"备份卷内含 {n} 条旧写入且卷身份为原卷")
        # 代理库持久化：节点状态、卷台账、写入卷绑定都在 SQLite 里。
        con = sqlite3.connect(work / "proxy.db")
        con.row_factory = sqlite3.Row
        state_a = dict(con.execute(
            "SELECT state, current_volume_id FROM nodes WHERE name='A'").fetchone())
        n_vols = con.execute(
            "SELECT COUNT(*) c FROM node_volumes WHERE node='A'").fetchone()["c"]
        n_bound = con.execute(
            "SELECT COUNT(*) c FROM writes WHERE volume_id IS NOT NULL").fetchone()["c"]
        total_writes = con.execute("SELECT COUNT(*) c FROM writes").fetchone()["c"]
        con.close()
        check(state_a["state"] == "OK" and state_a["current_volume_id"]
              == original_volume_a and n_vols >= 2 and n_bound == total_writes,
              f"代理 SQLite 持久化：状态/台账({n_vols}卷)/全部写入卷绑定({n_bound})齐全")

    finally:
        print("\n== 清理进程 ==")
        for kind in list(procs):
            kill(kind)
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


def uuid_arg() -> str:
    import uuid
    return uuid.uuid4().hex[:8]


if __name__ == "__main__":
    sys.exit(main())
