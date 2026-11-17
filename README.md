# 写入切流反向代理（FastAPI + SQLite）

值班工程师更新服务节点时，用本系统把写入流量从旧节点切到备用节点。核心保证：

- **不中断已送达的写入**：每个写入在投递前快照现役节点**及其数据卷身份**并持久化绑定，
  切流只改路由表，不取消在途请求；回执仍从**原送达节点的原绑定卷**读取，连续多次切流
  也不打断它。
- **不重复投递**：代理对任何写入只发一次。连接失败标 `FAILED`、读超时标
  `TIMEOUT_UNCERTAIN`，都**不会**自动改投另一节点；后端两侧写入 ID 集合永不相交。
- **探测失败保持旧路由**：切流前先探目标节点 `/healthz`，失败返回 409，路由表不变。
- **超时显式不确定**：写入已在后端落库但回执超时（读超时）时返回 504 +
  `TIMEOUT_UNCERTAIN`，页面醒目提示，可事后点"向原节点/原卷查询"核实，由原卷就地更正状态。
- **误挂空卷/异卷可识别、可隔离、可恢复**：每个数据卷有终身 `volume_id`（存在后端
  SQLite 里），代理周期对账卷身份。节点名与地址不变、健康检查仍通过，但卷身份变成
  未登记的异卷/空卷时，节点进入 `QUARANTINED`：不接新写入、不可切流；旧写入的归属与
  确认状态原样保留；向旧实例核实回执时，新卷的"查无记录"**不作结论**。原卷挂回后
  自动解除隔离，投递、切流、旧回执核实全部恢复。同卷重启（卷身份不变、代次 +1）不隔离。

## 故障模型与修复原理

事故场景：值班工程师更换节点 A 容器时误挂了一个**空数据卷**，节点名与地址不变，
新实例 `/healthz` 照样 200；旧卷上已确认的写入在代理视角里被错认成"新实例的记录"，
向它核实回执时新卷查无记录，可能被误判为写入不存在。

修复采用"**卷身份 + 代次 + 三重栅栏 + 原卷核实**"：

1. 后端每个数据卷首次初始化时在库内 `meta` 表生成终身 `volume_id`，进程每次启动
   （或重新挂载卷）`generation += 1`；另有进程内 `instance_id` 区分进程实例。
2. 代理 `nodes` 表记录节点当前卷身份与状态（`OK` / `QUARANTINED`），`node_volumes`
   台账登记该节点见过的所有卷：首次见到的卷标记为可信原卷，其余为异卷。
3. 代理后台每 `IDENTITY_POLL_INTERVAL` 秒（默认 2s）探测并对账，启动与切流/投递前
   还会即时对账一次：
   - 卷身份变为未登记异卷/空卷 → `QUARANTINED`，事件 `VOLUME_REPLACE`；
   - 可信原卷重新挂回 → 自动解除隔离，事件 `VOLUME_RECOVERED`；
   - 同卷重启（卷身份不变）→ 仅记 `VOLUME_RESTART`，不隔离。
4. 写入三重栅栏，堵住"探测之后、实际投递之前换卷"的窗口：
   - 投递前再探一次取卷身份快照，取不到身份 **fail-closed** 拒绝投递；
   - 请求头带 `X-Expected-Volume-Id`，后端发现卷不符即 **409 拒绝且绝不落库**；
   - 响应回来再复核回执中的卷身份，不符不确认回执。
5. 回执核实（`/api/writes/{id}/refresh`）只认写入绑定的原卷：应答方卷身份不符时，
   它的 404/查无记录被显式拦截（`blocked=true, reason=foreign_volume`），原状态保留。

## 目录结构

```
backend/            模拟写入后端（两个实例，各自 SQLite 保存收到的写入与卷身份）
  app/main.py       卷身份 meta 表、代次、健康故障 /control/fault、卷栅栏、
                    /control/simulate-replace、/control/simulate-restore 演练接口
  Dockerfile
proxy/              FastAPI 反向代理 + 切流控制台
  app/main.py       代理、切流、探测、卷身份对账/隔离、投递卷栅栏、在途/排空跟踪
  app/db.py         SQLite 持久层（nodes / node_volumes / route / writes / events）
  app/dashboard.html 切流网页（卷身份、隔离状态、演练按钮、写入与事件）
  Dockerfile
docker-compose.yml  一键启动 proxy + backend-a + backend-b（含健康检查与数据卷）
scripts/selftest.py 纯标准库端到端自测（76 项断言）
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 控制台： http://localhost:8080/
```

- 后端各自把写入与卷身份保存在卷 `backend-a-data` / `backend-b-data` 的
  `/data/writes.db`；代理状态在卷 `proxy-data` 的 `/data/proxy.db`。
- 想演练回执超时，可把 compose 里 `WRITER_READ_TIMEOUT` 调小（如 `2`）后重建。
- 代理库支持旧版本平滑升级：旧库启动时自动补列，历史写入（无卷绑定）保留可用。

## 不用 Docker 的本地运行（开发/自测）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r proxy/requirements.txt        # 后端依赖相同
mkdir -p .run/data
NODE_NAME=A BACKEND_DB=$PWD/.run/data/a.db \
  python -m uvicorn app.main:app --app-dir backend --port 8001 &
NODE_NAME=B BACKEND_DB=$PWD/.run/data/b.db \
  python -m uvicorn app.main:app --app-dir backend --port 8002 &
NODE_A_NAME=A NODE_B_NAME=B \
BACKEND_A_URL=http://127.0.0.1:8001 BACKEND_B_URL=http://127.0.0.1:8002 \
PROXY_DB=$PWD/.run/data/proxy.db \
  python -m uvicorn app.main:app --app-dir proxy --port 8080
```

## 网页操作演练

1. 慢回执延迟填 `10` 秒，点"投递写入"，在请求返回前立刻点另一节点的"切换流量"；
2. 观察：现役节点立即变更；旧节点卡片显示 **DRAINING 排空中** 与在途写入 ID；
   慢请求结束后仍由旧节点回执，写入只存在于旧节点；
3. 点某节点"注入健康故障"后再向它切流：收到 409 拦截提示，现役节点不变；
4. **空卷替换演练**：点节点 A 的"演练：空卷替换"（后端先备份旧卷再初始化新空卷，
   节点名/地址不变、健康仍 200）。约 2 秒内 A 出现红色 **QUARANTINED 已隔离**：
   - 对 A 的新写入返回 503，切流向 A 返回 409；可把流量切到健康的 B；
   - 旧写入行仍显示原节点/原卷与原状态；点"向原节点/原卷查询"会提示异卷不能核实；
   - 点"演练：恢复原卷"，A 自动回到 **OK**，旧回执可核实、可继续投递与切流。
5. "拉取 A/B 的记录"可对比两个后端各自 SQLite 中的写入与当前卷身份。

## HTTP 接口（代理 8080）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/writes?delay=秒` | 投递写入（代理只投一次，绑定节点 + 卷身份；隔离节点 503） |
| POST | `/api/switch` | body `{"target":"B"}`；探测失败或目标隔离返回 409 并保持旧路由 |
| GET  | `/api/status` | 现役节点、节点状态/卷身份/卷台账、健康、排空/在途、写入与事件 |
| GET  | `/api/writes/{id}/refresh` | 向**原送达节点的原绑定卷**查询回执；异卷作答时拦截，不作结论 |
| GET  | `/api/nodes/{name}/records` | 透传该后端自己保存的写入记录（含当前卷身份） |
| POST | `/api/nodes/{name}/fault` | 注入/解除健康故障 `{"on": true|false}` |
| POST | `/api/nodes/{name}/simulate-replace` | 演练：备份旧卷后挂接全新空卷（模拟误操作） |
| POST | `/api/nodes/{name}/simulate-restore` | 演练：从最近备份恢复原数据卷 |

后端直接接口：`GET /healthz`（响应含 `volume_id/generation/instance_id`）、
`POST /writes`（头 `X-Expected-Volume-Id` 不符即 409 不落库）、
`GET /writes/{id}`、`GET /records`、`POST /control/fault`、
`POST /control/simulate-replace`、`POST /control/simulate-restore`。

## 自测

```bash
python -I scripts/selftest.py            # 自动拉起 3 个进程，跑完即清理
```

覆盖（76 项断言）：普通写入无重复、慢回执在途切流、探测失败保旧路由、连续再次切流、
读超时 504 不确定且不补投（事后向原节点核实可转 ACKED）、两侧后端记录不相交；
以及本次故障修复：空卷替换识别与隔离、旧写入归属/确认状态保留、异卷"查无记录"
不作结论、隔离节点不接新写入且不可切流、原卷恢复后投递/切流/核实全部恢复、
同卷重启不隔离、后端卷栅栏拒绝误投且不落库、换卷窗口 fail-closed、卷台账与 SQLite
持久化、代理重启后隔离状态保留、旧版代理库平滑迁移。

## 关键环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `WRITER_READ_TIMEOUT` | 30 | 等回执读超时秒数，超时即结果不确定 |
| `CONNECT_TIMEOUT` | 3 | 连接超时，连接失败视为未送达 `FAILED` |
| `PROBE_TIMEOUT` | 2.5 | 切流健康探测超时 |
| `IDENTITY_POLL_INTERVAL` | 2 | 卷身份后台对账周期秒数（换卷识别上限） |
| `BACKEND_A_URL` / `BACKEND_B_URL` | 本地 8001/8002 | 两个后端地址 |
