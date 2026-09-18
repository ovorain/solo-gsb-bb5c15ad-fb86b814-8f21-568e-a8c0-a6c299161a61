# 乱序事件归并 API（Python · FastAPI · SQLite）

客户端按 `stream_id` 提交带有 `event_id`、`sequence`（从 0 开始递增）和整数 `delta`
的事件。服务端只把**从当前 `next_sequence` 起连续**的事件依次累加到余额；遇到缺号
时，后续事件先暂存。当缺失事件到达时，在**同一个数据库事务**内自动完成追赶
（catch-up）。

核心特性：

- **幂等重放**：相同 `event_id` 以相同内容重复提交返回 `200`，不会重复累加；
  内容不一致返回 `409 Conflict`。
- **序号唯一**：同一 `stream_id` 下每个 `sequence` 只能属于一个 `event_id`，
  重复序号（即使 `delta` 相同）返回 `409 Conflict`。
- **原子追赶**：缺失事件到达后，连续事件链的累加、`applied` 标记和
  `next_sequence`/`balance` 更新全部在同一事务内提交；冲突时整体回滚，已暂存
  事件不受影响。
- **并发安全**：SQLite WAL 模式 + `BEGIN IMMEDIATE`，并发提交串行化，每个序号
  恰好应用一次。
- **消费游标与租约**：消费者可领取（claim）从自身游标开始、已应用且连续的事件批，
  服务端发放带 `expires_at` 的 `lease_token`；租约未到期重复领取返回**同一批次、
  同一 token**，持 token 确认（ack）后游标原子推进；过期/旧 token 返回 `409`，
  租约到期后同批事件可被再次领取（at-least-once）。多个实例并发领取也只会存在
  一个有效租约。
- **流分叉（fork）**：把源流 `[0, through_sequence)` 的**已应用**事件整体复制为
  一个新的、可继续追加事件的目标流；`fork_id` 在同一源流下是幂等键，重试安全，
  分叉记录、目标流和复制事件在同一个 `BEGIN IMMEDIATE` 事务内提交，并发请求不会
  产生半成品。
- **跨事件流原子转账（transfer）**：`POST /transfers` 以全局幂等键 `transfer_id`
  从一个已有流扣款、向另一个已有流入账；扣款（`delta=-amount`）与入账
  （`delta=+amount`）两条事件分别追加在两侧当前 `next_sequence` 上，并在**同一个
  `BEGIN IMMEDIATE` 事务**内完成连续应用与余额更新，任一步失败整体回滚。相同
  `transfer_id` 携带相同参数重试返回原结果且不重复扣款，参数不一致返回 `409`，
  并发重试只提交一次。
- 无外部账号、无外部依赖，`docker compose up --build` 一条命令启动。

## 一键启动

```bash
docker compose up --build
```

服务监听 **8000** 端口，数据保存在命名卷 `events-data`（容器内 `/data`）中，
容器重启不丢数据。

健康检查：

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>

> 也可不使用 Docker 本地运行：
> `pip install -r requirements.txt && uvicorn app.main:app --host 0.0.0.0 --port 8000`

## 接口

### 1. 提交事件

```
POST /streams/{stream_id}/events
Content-Type: application/json
```

请求体：

| 字段       | 类型   | 约束                  | 说明                       |
| ---------- | ------ | --------------------- | -------------------------- |
| `event_id` | string | 非空，流内唯一        | 客户端分配的幂等键         |
| `sequence` | int    | `>= 0`，流内唯一      | 从 0 开始的单调递增序号    |
| `delta`    | int    | 整数                  | 事件应用时累加到余额的值   |

响应：

- `201 Created`：事件首次写入；
- `200 OK`：幂等重放（`replayed: true`）；
- `409 Conflict`：同一 `event_id` 内容不一致（`replay_conflict`），或
  `sequence` 已被其他 `event_id` 占用（`sequence_conflict`）；
- `422 Unprocessable Entity`：请求体校验失败。

### 2. 查询流状态

```
GET /streams/{stream_id}
```

```json
{
  "stream_id": "acct-1",
  "balance": 250,
  "next_sequence": 2,
  "pending_sequences": [3, 5]
}
```

- `balance`：已连续应用事件的累加余额；
- `next_sequence`：下一个可应用的序号（即连续前缀的长度）；
- `pending_sequences`：已暂存、但因缺号尚未应用的序号（升序）。

流不存在时返回 `404`。

### 3. 消费者领取事件（claim）

```
POST /streams/{stream_id}/consumers/{consumer_id}/claim
Content-Type: application/json
```

请求体：

| 字段        | 类型 | 约束              | 说明                                   |
| ----------- | ---- | ----------------- | -------------------------------------- |
| `max_events` | int  | `1 <= max_events <= 10000` | 本批最多返回多少事件                   |

从该消费者**游标**开始，返回已应用（applied）且连续的事件，最多 `max_events`
条；同时生成一个带 `expires_at`（POSIX 毫秒）的租约 `lease_token`。

- 租约**未到期**时重复领取（`consumer_id` 相同）始终返回同一批次、同一 token、
  同一 `expires_at`，忽略新的 `max_events`；
- 没有可消费事件时返回空批次（`events: []`，`from_sequence == to_sequence`），
  但同样发放租约；
- 游标之后若有事件尚未应用（缺号），本批只包含连续已应用部分；
- 流不存在返回 `404`，参数非法返回 `422`。

```json
{
  "stream_id": "acct-1",
  "consumer_id": "worker-a",
  "lease_token": "dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
  "expires_at": 1789676192280,
  "from_sequence": 0,
  "to_sequence": 2,
  "events": [
    {"event_id": "e0", "sequence": 0, "delta": 100},
    {"event_id": "e1", "sequence": 1, "delta": 200}
  ]
}
```

### 4. 消费者确认批次（ack）

```
POST /streams/{stream_id}/consumers/{consumer_id}/ack
Content-Type: application/json
```

请求体：`{"lease_token": "<claim 返回的 token>"}`。

只有 token **匹配当前未过期租约**时，才在同一个事务内把游标原子推进到
`to_sequence` 并清除租约：

```json
{"status": "ok", "stream_id": "acct-1", "consumer_id": "worker-a", "cursor": 2}
```

- `409 Conflict` 的情形：
  - `lease_not_active`：该消费者当前没有有效租约（尚未 claim，或已 ack）；
  - `lease_token_mismatch`：token 与当前租约不符（旧租约的 token、伪造 token）；
  - `lease_expired`：token 匹配的租约已过期。
- 租约到期后 token 失效（ack 返回 `409`），同批事件可被再次领取，新领取会得到
  新 token；
- 流不存在返回 `404`，请求体缺少 `lease_token` 返回 `422`。

不同 `consumer_id` 的游标与租约彼此独立；游标和租约都持久化在 SQLite 中。

### 5. 分叉流（fork）

```
POST /streams/{stream_id}/forks
Content-Type: application/json
```

请求体：

| 字段               | 类型   | 约束                                   | 说明                                   |
| ------------------ | ------ | -------------------------------------- | -------------------------------------- |
| `fork_id`          | string | 非空，源流内唯一                       | 客户端分配的幂等键（重试键）           |
| `target_stream_id` | string | 非空，必须是尚不存在的流               | 分叉创建的新流                         |
| `through_sequence` | int    | `>= 0` 且 `<=` 源流的 `next_sequence`  | 独占上界，复制 `[0, through_sequence)` |

把源流中序号位于 `[0, through_sequence)` 的**已应用**事件（`applied=1`）原样复制
到目标流：目标流的事件 `applied=1`，`next_sequence = through_sequence`，
`balance` 为这些事件 `delta` 之和。因缺号而暂存的事件（`pending_sequences`）不在
连续前缀内，不会被复制。目标流创建后即可用普通的提交事件接口从
`through_sequence` 继续追加。

```json
{
  "source_stream_id": "acct-1",
  "target_stream_id": "acct-1-backup",
  "through_sequence": 2,
  "balance": 300,
  "next_sequence": 2
}
```

- `201 Created`：分叉首次创建；
- `200 OK`：同一 `(源流, fork_id)` 携带完全相同参数的幂等重试，返回既有分叉；
- `409 Conflict`：
  - `target_stream_exists`：目标流已存在（含其他分叉已占用该 `target_stream_id`）；
  - `fork_replay_conflict`：同一 `fork_id` 重试时 `target_stream_id` 或
    `through_sequence` 与首次不同；
- `422 Unprocessable Entity`：`through_sequence` 超出 `[0, next_sequence]`
  （`through_sequence_out_of_range`），或请求体字段校验失败；
- `404 Not Found`：源流不存在（`stream_not_found`），此时不会创建任何目标流。

分叉记录、目标流行与复制的事件在**同一个 `BEGIN IMMEDIATE` 事务**内写入：并发的
相同 `fork_id` 请求只有一个创建分叉，其余全部拿到既有结果；并发的不同请求若争抢
同一目标流，只有一个成功，其余得到 `409`，数据库中不会残留半成品目标流。

### 6. 跨事件流原子转账（transfer）

```
POST /transfers
Content-Type: application/json
```

请求体：

| 字段               | 类型   | 约束                       | 说明                             |
| ------------------ | ------ | -------------------------- | -------------------------------- |
| `transfer_id`      | string | 非空，全局唯一             | 客户端分配的全局幂等键（重试键） |
| `source_stream_id` | string | 非空，必须是**已存在**的流 | 扣款流                           |
| `target_stream_id` | string | 非空，必须是**已存在**的流 | 入账流，且必须与源流不同         |
| `amount`           | int    | 正整数（`> 0`）            | 转账金额                         |

转账在**同一个 `BEGIN IMMEDIATE` 事务**内完成：

1. 在源流当前 `next_sequence` 追加一条扣款事件（`delta = -amount`，事件 id 为
   `transfer:<transfer_id>:debit`）；
2. 在目标流当前 `next_sequence` 追加一条入账事件（`delta = +amount`，事件 id 为
   `transfer:<transfer_id>:credit`）；
3. 两侧分别执行普通的连续追赶（catch-up）——若追加位置之前还有因缺号暂存的事件链，
   这次追加也会把它们一并连续应用；
4. 更新两侧的 `balance`/`next_sequence` 并写入 `transfers` 转账记录，统一提交。

任一步失败（流不存在、余额不足、约束冲突等）整体回滚，**不会留下任何事件或余额
变化**。转账事件就是普通事件：随后可以通过 `GET /streams/{id}`、消费 claim/ack、
fork 等既有接口正常观察和处理。

- `201 Created`：转账首次提交；
- `200 OK`：相同 `transfer_id` 携带**完全相同参数**的幂等重试，返回原扣款/入账
  事件序号与两侧**最新**流状态（重试不会再次扣款）；
- `409 Conflict`：`transfer_replay_conflict`——相同 `transfer_id` 重试时源流、
  目标流或 `amount` 与首次不一致；
- `422 Unprocessable Entity`：`insufficient_funds`——源流当前 `balance`（连续前缀
  余额，不含暂存事件）小于 `amount`；或请求体字段校验失败（金额非正整数、源流与
  目标流相同等）；
- `404 Not Found`：`source_stream_not_found` / `target_stream_not_found`——对应流
  不存在（转账不会自动创建流）。

并发的相同 `transfer_id` 请求在写锁上串行，数据库中只有一条 `transfers` 记录、
两侧各只有一条转账事件：恰好一个请求得到 `201`，其余全部拿到 `200` 原结果。

```json
{
  "status": "ok",
  "replayed": false,
  "transfer_id": "txn-1",
  "amount": 50,
  "source": {
    "stream_id": "acct-1",
    "event_id": "transfer:txn-1:debit",
    "sequence": 3,
    "state": {"stream_id": "acct-1", "balance": 50, "next_sequence": 4, "pending_sequences": []}
  },
  "target": {
    "stream_id": "acct-2",
    "event_id": "transfer:txn-1:credit",
    "sequence": 1,
    "state": {"stream_id": "acct-2", "balance": 50, "next_sequence": 2, "pending_sequences": []}
  }
}
```

## 工作机制示例

```bash
# 序号 1 先到：缺 0，暂存，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","sequence":1,"delta":200}'
# 201 -> balance=0, next_sequence=0, pending=[1]

# 序号 2 再到：仍然暂存
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e2","sequence":2,"delta":50}'
# 201 -> balance=0, next_sequence=0, pending=[1,2]

# 序号 0 到达：同一事务内自动追赶 0 -> 1 -> 2
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 201 -> balance=350, next_sequence=3, pending=[]

# 相同 event_id 重放：幂等，返回 200，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 200 -> replayed=true, balance=350, next_sequence=3

# 同一 event_id 内容不一致：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":999}'
# HTTP/1.1 409
# {"detail":{"code":"replay_conflict","message":"event_id already exists with different sequence/delta"}}

# 序号 0 被另一个 event_id 占用：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"other","sequence":0,"delta":100}'
# HTTP/1.1 409, code=sequence_conflict

# 查询
curl -s localhost:8000/streams/acct-1
```

## 消费游标示例（claim / ack）

```bash
# 先准备三个已应用事件（接上文，acct-1 的 next_sequence=3）
# 消费者 worker-a 领取最多 2 个事件
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' \
  -d '{"max_events":2}'
# {
#   "stream_id":"acct-1","consumer_id":"worker-a",
#   "lease_token":"dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
#   "expires_at":1789676192280,"from_sequence":0,"to_sequence":2,
#   "events":[{"event_id":"e0","sequence":0,"delta":100},
#             {"event_id":"e1","sequence":1,"delta":200}]
# }

# 租约未到期重复领取：同一批次、同一 token、同一 expires_at
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":99}'
# -> lease_token 不变，events 仍是 [e0, e1]

# 用错误/旧 token 确认：409
curl -i -X POST localhost:8000/streams/acct-1/consumers/worker-a/ack \
  -H 'Content-Type: application/json' -d '{"lease_token":"bogus"}'
# HTTP/1.1 409 -> {"detail":{"code":"lease_token_mismatch", ...}}

# 用有效 token 确认：游标原子推进到 2
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/ack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi"}'
# {"status":"ok","stream_id":"acct-1","consumer_id":"worker-a","cursor":2}

# 再次领取：从游标 2 继续
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events=[e2]，from_sequence=2, to_sequence=3

# 租约到期后再用旧 token 确认：409 lease_expired；
# 重新 claim 可再次拿到同批事件（新 token）。
```

## 流分叉示例（fork）

```bash
# 接上文：acct-1 已有 3 个已应用事件（balance=350, next_sequence=3）。
# 把 [0, 2) 的已应用事件复制到新流 acct-1-backup：
curl -s -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"daily-backup","target_stream_id":"acct-1-backup","through_sequence":2}'
# 201:
# {"source_stream_id":"acct-1","target_stream_id":"acct-1-backup",
#  "through_sequence":2,"balance":300,"next_sequence":2}

# 目标流可以继续追加事件（序号从 next_sequence=2 开始）
curl -s -X POST localhost:8000/streams/acct-1-backup/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"b-e2","sequence":2,"delta":10}'
# 201 -> balance=310, next_sequence=3

# 相同 fork_id + 相同参数重试：幂等，返回 200 与既有分叉（不会重复复制事件）
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"daily-backup","target_stream_id":"acct-1-backup","through_sequence":2}'
# HTTP/1.1 200 OK

# 相同 fork_id 但参数不同：409 fork_replay_conflict
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"daily-backup","target_stream_id":"acct-1-backup","through_sequence":3}'
# HTTP/1.1 409

# 目标流已存在：409 target_stream_exists
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"another","target_stream_id":"acct-1-backup","through_sequence":1}'
# HTTP/1.1 409

# 上界超过源流的 next_sequence：422 through_sequence_out_of_range
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"oob","target_stream_id":"acct-1-oob","through_sequence":99}'
# HTTP/1.1 422

# 源流不存在：404，且不会创建目标流
curl -s -i -X POST localhost:8000/streams/unknown/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"f","target_stream_id":"ghost","through_sequence":0}'
# HTTP/1.1 404

# through_sequence=0：创建一个空的、可从头追加的目标流
curl -s -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"empty","target_stream_id":"acct-1-empty","through_sequence":0}'
# {"...","through_sequence":0,"balance":0,"next_sequence":0}
```

## 跨事件流转账示例（transfer）

```bash
# 准备两个已存在的流（沿用前文 acct-1：balance=350, next_sequence=3）
curl -s -X POST localhost:8000/streams/acct-2/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":0}'
# 201 -> acct-2 balance=0, next_sequence=1

# 从 acct-1 向 acct-2 转账 50：两侧各自在当前 next_sequence 追加事件
curl -s -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-1","source_stream_id":"acct-1","target_stream_id":"acct-2","amount":50}'
# 201:
# {"status":"ok","replayed":false,"transfer_id":"txn-1","amount":50,
#  "source":{"stream_id":"acct-1","event_id":"transfer:txn-1:debit",
#            "sequence":3,
#            "state":{"stream_id":"acct-1","balance":300,"next_sequence":4,"pending_sequences":[]}},
#  "target":{"stream_id":"acct-2","event_id":"transfer:txn-1:credit",
#            "sequence":1,
#            "state":{"stream_id":"acct-2","balance":50,"next_sequence":2,"pending_sequences":[]}}}

# 相同 transfer_id + 相同参数重试：幂等返回 200 与原事件序号，不重复扣款
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-1","source_stream_id":"acct-1","target_stream_id":"acct-2","amount":50}'
# HTTP/1.1 200 OK -> replayed=true, source.sequence 仍为 3, target.sequence 仍为 1

# 相同 transfer_id 但金额/流不一致：409 transfer_replay_conflict
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-1","source_stream_id":"acct-1","target_stream_id":"acct-2","amount":99}'
# HTTP/1.1 409

# 余额不足：422 insufficient_funds（事务整体回滚，两侧均无变化）
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-poor","source_stream_id":"acct-2","target_stream_id":"acct-1","amount":51}'
# HTTP/1.1 422

# 流不存在：404（转账不会自动建流）
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-404","source_stream_id":"acct-1","target_stream_id":"ghost","amount":1}'
# HTTP/1.1 404 -> {"detail":{"code":"target_stream_not_found", ...}}

# 源流与目标流相同 / amount 非正整数：422 参数校验失败
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"bad","source_stream_id":"acct-1","target_stream_id":"acct-1","amount":1}'
# 422

# 转账事件与普通事件无异：消费者可以领取到扣款/入账事件
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events 最后一项为 {"event_id":"transfer:txn-1:debit","sequence":3,"delta":-50}
```

## 数据模型

```text
streams(stream_id PK, balance, next_sequence)
events(stream_id, event_id, sequence, delta, applied,
       PK(stream_id, event_id), UNIQUE(stream_id, sequence))
forks(source_stream_id, fork_id, target_stream_id, through_sequence, balance,
      PK(source_stream_id, fork_id), UNIQUE(target_stream_id))
transfers(transfer_id PK, source_stream_id, target_stream_id, amount,
          source_sequence, target_sequence,
          CHECK(amount > 0), CHECK(source_stream_id <> target_stream_id))
consumers(stream_id, consumer_id, cursor_seq,
          lease_token, lease_expires_at, lease_from_seq, lease_to_seq,
          PK(stream_id, consumer_id))
```

- `UNIQUE(stream_id, event_id)` 保证重放幂等；
- `UNIQUE(stream_id, sequence)` 保证序号唯一；
- `applied=0` 表示暂存，`applied=1` 表示已计入余额；
- `transfers` 以全局 `transfer_id` 为主键记录每笔已提交转账的参数与两侧事件序号：
  重试按 `transfer_id` 比对参数，`source_sequence`/`target_sequence` 即原扣款/入账
  事件的追加位置；转账事件本身仍是 `events` 表中的普通行，事件 id 形如
  `transfer:<transfer_id>:debit|credit`，`events` 的两个唯一约束为其兜底；
- 每个 `(stream_id, consumer_id)` 一行：`cursor_seq` 为消费游标（下一个待领取
  的序号），租约四列要么整体为 NULL（无有效租约），要么整体非空。一行只持有一个
  租约，因此领取与确认都在 `BEGIN IMMEDIATE` 事务内串行执行——即使多个应用实例
  同时领取，数据库中也只会存在一个有效租约。

租约生命周期：

1. claim 时若存在未到期租约（`lease_expires_at > now`），直接返回该租约覆盖的
   原有批次（`lease_from_seq..lease_to_seq`）、原 token、原到期时间；
2. 否则从 `cursor_seq` 起取连续已应用事件（序号上界为流的 `next_sequence`），
   至多 `max_events` 条，生成新 token（`secrets.token_urlsafe`）和
   `expires_at = now + LEASE_DURATION_SECONDS`，写入租约四列并提交；
3. ack 在单个 `BEGIN IMMEDIATE` 事务内校验：存在流（否则 `404`）→ 存在租约
   （否则 `lease_not_active`）→ token 一致（否则 `lease_token_mismatch`）→
   未过期（否则 `lease_expired`）；全部通过后把游标推进到 `lease_to_seq` 并清空
   租约列，原子提交；
4. 租约到期不会自动删除：旧 token 无法 ack（`409`），下一次 claim 会发放新
   token，同一批事件可被重新消费。

提交事件的处理在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 若流不存在则创建（`balance=0, next_sequence=0`）；
2. 若 `event_id` 已存在：比对 `sequence`/`delta`，不一致即 `409`，一致则直接
   返回当前状态（幂等）；
3. 否则检查 `sequence` 是否已被占用，占用则 `409`，未占用则写入
   `applied=0` 的事件；
4. 从 `next_sequence` 开始循环取 `applied=0` 的连续事件累加 `delta`、标记
   `applied=1`，直到遇到缺号；
5. 更新 `balance` 与 `next_sequence`，提交事务。

分叉的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 锁定并读取源流：不存在直接返回 `404`（事务内不做任何写入）；
2. 按 `(source_stream_id, fork_id)` 查 `forks`：已存在则比对
   `target_stream_id`/`through_sequence`，不一致返回 `409 fork_replay_conflict`，
   一致则返回既有分叉（幂等重试，HTTP `200`）；
3. 校验 `0 <= through_sequence <= 源流 next_sequence`，越界返回
   `422 through_sequence_out_of_range`；
4. 确认目标流不存在（存在则 `409 target_stream_exists`，`streams.stream_id` 的
   主键与 `forks.target_stream_id` 的唯一约束共同兜底并发情况）；
5. 取源流 `applied=1` 且序号在 `[0, through_sequence)` 的事件（按序号升序），
   求和得到目标流初始 `balance`；插入目标流
   `(balance=和, next_sequence=through_sequence)`，把事件以 `applied=1` 原样复制，
   再写入 `forks` 分叉记录，统一提交——任一步失败整体回滚，不留半成品。

转账的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 按 `transfer_id` 查 `transfers`：已存在则比对源流、目标流、`amount`，不一致
   返回 `409 transfer_replay_conflict`，一致则回放原结果（`200`）——返回记录中的
   原始两侧事件序号，并重新读取两侧**当前**的流状态一并返回，不做第二次扣款；
2. 首次请求：读取并锁定源流与目标流行，任一不存在即 `404`
   （`source_stream_not_found` / `target_stream_not_found`），事务内无任何写入；
3. 校验源流当前 `balance >= amount`（余额只统计连续已应用前缀），不足返回
   `422 insufficient_funds`；
4. 在两侧当前 `next_sequence` 分别插入扣款（`-amount`）与入账（`+amount`）事件
   （`applied=0`，确定性事件 id `transfer:<transfer_id>:debit|credit`）；
5. 两侧分别从各自 `next_sequence` 起执行连续追赶：新事件（以及因此解除缺号的
   暂存事件链）依次累加并标记 `applied=1`，直到再次遇到缺号；
6. 更新两侧 `balance`/`next_sequence`，写入 `transfers` 记录
   （含两侧事件序号），统一提交——任一步失败整体回滚，事件与余额都不会残留。

并发重试时，多个相同 `transfer_id` 请求在 `BEGIN IMMEDIATE` 写锁上排队：第一个
拿到锁的请求完成插入并提交，其余请求随后读到既有记录，按幂等回放返回，因此
`transfers` 中只有一行、两侧各只有一条转账事件。

## 配置

| 环境变量                | 默认值                | 说明                          |
| ----------------------- | --------------------- | ----------------------------- |
| `APP_PORT`              | `8000`                | 服务监听端口                  |
| `DATABASE_PATH`         | `/data/events.db`     | SQLite 文件路径               |
| `LEASE_DURATION_SECONDS` | `30`                 | claim 租约有效时长（秒，> 0） |

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/test_api.py` 覆盖：顺序累加、缺号暂存与自动追赶、幂等重放、两种 409、
404、422 参数校验、大批量追赶以及 20 线程并发；消费者游标部分覆盖：领取连续批次、
缺号时只返回已应用事件、租约内重复领取同批同 token、ack 推进游标、错误/旧/过期
token 的三种 409、租约到期后同批重新领取、多消费者独立游标、空批次以及 20 线程
并发只产生一个有效租约；流分叉部分覆盖：整段/部分/空前缀复制、暂存事件不复制、
目标流可继续追加、源流 404、目标已存在 409、上界越界 422、相同参数重试 200 幂等、
参数漂移 409、请求体 422，以及 20 线程并发（同一 `fork_id` 只产生一个分叉；
不同 `fork_id` 争抢同一目标流时不留半成品）；跨流转账部分覆盖：正常扣款/入账与
响应字段、事件可被 claim 消费、恰好足额、余额不足 `422` 且无副作用、源流/目标流
缺失 `404`、相同参数重试 `200` 返回原序号且不重复扣款、重试时返回最新流状态、
参数漂移 `409`、请求体 `422`、暂存事件在转账时一并追赶、与 fork/消费者接口共存，
以及 20 线程并发（同一 `transfer_id` 只提交一次；50 笔不同转账串行提交且总额守恒）。

仅用 Python 标准库验证核心归并算法（无需安装 FastAPI）：

```bash
python3 verify_core.py
python3 verify_forks.py
python3 verify_transfers.py
```

## 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── main.py        # FastAPI 路由与请求/响应模型
│   ├── repo.py        # 归并/追赶算法（纯 sqlite3，事务边界清晰）
│   ├── forks.py       # 流分叉（fork）幂等事务
│   ├── transfers.py   # 跨事件流原子转账（transfer）幂等事务
│   ├── consumers.py   # 消费游标与 claim/ack 租约
│   ├── db.py          # 连接管理、WAL、schema 初始化
│   └── config.py      # 环境变量配置
├── tests/test_api.py
├── verify_core.py
├── verify_forks.py
├── verify_transfers.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
