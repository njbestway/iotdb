# Apache IoTDB 2.0.x 2-Node Active-Active Control Plane

## Implementation SPEC V1.2

---

## 0. 项目定位

本项目基于 Apache IoTDB 已有能力构建一个 **轻量级 HA Monitor**，以 IoTDB 工程内的 **独立 Maven 子模块** 形式存在。

```
IoTDB 已提供（Data Plane）：
  Data Storage + Pipe + Double Living + Progress + Auto Restart

本项目提供（iotdb-ha 子模块）：
  聚合集群视图（SHOW CLUSTER 增强）
  Node Health Check
  Pipe/Channel Monitor
  Replication Lag Alert
  Offline Node Recovery Helper
  Prometheus Metrics
```

```
┌─────────────────────────────────────────────┐
│  iotdb-ha（独立 Maven 子模块，Java）         │
│                                             │
│  聚合集群视图 · Node Health · Pipe Status    │
│  Replication Lag · Alert · Prometheus        │
│                                             │
│  运行模式（二选一）：                         │
│  A. 独立进程（默认）                          │
│  B. 嵌入 ConfigNode（可选，加 1-2 行代码）    │
└──────────────────────┬──────────────────────┘
                       │ JDBC
           ┌───────────┴───────────┐
           ▼                       ▼
     ┌───────────┐          ┌───────────┐
     │  IoTDB A  │          │  IoTDB B  │
     │   1C1D    │          │   1C1D    │
     └─────┬─────┘          └─────┬─────┘
           │                       │
           │   Double-Living Pipe  │
           └──────────↔────────────┘

     ┌─────────────────────────────────┐
     │  VIP / Client Dual-IP           │
     │  （写入路由，不依赖本项目）        │
     └─────────────────────────────────┘
```

IoTDB 版本要求：**Apache IoTDB 2.0.x，最低验证版本 2.0.8+**。

---

## 1. 核心假设（不可违反）

| ID | 假设 | 含义 |
|----|------|------|
| A1 | 单写源 | 同一时刻只有一个节点接收写入（VIP 或客户端选择） |
| A2 | 无并发双写 | 不存在两个客户端同时向不同节点写同一设备 |
| A3 | 数据幂等 | IoTDB 同设备同时间戳 INSERT = 覆盖（upsert），重复数据不膨胀 |
| A4 | 节点故障 = 网络不通 | 不是磁盘损坏，数据持久化（WAL + TsFile）可靠 |
| A5 | Pipe 故障 ≠ 节点故障 | Pipe 异常不触发节点切换 |
| A6 | 数据时间由客户端产生 | 传感器/PLC 产生数据和时间戳，服务端不生成时间 |
| A7 | 重复数据值相同或可接受 | 客户端重发的数据值相同，或微小差异在传感器精度范围内 |

---

## 2. 两种写入模式

### 模式 1：VIP 写入

```
正常：  Client → VIP → A
漂移：  VIP → B（A 不可达时）
恢复：  VIP → A（A 恢复后）
```

- 写入整体切换，不是按设备分片
- VIP 漂移由基础设施层（Keepalived / 云 VIP）管理
- 本项目不参与 VIP 管理

### 模式 2：客户端双 IP

```
正常：  Client → A
故障：  Client → B（A 不可达）
恢复：  Client → A
```

- 客户端自行判断连通性
- 切换粒度也是整体

### 共同点

| 特征 | 说明 |
|------|------|
| 单写源 | 同一时刻只有一个节点接收写入 |
| 整体切换 | 不是按设备分配 Owner |
| 切换由外部触发 | VIP 基础设施 / 客户端逻辑 |
| 切换窗口短暂 | VIP 漂移通常秒级 |

---

## 3. 数据平面复制

### 双向 Double-Living Pipe

```
A 本地写入 → Pipe AB → B（副本）
B 本地写入 → Pipe BA → A（副本）
```

由于 A1（单写源），正常只有一个方向有实际数据流：

```
VIP → A 写入时：A→B 有数据流，B→A 空闲
VIP → B 写入时：B→A 有数据流，A→B 空闲
```

### Pipe 自动拆分（2.0.8+）

```
CREATE PIPE AB → AB_HISTORY + AB_REALTIME
CREATE PIPE BA → BA_HISTORY + BA_REALTIME
```

### 逻辑 Channel

```
Channel A_TO_B = { AB_HISTORY, AB_REALTIME }
Channel B_TO_A = { BA_HISTORY, BA_REALTIME }
```

| 状态 | 条件 |
|------|------|
| HEALTHY | history=RUNNING 且 realtime=RUNNING |
| DEGRADED | 至少一条 RUNNING |
| FAILED | 全部 STOPPED/FAILED |

---

## 4. 故障模型

### 4.1 VIP 漂移窗口

```
t=0: VIP → A 写入
t=1: A 网络抖动, VIP 开始漂移
t=2: VIP → B, Client 开始写入 B
t=3: A 网络恢复
```

t=1 到 t=2 之间 A 已接收的数据，在 A 恢复后通过 Pipe AB 同步到 B。前提：WAL/TsFile 持久化可靠（A4）。

### 4.2 重复数据

IoTDB 语义：同设备同时间戳 INSERT = 覆盖（upsert）。值相同 → 无影响。不需要去重机制。

### 4.3 短暂网络分区

Pipe AB/BA 短暂中断 → IoTDB 内置 auto-restart 恢复 → 自动 catch-up。不需要 Monitor 介入。

### 4.4 节点长时间离线

A 离线 7 天 → B 持续接收写入 → A 重新上线 → Pipe 追赶。见第 11 节。

---

## 5. iotdb-ha 子模块设计

### 5.1 设计原则：零侵入

```
iotdb-ha 是一个独立 Maven 子模块
只依赖 iotdb-jdbc（公开客户端 API）
不修改 iotdb-core、iotdb-client 等任何现有模块源码
```

### 5.2 两种运行模式

**模式 A：独立进程（默认，零侵入）**

```
Server A 进程列表：
  ├── ConfigNode (Java)    ← IoTDB 官方
  ├── DataNode  (Java)     ← IoTDB 官方
  └── iotdb-ha  (Java)     ← 我们的子模块，独立进程
```

**模式 B：嵌入 ConfigNode（可选，侵入 1-2 行）**

```java
// ConfigNode 启动流程中唯一新增的代码：
HaMonitorBootstrap.maybeStart(config);
```

```properties
# iotdb-confignode.properties 中新增：
ha_monitor_enabled=true
```

功能完全相同。区别只是进程模型不同。

### 5.3 侵入性分析

| 改动 | 位置 | 行数 |
|------|------|------|
| root pom.xml 加 `<module>iotdb-ha</module>` | 1 行 |
| 模式 B：ConfigNode 加 `HaMonitorBootstrap.maybeStart()` | 1-2 行 |
| 模式 B：ConfigNode 加配置项 `ha_monitor_enabled` | 1 行 |
| **模式 A 总计** | **1 行** |
| **模式 B 总计** | **3-4 行** |

**所有现有模块源码零修改。** upstream 合并时最多 3-4 行冲突。

### 5.4 Monitor 挂了不影响数据面

```
iotdb-ha DOWN：
  - VIP 继续工作
  - Pipe 继续同步
  - 数据写入不受影响
  - 只是失去告警和聚合视图
```

**Monitor 是观察者，不是控制者。**

### 5.5 职责

| 职责 | 说明 |
|------|------|
| 聚合集群视图 | 合并 A/B 的 SHOW CLUSTER + Pipe 状态，统一展示 |
| 节点健康检查 | 定期 JDBC 探测 IoTDB A/B 是否可达 |
| Pipe 状态监控 | 定期 SHOW PIPE，跟踪 Channel 状态 |
| 复制延迟监控 | 跟踪 remainingEventCount / lag |
| 告警 | 节点 DOWN / Pipe FAILED / lag 超阈值 → 通知 |
| REST API | 聚合状态查询 |
| Prometheus | 暴露监控指标 |

---

## 6. 聚合集群视图（SHOW CLUSTER 增强）

### 6.1 问题

IoTDB 原生 `SHOW CLUSTER` 只展示本节点信息：

```sql
-- 在 A 上执行
SHOW CLUSTER;
-- 只看到：ConfigNode A, DataNode A
-- 看不到 B 的任何信息
```

运维需要分别登录 A 和 B 才能了解全局状态。

### 6.2 解决方案

iotdb-ha 通过 JDBC 同时查询 A 和 B，提供 **聚合集群视图**：

```
GET /api/v1/cluster
```

```json
{
  "cluster_id": "iotdb-prod-01",
  "timestamp": "2026-09-24T10:00:00Z",

  "nodes": {
    "iotdb-a": {
      "role": "local",
      "config_node": { "status": "Running", "address": "10.0.0.101:10710" },
      "data_node":  { "status": "Running", "address": "10.0.0.101:6667" },
      "pipes": {
        "AB_HISTORY":  { "status": "RUNNING", "remaining_events": 0 },
        "AB_REALTIME": { "status": "RUNNING", "remaining_events": 0 }
      }
    },
    "iotdb-b": {
      "role": "remote",
      "config_node": { "status": "Running", "address": "10.0.0.102:10710" },
      "data_node":  { "status": "Running", "address": "10.0.0.102:6667" },
      "pipes": {
        "BA_HISTORY":  { "status": "RUNNING", "remaining_events": 0 },
        "BA_REALTIME": { "status": "RUNNING", "remaining_events": 12 }
      }
    }
  },

  "replication": {
    "A_TO_B": { "channel_state": "HEALTHY", "lag_seconds": 0 },
    "B_TO_A": { "channel_state": "HEALTHY", "lag_seconds": 0 }
  },

  "cluster_state": "NORMAL"
}
```

### 6.3 实现原理

```
iotdb-ha 定期执行：
  JDBC → A: SHOW CLUSTER, SHOW DATANODES, SHOW PIPE
  JDBC → B: SHOW CLUSTER, SHOW DATANODES, SHOW PIPE
  合并结果 → 聚合视图
```

不需要修改 IoTDB 的 SHOW CLUSTER 命令。iotdb-ha 在外部聚合。

### 6.4 对运维的价值

```
原来：
  ssh A → start-cli.sh → SHOW CLUSTER → 只看 A
  ssh B → start-cli.sh → SHOW CLUSTER → 只看 B
  人工对比两边状态

现在：
  curl http://iotdb-ha:8080/api/v1/cluster
  一次看到 A + B + Pipe + 复制状态
```

---

## 7. 一致性校验（两级）

### Level 0：Pipe Progress（持续运行，零成本）

```
SHOW PIPE → remainingEventCount, estimatedRemainingSeconds
```

Replication Health 指标，不是 Data Correctness 验证。

### Level 1：简单逻辑比对（低频，轻量）

```
每 10 min 对关键设备抽样：
  A: SELECT count(*), min(time), max(time) FROM ...
  B: SELECT count(*), min(time), max(time) FROM ...
  对比结果
```

---

## 8. Pipe 创建规范

### Pipe AB（在 A 上创建）

```sql
CREATE PIPE AB
WITH SOURCE (
    'source.mode.double-living'='true',
    'source.inclusion'='data, schema'
)
WITH SINK (
    'sink'='iotdb-thrift-sink',
    'sink.node-urls'='10.0.0.102:6667',
    'sink.exception.conflict.resolve-strategy'='retry',
    'sink.exception.conflict.retry-max-time-seconds'='-1'
);
```

### Pipe BA（在 B 上创建）

```sql
CREATE PIPE BA
WITH SOURCE (
    'source.mode.double-living'='true',
    'source.inclusion'='data, schema'
)
WITH SINK (
    'sink'='iotdb-thrift-sink',
    'sink.node-urls'='10.0.0.101:6667',
    'sink.exception.conflict.resolve-strategy'='retry',
    'sink.exception.conflict.retry-max-time-seconds'='-1'
);
```

| 参数 | 值 | 说明 |
|------|------|------|
| `source.mode.double-living` | true | 防循环 |
| `source.inclusion` | data, schema | 同步数据 + Schema |
| `sink.exception.conflict.resolve-strategy` | retry | 瞬态故障重试 |

**注意：** `forwarding-pipe-requests=true` 在 double-living 下被 IoTDB 源码拒绝。

### Pipe 创建时机

V1.0 中 Pipe 创建是 **手动操作**（运维在 A/B 上分别执行 CREATE PIPE）。

Monitor 启动时检查 Pipe 是否存在，缺失则告警。

### Schema 模型

V1.0 使用 IoTDB **Tree Model**（`root.xx.xx` 路径格式）。一致性检查 SQL 使用 Tree Model 语法。

如果未来切换到 Table Model，需更新一致性检查 SQL 格式。

---

## 9. IoTDB 配置规范

两台节点一致：

```
IoTDB Version     2.0.8+
Java Version      17+
Configuration     完全一致
enable_auto_create_schema = false（接收端）
编码 / 压缩 / 时区  一致
```

Schema 通过 Pipe 同步。TTL 通过 Pipe 同步，两端必须一致。V1.0 禁止业务 DELETE，使用 TTL 自动过期。

---

## 10. 三层恢复机制

```
Layer 1: IoTDB Pipe Auto Restart（IoTDB 内置，Monitor 不干预）
Layer 2: HA Monitor 观察（等待 T_auto_restart 窗口）
Layer 3: HA Monitor 告警（仍 STOPPED → 通知运维）
```

Monitor 不执行 Pipe Restart，避免与 IoTDB Auto Restart 互相干扰。

---

## 11. 长期离线节点快速恢复

### 问题

A 离线 7 天 → B 持续写入 → A 上线 → Pipe 追赶可能耗时数小时。

### 策略

#### 策略 1：Pipe 自动追赶（默认）

Pipe BA_history 自动传输 TsFile。全自动，适用离线 < 24h。

#### 策略 2：TsFile 批量拷贝（手动辅助）

```
1. Monitor 检测到巨大 lag → 告警
2. 运维 scp/rsync B 的 data/ 到 A
3. A 加载 TsFile
4. Pipe 从最新 ProgressIndex 继续
```

适用离线 > 24h，数据量巨大。

#### 策略 3：Monitor 辅助批量传输（未来增强）

自动化的策略 2。V1.0 暂不实现，预留接口。

### 追赶期间

| 操作 | 是否允许 |
|------|---------|
| 查询 | 允许（数据可能不完整） |
| 写入 | 允许 |
| 一致性校验 | 暂缓 |

追赶完成标志：所有 Pipe `remainingEventCount = 0` 且 `estimatedRemainingSeconds = 0`。

---

## 12. 监控指标

### Prometheus

```
# 节点状态
iotdb_ha_node_up{node="iotdb-a"}
iotdb_ha_node_up{node="iotdb-b"}

# Pipe 物理状态
iotdb_ha_pipe_state{pipe="AB_HISTORY"}
iotdb_ha_pipe_state{pipe="AB_REALTIME"}
iotdb_ha_pipe_state{pipe="BA_HISTORY"}
iotdb_ha_pipe_state{pipe="BA_REALTIME"}

# Channel 逻辑状态
iotdb_ha_channel_state{channel="A_TO_B"}
iotdb_ha_channel_state{channel="B_TO_A"}

# 复制延迟
iotdb_ha_pipe_remaining_events{pipe="AB_REALTIME"}
iotdb_ha_pipe_remaining_events{pipe="BA_REALTIME"}
iotdb_ha_pipe_lag_seconds{pipe="AB_REALTIME"}
iotdb_ha_pipe_lag_seconds{pipe="BA_REALTIME"}

# 告警计数
iotdb_ha_alert_total{type="node_down"}
iotdb_ha_alert_total{type="pipe_failed"}
iotdb_ha_alert_total{type="lag_critical"}
iotdb_ha_alert_total{type="schema_drift"}
iotdb_ha_alert_total{type="ttl_drift"}
```

### 告警阈值

| 指标 | WARNING | CRITICAL |
|------|---------|----------|
| Replication lag | > 5s | > 30s |
| Pipe state | DEGRADED | FAILED |
| Node state | - | DOWN |

具体阈值需压测确定。

### 健康检查退避策略

节点 DOWN 时不保持 5s 轮询，采用指数退避：

```
5s → 10s → 20s → 40s → 60s（上限）
```

节点恢复后重置为 5s。避免长时间 DOWN 期间产生大量无效连接和日志。

### 双节点同时不可达

```
A DOWN + B DOWN：
  - Monitor 标记两者状态为 UNKNOWN
  - 触发 CRITICAL 告警
  - 不做任何自动操作（Monitor 是观察者）
  - 等待节点恢复后自动重新检测
```

---

## 13. API

### 聚合集群视图

```
GET /api/v1/cluster
```

返回 A + B 的完整节点信息 + Pipe 状态 + 复制状态。见第 6.2 节。

### 状态摘要

```
GET /api/v1/status

{
  "cluster": "iotdb-prod-01",
  "cluster_state": "NORMAL",
  "nodes": {
    "iotdb-a": { "state": "UP", "uptime": "72h" },
    "iotdb-b": { "state": "UP", "uptime": "72h" }
  },
  "channels": {
    "A_TO_B": { "state": "HEALTHY" },
    "B_TO_A": { "state": "HEALTHY" }
  },
  "replication": {
    "ab_remaining_events": 0,
    "ba_remaining_events": 0,
    "ab_lag_seconds": 0,
    "ba_lag_seconds": 0
  }
}
```

### 告警列表

```
GET /api/v1/alerts

[
  {
    "type": "lag_critical",
    "severity": "CRITICAL",
    "message": "Pipe AB_REALTIME lag > 30s",
    "timestamp": "2026-09-24T10:00:00Z"
  }
]
```

### 一致性检查

```
POST /api/v1/consistency/check

{
  "devices": ["device01", "device02"],
  "time_range": ["2026-09-24T09:00:00", "2026-09-24T10:00:00"]
}

Response:
{
  "status": "PASS",
  "details": [
    { "device": "device01", "a_count": 1800, "b_count": 1800, "match": true }
  ]
}
```

---

## 14. 部署

### 独立进程模式（默认）

```
Server A:
  /opt/iotdb/                ← IoTDB 1C1D（官方二进制，不改）
  /opt/iotdb-ha/             ← HA Monitor
    ├── iotdb-ha.jar         ← fat jar
    └── config.yaml

Server B:
  /opt/iotdb/                ← IoTDB 1C1D（官方二进制，不改）
  /opt/iotdb-ha/             ← HA Monitor
    ├── iotdb-ha.jar
    └── config.yaml
```

### 嵌入 ConfigNode 模式（可选）

```
Server A:
  /opt/iotdb/                ← IoTDB 1C1D（含 iotdb-ha 模块）
    └── conf/iotdb-confignode.properties
        ha_monitor_enabled=true

Server B:
  /opt/iotdb/                ← IoTDB 1C1D（含 iotdb-ha 模块）
    └── conf/iotdb-confignode.properties
        ha_monitor_enabled=true
```

### 配置文件

```yaml
# config.yaml
cluster:
  id: iotdb-prod-01

nodes:
  - id: iotdb-a
    rpc_url: 10.0.0.101:6667
  - id: iotdb-b
    rpc_url: 10.0.0.102:6667

monitor:
  node_check_interval: 5s
  pipe_check_interval: 10s
  consistency_check_interval: 10m

alert:
  lag_warning_seconds: 5
  lag_critical_seconds: 30
  webhook_url: https://your-alert-system/webhook

api:
  port: 8080

metrics:
  enabled: true
  port: 9090
```

---

## 15. 网络规划

```
A: 10.0.0.101
B: 10.0.0.102
VIP: 10.0.0.100

IoTDB RPC: 6667
ConfigNode: 10710
HA Monitor API: 8080
HA Monitor Metrics: 9090

Pipe: A:6667 → B:6667
Pipe: B:6667 → A:6667
```

---

## 16. Chaos Test

### Test 01：正常双向复制

VIP → A 写入 → B 通过 Pipe 获得数据。

### Test 02：反向复制

VIP → B 写入 → A 通过 Pipe 获得数据。

### Test 03：防循环

A 写入 → B 收到 → Pipe BA → A → remainingEventCount 不持续增长。

### Test 04：VIP 漂移

VIP → A → VIP 漂移到 B → B 写入 → A 恢复后 Pipe 同步 → 数据最终一致。

### Test 05：节点宕机与恢复

A DOWN → VIP → B → A UP → Pipe catch-up → 数据一致。

### Test 06：Pipe 中断

A ──X── B → A 继续写入 → 恢复后 Pipe auto-restart → remainingEvents = 0。

### Test 07：短暂网络分区

A ←─X─→ B（10s）→ Pipe auto-restart 恢复，数据无丢失。

### Test 08：重复数据

Client 发送 → ACK 丢失 → 重发 → A 只有一条（upsert）→ B 通过 Pipe 也只有一条。

### Test 09：大量数据

1000 devices × 10 measurements × 2s = 5000 pts/s → 逐步到 100k → 测 throughput/lag/CPU/memory。

### Test 10：长时间断链

B unavailable 30min → A 持续写 → B recover → 测 catch-up time。

### Test 11：长期离线恢复

A 离线 24h+ → B 持续写入 → A 上线 → 测追赶时间、性能影响。

### Test 12：HA Monitor 宕机

iotdb-ha DOWN → VIP 继续、Pipe 继续、写入不受影响 → UP 后恢复监控。

### Test 13：Schema 同步

A: CREATE TIMESERIES → B 通过 Pipe 自动获得 Schema。

### Test 14：Pipe Auto-Restart 不干扰

Pipe exception → IoTDB auto restart → Monitor 不重复 restart。

### Test 15：VIP 漂移窗口数据完整性

VIP → A 写入 → VIP 漂移 → VIP → B → A 最后数据通过 Pipe 同步到 B。

### Test 16：聚合集群视图

```
调用 GET /api/v1/cluster
验证：同时展示 A + B 的节点信息、Pipe 状态、复制状态
在 A 宕机时调用：B 信息正常，A 标记为 DOWN
```

---

## 17. 验收指标

| 指标 | 验收目标 |
|------|----------|
| 双向复制 | PASS |
| 防循环 | PASS |
| VIP 漂移后数据一致 | PASS |
| 节点恢复后数据一致 | PASS |
| Pipe 自动恢复 | PASS |
| 重复数据处理 | PASS（upsert 幂等） |
| Schema 同步 | PASS |
| 长期离线追赶 | PASS（时间可接受） |
| Monitor 宕机不影响数据面 | PASS |
| 聚合集群视图 | PASS（同时展示 A + B） |
| 数据丢失 | 0（持久化可靠时） |

---

## 18. V1.0 明确边界

### 支持

```
2 Node · 1C1D
Active-Active（VIP 或客户端双 IP）
Bidirectional Double-Living Pipe
Schema Sync · TTL Sync
聚合集群视图（SHOW CLUSTER 增强）
Node/Pipe Health Monitor
Replication Lag Alert
Long Offline Recovery
Prometheus · REST API
Chaos Test
```

### 不支持

```
3+ Node · 自动分片 · 跨区域多活 · 强一致同步
Device-level Ownership / Epoch Fencing
Witness / 第三方仲裁
Semantic Conflict Resolution
自动 DELETE · 在线 Schema 任意变更
```

| 不支持项 | 理由 |
|---------|------|
| Device-level Ownership | 场景是整体切换（VIP） |
| Epoch Fencing | 无并发双写 |
| Witness | 增加故障点 |
| Conflict Resolution | IoTDB upsert 天然处理 |

---

## 19. iotdb-ha 子模块结构

### Maven 模块

```
e:\javapro\iotdb/
├── iotdb-core/          ← 不改
├── iotdb-client/        ← 不改
├── iotdb-ha/            ← 新增
│   ├── pom.xml
│   └── src/
├── pom.xml              ← 加一行 <module>iotdb-ha</module>
└── ...
```

### 代码结构

```
iotdb-ha/
├── pom.xml
│
└── src/main/java/org/apache/iotdb/ha/
    │
    ├── HaMonitorMain.java            # 入口（独立进程时 main()）
    │
    ├── cluster/
    │   └── ClusterAggregator.java    # 聚合集群视图（SHOW CLUSTER 增强）
    │
    ├── checker/
    │   ├── NodeHealthChecker.java    # JDBC 连通性检查
    │   └── PipeInspector.java        # SHOW PIPE 解析与 Channel 状态
    │
    ├── alert/
    │   └── Alerter.java              # 告警（webhook / 日志）
    │
    ├── api/
    │   ├── ApiServer.java            # 轻量 HTTP 服务
    │   └── ApiHandler.java           # REST 路由
    │
    ├── metrics/
    │   └── MetricsExporter.java      # Prometheus 指标
    │
    ├── config/
    │   └── HaConfig.java             # 配置加载
    │
    └── bootstrap/
        └── HaMonitorBootstrap.java   # 嵌入 ConfigNode 时的入口
```

### 依赖

```xml
<!-- iotdb-ha/pom.xml -->
<dependencies>
    <!-- 只依赖公开客户端 API -->
    <dependency>
        <groupId>org.apache.iotdb</groupId>
        <artifactId>iotdb-jdbc</artifactId>
    </dependency>
    <!-- Prometheus -->
    <dependency>
        <groupId>io.prometheus</groupId>
        <artifactId>simpleclient</artifactId>
    </dependency>

    <!-- JSON -->
    <dependency>
        <groupId>com.google.code.gson</groupId>
        <artifactId>gson</artifactId>
    </dependency>
</dependencies>
```

**不依赖 iotdb-core、iotdb-confignode、iotdb-datanode。**

### 嵌入 ConfigNode 时的连接方式

```
模式 A（独立进程）：
  iotdb-ha → JDBC → IoTDB A / IoTDB B（网络连接）

模式 B（嵌入 ConfigNode）：
  iotdb-ha → JDBC → 本地 DataNode（localhost）
                   → 远程 DataNode（网络连接）
```

模式 B 查本地节点走 localhost JDBC，减少一跳。

---

## 20. 演进路线

### V0.1 — Pipe Proof

```
步骤：
  1. 两台服务器部署 IoTDB 2.0.8+（1C1D）
  2. 在 A 上执行 CREATE PIPE AB ...
  3. 在 B 上执行 CREATE PIPE BA ...
  4. 验证 SHOW PIPE 状态为 RUNNING
  5. VIP → A 写入 → 在 B 查询验证数据到达
  6. VIP → B 写入 → 在 A 查询验证数据到达
  7. 验证 remainingEventCount 不持续增长（防循环）
  8. 验证同设备同时间戳重复 INSERT 不膨胀（upsert）
```

### V0.2 — iotdb-ha 骨架

```
Maven 子模块：Node Health + Pipe Status + Prometheus + 聚合集群视图
```

### V0.3 — Alert

```
增加：告警通知（webhook / 日志）
```

### V0.4 — Recovery Test

```
长期离线恢复测试 + Pipe 追赶性能基准
```

### V0.5 — Chaos

```
Node Crash, Network Partition, Pipe Failure,
VIP Drift, Long Disconnect, Monitor Crash
```

### V1.0 — Production

```
Apache IoTDB 2.0.x
  + Double-Living Pipe
  + iotdb-ha 子模块（聚合视图 + 监控 + 告警）
  + Prometheus + Alert
  + Chaos Test
```

---

## 21. Upstream 合并策略

### 核心原则

```
iotdb-ha 作为独立子模块存在
不修改现有模块源码
upstream 合并时只需解决 pom.xml 的 module 声明冲突
```

### 具体操作

```bash
# 拉取 upstream 最新代码
git fetch upstream
git merge upstream/main

# 可能冲突的文件（最多 2 个）：
#   pom.xml                    → 接受 upstream + 保留 iotdb-ha module
#   （如果用模式 B）confignode → 1-2 行代码冲突
```

### 如果 upstream 不接受 iotdb-ha

iotdb-ha 可以独立存在：
1. 从 IoTDB 工程中移出，成为独立 Git 仓库
2. 只依赖 iotdb-jdbc Maven artifact（从 Maven Central 获取）
3. 完全独立构建和发布

**这是零退路方案：即使永远不能合入 upstream，项目也能正常运行。**

---

## 22. V1.1 → V1.2 对照表

| V1.1 | V1.2 | 原因 |
|------|------|------|
| Witness + Epoch + Fencing | 删除 | 无双写冲突 |
| Device-level Ownership | 删除 | VIP 整体切换 |
| Conflict Manager | 删除 | upsert 幂等 |
| 四层一致性校验 | 两级 | 简化 |
| 12+ 子模块 | 6 个核心包 | 减少故障点 |
| Go 独立进程 | Java 子模块 | 统一技术栈 |
| Controller = Manager | Monitor | 只观察 |
| 未覆盖长期离线 | 新增 | 核心需求 |
| 未覆盖 VIP 漂移 | 新增 | 真实故障 |
| 未覆盖聚合视图 | 新增 | 运维便利 |

---

## 23. 关键源码验证记录

以下事实在 Apache IoTDB 2.0.x 源码中已验证：

1. **double-living 模式**：`PipeSourceConstant.isDoubleLiving()` 存在，禁止与 `forwarding-pipe-requests=true` 同时使用（[IoTDBSource.java](file:///e:/javapro/iotdb/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/source/IoTDBSource.java#L117-L132)）。

2. **Pipe 自动拆分**：2.0.8+ Full Data Pipe 自动拆成 history + realtime（[ClusterConfigTaskExecutor.java](file:///e:/javapro/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/config/executor/ClusterConfigTaskExecutor.java#L2317-L2318)）。

3. **Pipe Auto Restart**：存在自动重启机制（[PipeTaskInfoAutoRestartTest.java](file:///e:/javapro/iotdb/iotdb-core/confignode/src/test/java/org/apache/iotdb/confignode/persistence/pipe/PipeTaskInfoAutoRestartTest.java)）。

4. **TsFile 资源保护**：历史数据抽取有引用计数保护（[PipeHistoricalDataRegionTsFileAndDeletionSource.java](file:///e:/javapro/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/source/dataregion/historical/PipeHistoricalDataRegionTsFileAndDeletionSource.java)）。

5. **集成测试**：双集群 double-living 测试（[IoTDBPipeLifeCycleIT.testDoubleLiving](file:///e:/javapro/iotdb/integration-test/src/test/java/org/apache/iotdb/pipe/it/dual/treemodel/auto/basic/IoTDBPipeLifeCycleIT.java#L672)）。

6. **Sink 冲突策略**：`retry` 已在集成测试中使用（[IoTDBPipeManualConflictIT](file:///e:/javapro/iotdb/integration-test/src/test/java/org/apache/iotdb/pipe/it/dual/treemodel/manual/IoTDBPipeManualConflictIT.java)）。

7. **Schema 同步**：`source.inclusion=data, schema` 已验证（[IoTDBPipeMetaHistoricalIT](file:///e:/javapro/iotdb/integration-test/src/test/java/org/apache/iotdb/pipe/it/dual/treemodel/manual/IoTDBPipeMetaHistoricalIT.java)）。
