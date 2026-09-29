# IoTDB HA Monitor — 开发计划

基于 [dual-active-spec.md](dual-active-spec.md) V1.2

---

## 总览

```
Phase 0: 环境准备          ← 基础设施，不写代码
Phase 1: Pipe Proof        ← 验证核心假设（SPEC §1 A1-A7）
Phase 2: iotdb-ha 骨架     ← Maven 子模块 + 核心功能
Phase 3: 监控与告警         ← Prometheus + Alert
Phase 4: 恢复测试           ← 长期离线 + 追赶
Phase 5: Chaos Test         ← 故障注入
Phase 6: 收尾               ← 文档 + 部署脚本 + 验收
```

预估总工期：**4-6 周**（1 人，含环境搭建和测试）

---

## Phase 0：环境准备

**目标：** 两台服务器运行 IoTDB 2.0.8+，网络互通。

**工期：** 2-3 天

### 任务清单

| # | 任务 | 交付物 | 验证标准 |
|---|------|--------|---------|
| 0.1 | 服务器 A 部署 IoTDB 2.0.8+（1C1D） | IoTDB A 运行中 | `start-cli.sh` 可连接，`SHOW CLUSTER` 返回 1C1D |
| 0.2 | 服务器 B 部署 IoTDB 2.0.8+（1C1D） | IoTDB B 运行中 | 同上 |
| 0.3 | 网络互通验证 | A:6667 ↔ B:6667 | A 上 `telnet B 6667` 通，反向通 |
| 0.4 | 配置一致性检查 | 两节点 iotdb-system.properties 一致 | diff 对比无差异 |
| 0.5 | VIP 搭建（Keepalived 或云 VIP） | VIP 可漂移 | VIP → A → 手动切 → VIP → B |

### 依赖

- 无。这是所有后续 Phase 的前提。

### 注意事项

- 两台服务器必须使用相同 IoTDB 版本（2.0.8+）
- `enable_auto_create_schema = false` 在接收端设置
- 记录两节点的 Node ID（不可变）

---

## Phase 1：Pipe Proof（V0.1）

**目标：** 验证双向 Double-Living Pipe 工作，防循环，upsert 幂等。

**工期：** 3-5 天

**这是最关键的 Phase。** 如果 Pipe 不工作，后续全部无意义。

### 任务清单

| # | 任务 | 交付物 | 验证标准 |
|---|------|--------|---------|
| 1.1 | 在 A 上创建 Pipe AB | SQL 脚本 | `SHOW PIPE` 显示 AB_HISTORY + AB_REALTIME 均为 RUNNING |
| 1.2 | 在 B 上创建 Pipe BA | SQL 脚本 | 同上 |
| 1.3 | A→B 正向复制验证 | 测试数据 | VIP→A 写入 1000 条 → B 查询到相同数据 |
| 1.4 | B→A 反向复制验证 | 测试数据 | VIP→B 写入 1000 条 → A 查询到相同数据 |
| 1.5 | 防循环验证 | 监控记录 | 写入 1000 条后 `remainingEventCount` 稳定归零，不持续增长 |
| 1.6 | upsert 幂等验证 | 测试数据 | 同设备同时间戳写两次 → 两端 count 相同（不翻倍） |
| 1.7 | Schema 同步验证 | 测试 Schema | A 上 CREATE TIMESERIES → B 上 SHOW TIMESERIES 可见 |
| 1.8 | Pipe 参数验证 | 记录 | 确认 `forwarding-pipe-requests=true` 被 double-living 拒绝 |

### 交付物

- `scripts/create-pipe-ab.sql`
- `scripts/create-pipe-ba.sql`
- `scripts/test-pipe-proof.sh`（自动化验证脚本）
- 测试报告

### 风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| IoTDB 版本不含 double-living | 无法防循环 | 确认版本 ≥ 2.0.8 |
| Pipe 创建参数错误 | Pipe 不启动 | 参照 SPEC §8 |
| 网络不稳定 | Pipe 频繁中断 | 先确保 Phase 0 网络可靠 |

---

## Phase 2：iotdb-ha 骨架（V0.2）

**目标：** 创建 Maven 子模块，实现核心监控功能 + 聚合集群视图。

**工期：** 1-2 周

### 任务清单

| # | 任务 | 交付物 | 验证标准 |
|---|------|--------|---------|
| 2.1 | 创建 `iotdb-ha/` Maven 子模块 | pom.xml + 目录结构 | `mvn compile` 通过 |
| 2.2 | root pom.xml 注册模块 | pom.xml 加 1 行 | `mvn -pl iotdb-ha compile` 通过 |
| 2.3 | 实现 `HaConfig` | HaConfig.java | 加载 config.yaml |
| 2.4 | 实现 `NodeHealthChecker` | NodeHealthChecker.java | JDBC `SELECT now()` 探测 A/B，返回 UP/DOWN |
| 2.5 | 实现 `PipeInspector` | PipeInspector.java | JDBC `SHOW PIPE` 解析状态，计算 Channel 状态 |
| 2.6 | 实现 `ClusterAggregator` | ClusterAggregator.java | 聚合 A+B 的 SHOW CLUSTER + Pipe 状态 |
| 2.7 | 实现 `ApiServer` + `ApiHandler` | api/*.java | `curl localhost:8080/api/v1/cluster` 返回 JSON |
| 2.8 | 实现 `HaMonitorMain` | HaMonitorMain.java | 独立进程启动，定期刷新状态 |
| 2.9 | 集成测试 | 测试代码 | 连接真实 IoTDB A/B，验证聚合视图正确 |

### 代码结构

```
iotdb-ha/
├── pom.xml
└── src/
    ├── main/java/org/apache/iotdb/ha/
    │   ├── HaMonitorMain.java
    │   ├── config/HaConfig.java
    │   ├── checker/NodeHealthChecker.java
    │   ├── checker/PipeInspector.java
    │   ├── cluster/ClusterAggregator.java
    │   └── api/ApiServer.java
    │   └── api/ApiHandler.java
    └── test/java/org/apache/iotdb/ha/
        └── ...
```

### 依赖

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.iotdb</groupId>
        <artifactId>iotdb-jdbc</artifactId>
    </dependency>
    <dependency>
        <groupId>io.prometheus</groupId>
        <artifactId>simpleclient</artifactId>
    </dependency>
    <dependency>
        <groupId>com.google.code.gson</groupId>
        <artifactId>gson</artifactId>
    </dependency>
</dependencies>
```

### 里程碑

Phase 2 完成后，运维可以：
```bash
# 启动 iotdb-ha
java -jar iotdb-ha.jar --config config.yaml

# 查看聚合集群视图
curl http://localhost:8080/api/v1/cluster
```

---

## Phase 3：监控与告警（V0.3）

**目标：** Prometheus 指标暴露 + 告警通知。

**工期：** 3-5 天

### 任务清单

| # | 任务 | 交付物 | 验证标准 |
|---|------|--------|---------|
| 3.1 | 实现 `MetricsExporter` | MetricsExporter.java | `curl localhost:9090/metrics` 返回 Prometheus 格式 |
| 3.2 | 注册所有指标（SPEC §12） | 代码 | node_up, pipe_state, channel_state, remaining_events, lag_seconds 均可采集 |
| 3.3 | 实现 `Alerter` | Alerter.java | 阈值触发 webhook 通知 |
| 3.4 | 实现健康检查退避策略 | 代码 | 节点 DOWN 时 5s→10s→20s→40s→60s |
| 3.5 | 实现双节点不可达处理 | 代码 | 两者标记 UNKNOWN + CRITICAL 告警 |
| 3.6 | Grafana Dashboard（可选） | dashboard.json | 可视化集群状态 |

### 里程碑

Phase 3 完成后：
- Prometheus 可以采集 iotdb-ha 指标
- 节点 DOWN / Pipe FAILED / lag 超阈值时触发告警
- Grafana 可以看到集群状态面板

---

## Phase 4：恢复测试（V0.4）

**目标：** 验证长期离线节点的恢复能力。

**工期：** 3-5 天

### 任务清单

| # | 任务 | 交付物 | 验证标准 |
|---|------|--------|---------|
| 4.1 | Pipe 自动追赶基准测试 | 测试报告 | 离线 1h / 6h / 24h 的追赶时间 |
| 4.2 | 追赶期间性能影响测试 | 测试报告 | 追赶期间写入/查询 QPS 和延迟 |
| 4.3 | Monitor 追赶进度监控验证 | 测试报告 | remainingEventCount 正确反映追赶进度 |
| 4.4 | TsFile 批量拷贝流程验证（策略 2） | 操作手册 | scp + 加载 + Pipe 断点续传 |
| 4.5 | 一致性校验（Level 0 + Level 1） | 代码 + 测试 | API 返回 A/B count 对比结果 |

### 关键指标记录模板

```
离线时长    数据量     追赶时间    追赶期间写入 QPS    追赶期间查询延迟
1h         ___MB     ___s       ___qps              ___ms
6h         ___MB     ___s       ___qps              ___ms
24h        ___GB     ___s       ___qps              ___ms
```

---

## Phase 5：Chaos Test（V0.5）

**目标：** 验证所有故障场景。

**工期：** 1 周

### 任务清单

| # | 测试 | 方法 | 预期结果 |
|---|------|------|---------|
| 5.1 | Test 01-03：双向复制 + 防循环 | 自动脚本 | PASS |
| 5.2 | Test 04-05：VIP 漂移 + 节点恢复 | 手动 VIP 切换 + kill IoTDB | 数据最终一致 |
| 5.3 | Test 06-07：Pipe 中断 + 网络分区 | iptables / tc | Pipe auto-restart，数据无丢失 |
| 5.4 | Test 08：重复数据 | 模拟 ACK 丢失 | upsert 幂等 |
| 5.5 | Test 09：大量数据 | 压测工具 | 记录 throughput/lag/CPU/memory |
| 5.6 | Test 10-11：长时间断链 + 离线恢复 | 停 IoTDB 30min / 24h | 追赶成功 |
| 5.7 | Test 12：Monitor 宕机 | kill iotdb-ha | VIP/Pipe/写入不受影响 |
| 5.8 | Test 13-14：Schema 同步 + Auto-Restart 不干扰 | 手动 | PASS |
| 5.9 | Test 15：VIP 漂移窗口完整性 | 快速切换 | WAL 数据通过 Pipe 同步 |
| 5.10 | Test 16：聚合集群视图 | API 调用 | 正确展示 A+B 状态 |

### 交付物

- `scripts/chaos/` 目录下的自动化测试脚本
- 完整测试报告（每个 Test 的 PASS/FAIL + 证据）

---

## Phase 6：收尾

**目标：** 生产就绪。

**工期：** 3-5 天

### 任务清单

| # | 任务 | 交付物 |
|---|------|--------|
| 6.1 | 部署脚本（systemd service） | `iotdb-ha.service` |
| 6.2 | config.yaml 模板 | `config.yaml.example` |
| 6.3 | 运维手册 | `docs/ops-guide.md` |
| 6.4 | 验收测试报告 | 对照 SPEC §17 逐项 PASS |
| 6.5 | 代码审查 + 清理 | 代码质量 |

---

## 关键路径

```
Phase 0（环境）
    │
    ▼
Phase 1（Pipe Proof）  ← 最关键，不通过则停止
    │
    ▼
Phase 2（iotdb-ha 骨架）
    │
    ├──→ Phase 3（监控告警）
    │
    └──→ Phase 4（恢复测试）
              │
              ▼
         Phase 5（Chaos Test）
              │
              ▼
         Phase 6（收尾）
```

Phase 3 和 Phase 4 可以并行。

---

## 开发环境需求

| 资源 | 说明 |
|------|------|
| 服务器 A | 10.0.0.101，IoTDB 2.0.8+，Java 17+ |
| 服务器 B | 10.0.0.102，IoTDB 2.0.8+，Java 17+ |
| 开发机 | 本地，Java 17+，Maven（已配置） |
| VIP | 10.0.0.100（Keepalived 或云 VIP） |
| Prometheus | 用于采集指标（可选，Phase 3 需要） |
| Grafana | 用于可视化（可选） |

---

## 每个 Phase 的准入/准出标准

### Phase 0 准出

- [ ] IoTDB A/B 均可通过 CLI 连接
- [ ] A ↔ B 网络互通（6667 端口）
- [ ] VIP 可漂移
- [ ] 两节点配置一致

### Phase 1 准出

- [ ] Pipe AB / BA 状态 RUNNING
- [ ] A→B 和 B→A 数据复制验证通过
- [ ] 防循环验证通过（remainingEventCount 归零）
- [ ] upsert 幂等验证通过
- [ ] Schema 同步验证通过

### Phase 2 准出

- [ ] `mvn -pl iotdb-ha compile` 通过
- [ ] 独立进程启动成功
- [ ] `GET /api/v1/cluster` 返回 A+B 聚合信息
- [ ] 节点 DOWN 时 API 正确标记

### Phase 3 准出

- [ ] Prometheus 可采集 iotdb-ha 指标
- [ ] 节点 DOWN 触发告警
- [ ] Pipe FAILED 触发告警
- [ ] lag 超阈值触发告警

### Phase 4 准出

- [ ] 离线 1h/6h/24h 追赶时间已记录
- [ ] 追赶期间性能影响已评估
- [ ] 一致性校验 API 工作正常

### Phase 5 准出

- [ ] 16 个 Chaos Test 全部执行
- [ ] 测试结果记录在案

### Phase 6 准出

- [ ] SPEC §17 验收指标全部 PASS
- [ ] 部署脚本可用
- [ ] 运维手册完成
