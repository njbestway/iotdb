/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.ha.config;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** HA Monitor configuration, loaded from config.yaml (JSON format). */
public class HaConfig {
  private static final Logger logger = LoggerFactory.getLogger(HaConfig.class);

  @SerializedName("cluster")
  private ClusterConfig cluster = new ClusterConfig();

  @SerializedName("nodes")
  private List<NodeConfig> nodes = new ArrayList<>();

  @SerializedName("monitor")
  private MonitorConfig monitor = new MonitorConfig();

  @SerializedName("alert")
  private AlertConfig alert = new AlertConfig();

  @SerializedName("api")
  private ApiConfig api = new ApiConfig();

  @SerializedName("metrics")
  private MetricsConfig metrics = new MetricsConfig();

  @SerializedName("recovery")
  private RecoveryConfig recovery = new RecoveryConfig();

  @SerializedName("session")
  private SessionConfig session = new SessionConfig();

  @SerializedName("audit")
  private AuditConfig audit = new AuditConfig();

  @SerializedName("backlog")
  private BacklogConfig backlog = new BacklogConfig();

  public static HaConfig load(String configPath) throws IOException {
    Path path = Paths.get(configPath);
    logger.info("Loading config from: {}", path.toAbsolutePath());
    try (Reader reader = new FileReader(path.toFile())) {
      HaConfig config = new Gson().fromJson(reader, HaConfig.class);
      config.validate();
      logger.info("Config loaded: cluster={}, nodes={}", config.cluster.id, config.nodes.size());
      return config;
    }
  }

  private void validate() {
    if (nodes.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 nodes required, got: " + nodes.size());
    }
    for (NodeConfig node : nodes) {
      if (node.id == null || node.id.isEmpty()) {
        throw new IllegalArgumentException("Node id is required");
      }
      if (node.rpcUrl == null || node.rpcUrl.isEmpty()) {
        throw new IllegalArgumentException("Node rpc_url is required for: " + node.id);
      }
    }
  }

  public String getClusterId() {
    return cluster.id;
  }

  public List<NodeConfig> getNodes() {
    return nodes;
  }

  public NodeConfig getLocalNode() {
    return nodes.get(0);
  }

  public NodeConfig getRemoteNode() {
    return nodes.get(1);
  }

  public long getNodeCheckIntervalMs() {
    return parseDuration(monitor.nodeCheckInterval);
  }

  public long getPipeCheckIntervalMs() {
    return parseDuration(monitor.pipeCheckInterval);
  }

  /**
   * TTL of the per-node pipe-status cache in {@code PipeInspector}. A single recovery cycle calls
   * {@code getPipes} 3x per node and a single REST {@code /cluster} call 4x; the cache collapses
   * those redundant calls into one real fetch per node per TTL window, drastically cutting the
   * number of {@code SHOW PIPES} statements (each of which borrows a DataNode→ConfigNode client).
   * Keep it well below {@code recovery.auto_restart_wait} so recovery stays responsive.
   */
  public long getPipeCacheTtlMs() {
    return parseDuration(monitor.pipeCacheTtl != null ? monitor.pipeCacheTtl : "2s");
  }

  public long getConsistencyCheckIntervalMs() {
    return parseDuration(monitor.consistencyCheckInterval);
  }

  /** Connect/read timeout (ms) for the HA↔HA pipe-status HTTP exchange. */
  public long getRemoteHttpTimeoutMs() {
    return parseDuration(monitor.remoteHttpTimeout != null ? monitor.remoteHttpTimeout : "3s");
  }

  /** Whether the legacy remote {@code SHOW PIPES} Session fallback is allowed for the peer node. */
  public boolean isRemotePipeStatusViaSessionEnabled() {
    return monitor.remotePipeStatusViaSession;
  }

  /**
   * Whether this node pushes a one-shot "I'm back UP" hint to the peer the moment its own DataNode
   * recovers, so the peer collapses its exponential backoff and re-probes within ~1 RTT instead of
   * waiting out the (up to 60s) backoff window. The hint only accelerates perception — the peer
   * still confirms liveness with its own probe, so polling remains the correctness baseline.
   */
  public boolean isNotifyPeerOnUp() {
    return monitor.notifyPeerOnUp;
  }

  public int getLagWarningSeconds() {
    return alert.lagWarningSeconds;
  }

  public int getLagCriticalSeconds() {
    return alert.lagCriticalSeconds;
  }

  public String getWebhookUrl() {
    return alert.webhookUrl;
  }

  public long getAlertCooldownMs() {
    return parseDuration(alert.cooldownSeconds != null ? alert.cooldownSeconds + "s" : "60s");
  }

  public int getApiPort() {
    return api.port;
  }

  public boolean isMetricsEnabled() {
    return metrics.enabled;
  }

  public int getMetricsPort() {
    return metrics.port;
  }

  // ── Recovery config ──

  public boolean isRecoveryEnabled() {
    return recovery.enabled;
  }

  public long getAutoRestartWaitMs() {
    return parseDuration(recovery.autoRestartWait != null ? recovery.autoRestartWait : "30s");
  }

  public long getNodeStartupWaitMs() {
    return parseDuration(recovery.nodeStartupWait != null ? recovery.nodeStartupWait : "30s");
  }

  public long getRecoveryCheckIntervalMs() {
    return parseDuration(recovery.checkInterval != null ? recovery.checkInterval : "15s");
  }

  public int getLongOfflineThresholdHours() {
    return recovery.longOfflineThresholdHours;
  }

  /** Whether the monitor may proactively FLUSH the local node to accelerate pipe catch-up. */
  public boolean isFlushEnabled() {
    return recovery.flushEnabled;
  }

  /** Backlog (total remaining events) at or above which a proactive FLUSH is triggered. */
  public long getFlushRemainingEventsThreshold() {
    return recovery.flushRemainingEventsThreshold;
  }

  /** Minimum interval between two proactive FLUSHes on the same node (avoids small-file churn). */
  public long getFlushCooldownMs() {
    return parseDuration(recovery.flushCooldown != null ? recovery.flushCooldown : "5m");
  }

  /**
   * Catch-up ETA (seconds) at or above which a proactive FLUSH is triggered even if the remaining
   * event count is below {@link #getFlushRemainingEventsThreshold()}. Sealing active TsFiles lets
   * the hybrid pipe switch to whole-file batch transfer, shrinking a slow-draining ETA.
   */
  public long getFlushEtaSeconds() {
    return recovery.flushEtaSeconds;
  }

  /**
   * Whether a STUCK backlog (RUNNING pipe whose remaining events are not draining) may trigger a
   * proactive FLUSH. A frozen backlog is the prime candidate for sealing TsFiles to force batch
   * transfer, so this is on by default.
   */
  public boolean isFlushOnStuck() {
    return recovery.flushOnStuck;
  }

  /**
   * Upper bound of the cooldown multiplier applied when consecutive FLUSHes fail to make the backlog
   * drain (futile backoff). Prevents repeated useless FLUSHes — and the resulting small-file churn —
   * when sealing TsFiles cannot accelerate catch-up (e.g. a degraded but reachable sink).
   */
  public int getFlushFutileBackoffMax() {
    return recovery.flushFutileBackoffMax > 0 ? recovery.flushFutileBackoffMax : 1;
  }

  public String getHaStatusFilePath() {
    return recovery.haStatusFilePath != null
        ? recovery.haStatusFilePath
        : "data/ha-cluster-status.json";
  }

  /** Override the HA status file path (e.g. to set an absolute path). */
  public void setHaStatusFilePath(String absolutePath) {
    recovery.haStatusFilePath = absolutePath;
  }

  public String getSessionUsername() {
    return session.username;
  }

  public String getSessionPassword() {
    return session.password;
  }

  // ── Data audit config ──

  /**
   * Whether periodic data audit is enabled. Audit compares row counts of every replicated table
   * between the local (source) node and the remote (target) node and alerts on divergence beyond
   * tolerance. It should run on the replication SOURCE node only to avoid duplicate reports.
   */
  public boolean isAuditEnabled() {
    return audit.enabled;
  }

  /** Interval between two data-audit passes. */
  public long getAuditIntervalMs() {
    return parseDuration(audit.interval != null ? audit.interval : "10m");
  }

  /**
   * Databases to audit. Empty means auto-discover all user databases via {@code SHOW DATABASES}
   * (system databases such as {@code information_schema} are always excluded).
   */
  public List<String> getAuditDatabases() {
    return audit.databases != null ? audit.databases : new ArrayList<>();
  }

  /**
   * Tables to audit within each database. Empty means auto-discover all tables via {@code SHOW
   * TABLES FROM <db>}.
   */
  public List<String> getAuditTables() {
    return audit.tables != null ? audit.tables : new ArrayList<>();
  }

  /** Absolute row-count difference tolerated before a WARNING audit alert fires. */
  public long getAuditRowCountTolerance() {
    return audit.rowCountTolerance;
  }

  /** Absolute row-count difference at or above which the audit alert escalates to CRITICAL. */
  public long getAuditCriticalThreshold() {
    return audit.criticalThreshold;
  }

  // ── Pipe backlog alerting config ──

  /**
   * Whether intelligent pipe-backlog alerting is enabled. Independent of {@code recovery.enabled}
   * so backlog grading/alerting also works in monitor-only mode.
   */
  public boolean isBacklogEnabled() {
    return backlog.enabled;
  }

  /** Interval between two backlog-monitor passes. */
  public long getBacklogIntervalMs() {
    return parseDuration(backlog.interval != null ? backlog.interval : "15s");
  }

  /** Remaining-event count at/above which a pipe backlog is graded WARNING. */
  public long getBacklogWarningEvents() {
    return backlog.warningRemainingEvents;
  }

  /** Remaining-event count at/above which a pipe backlog is graded CRITICAL. */
  public long getBacklogCriticalEvents() {
    return backlog.criticalRemainingEvents;
  }

  /** Estimated catch-up seconds at/above which a pipe backlog is graded WARNING. */
  public long getBacklogWarningEtaSeconds() {
    return backlog.warningEtaSeconds;
  }

  /** Estimated catch-up seconds at/above which a pipe backlog is graded CRITICAL. */
  public long getBacklogCriticalEtaSeconds() {
    return backlog.criticalEtaSeconds;
  }

  /**
   * Window over which a non-draining backlog (remaining not decreasing while RUNNING) is treated as
   * STUCK and escalated to CRITICAL.
   */
  public long getBacklogStuckWindowMs() {
    return parseDuration(backlog.stuckWindow != null ? backlog.stuckWindow : "3m");
  }

  /** Assumed sink throughput (events/sec) used to derive an ETA when IoTDB reports none. */
  public double getBacklogAssumedThroughputEps() {
    return backlog.assumedThroughputEps > 0 ? backlog.assumedThroughputEps : 10000.0;
  }

  /** Cooldown between repeated backlog alerts for the same pipe. */
  public long getBacklogCooldownMs() {
    return parseDuration(backlog.cooldown != null ? backlog.cooldown : "5m");
  }

  private static long parseDuration(String duration) {
    if (duration == null || duration.isEmpty()) {
      return 10000;
    }
    duration = duration.trim().toLowerCase();
    if (duration.endsWith("ms")) {
      return Long.parseLong(duration.substring(0, duration.length() - 2));
    } else if (duration.endsWith("s")) {
      return Long.parseLong(duration.substring(0, duration.length() - 1)) * 1000;
    } else if (duration.endsWith("m")) {
      return Long.parseLong(duration.substring(0, duration.length() - 1)) * 60000;
    }
    return Long.parseLong(duration);
  }

  // --- Inner config classes ---

  public static class ClusterConfig {
    @SerializedName("id")
    public String id = "iotdb-cluster";
  }

  public static class NodeConfig {
    @SerializedName("id")
    public String id;

    @SerializedName("rpc_url")
    public String rpcUrl;

    /**
     * Base URL of this node's HA Monitor REST API (e.g. {@code http://127.0.0.1:8081}). When set,
     * the peer monitor fetches this node's pipe status over HA↔HA HTTP ({@code /api/v1/pipes})
     * instead of opening a remote {@code SHOW PIPES} Session against this node's DataNode. Leaving
     * it empty disables the HTTP exchange for this node.
     */
    @SerializedName("api_url")
    public String apiUrl;
  }

  public static class MonitorConfig {
    @SerializedName("node_check_interval")
    public String nodeCheckInterval = "5s";

    @SerializedName("pipe_check_interval")
    public String pipeCheckInterval = "10s";

    @SerializedName("pipe_cache_ttl")
    public String pipeCacheTtl = "2s";

    @SerializedName("consistency_check_interval")
    public String consistencyCheckInterval = "10m";

    /** Connect/read timeout for the HA↔HA pipe-status HTTP exchange. */
    @SerializedName("remote_http_timeout")
    public String remoteHttpTimeout = "3s";

    /**
     * When {@code false} (default) the monitor NEVER opens a remote {@code SHOW PIPES} Session for
     * the peer node — it relies solely on the HA↔HA HTTP exchange and serves the last known-good
     * snapshot when the peer is unreachable. This keeps the peer's DataNode RPC threads and
     * ConfigNode client pool free of monitoring traffic (which otherwise competes with schema
     * transfers). Set {@code true} only to restore the legacy remote-Session fallback.
     */
    @SerializedName("remote_pipe_status_via_session")
    public boolean remotePipeStatusViaSession = false;

    /**
     * When {@code true} (default) a node whose own DataNode just transitioned DOWN→UP pushes a
     * one-shot hint to the peer's {@code /api/v1/peer-up} so the peer re-probes immediately instead
     * of waiting out its exponential backoff. Best-effort and fire-and-forget: if the peer is still
     * unreachable the hint is simply dropped and the normal poll path stays authoritative.
     */
    @SerializedName("notify_peer_on_up")
    public boolean notifyPeerOnUp = true;
  }

  public static class AlertConfig {
    @SerializedName("lag_warning_seconds")
    public int lagWarningSeconds = 5;

    @SerializedName("lag_critical_seconds")
    public int lagCriticalSeconds = 30;

    @SerializedName("cooldown_seconds")
    public Integer cooldownSeconds = 60;

    @SerializedName("webhook_url")
    public String webhookUrl = "";
  }

  public static class ApiConfig {
    @SerializedName("port")
    public int port = 8080;
  }

  public static class MetricsConfig {
    @SerializedName("enabled")
    public boolean enabled = true;

    @SerializedName("port")
    public int port = 9090;
  }

  public static class RecoveryConfig {
    @SerializedName("enabled")
    public boolean enabled = true;

    @SerializedName("auto_restart_wait")
    public String autoRestartWait = "30s";

    @SerializedName("node_startup_wait")
    public String nodeStartupWait = "30s";

    @SerializedName("check_interval")
    public String checkInterval = "15s";

    @SerializedName("long_offline_threshold_hours")
    public int longOfflineThresholdHours = 2;

    @SerializedName("ha_status_file_path")
    public String haStatusFilePath = "data/ha-cluster-status.json";

    /** Proactive FLUSH to seal active TsFiles so hybrid pipes switch to batch transfer. */
    @SerializedName("flush_enabled")
    public boolean flushEnabled = true;

    /** Backlog threshold (total remaining events) that triggers a proactive FLUSH. */
    @SerializedName("flush_remaining_events_threshold")
    public long flushRemainingEventsThreshold = 10000;

    /** Cooldown between two proactive FLUSHes on the same node. */
    @SerializedName("flush_cooldown")
    public String flushCooldown = "5m";

    /** Catch-up ETA (seconds) at/above which a proactive FLUSH is triggered. */
    @SerializedName("flush_eta_seconds")
    public long flushEtaSeconds = 600;

    /** Whether a STUCK (non-draining) backlog may trigger a proactive FLUSH. */
    @SerializedName("flush_on_stuck")
    public boolean flushOnStuck = true;

    /** Max cooldown multiplier when consecutive FLUSHes fail to drain the backlog. */
    @SerializedName("flush_futile_backoff_max")
    public int flushFutileBackoffMax = 8;
  }

  public static class SessionConfig {
    @SerializedName("username")
    public String username = "root";

    @SerializedName("password")
    public String password = "root";
  }

  public static class AuditConfig {
    /** Enable periodic data audit. Should be true only on the replication source node. */
    @SerializedName("enabled")
    public boolean enabled = false;

    /** Interval between two audit passes. */
    @SerializedName("interval")
    public String interval = "10m";

    /** Databases to audit; empty = auto-discover all user databases. */
    @SerializedName("databases")
    public List<String> databases = new ArrayList<>();

    /** Tables to audit within each database; empty = auto-discover all tables. */
    @SerializedName("tables")
    public List<String> tables = new ArrayList<>();

    /** Row-count difference tolerated before a WARNING fires (absorbs async replication lag). */
    @SerializedName("row_count_tolerance")
    public long rowCountTolerance = 0;

    /** Row-count difference at/above which the alert escalates to CRITICAL. */
    @SerializedName("critical_threshold")
    public long criticalThreshold = 10000;
  }

  public static class BacklogConfig {
    /** Enable intelligent pipe-backlog alerting. */
    @SerializedName("enabled")
    public boolean enabled = true;

    /** Interval between two backlog-monitor passes. */
    @SerializedName("interval")
    public String interval = "15s";

    /** Remaining events at/above which the backlog is graded WARNING. */
    @SerializedName("warning_remaining_events")
    public long warningRemainingEvents = 10000;

    /** Remaining events at/above which the backlog is graded CRITICAL. */
    @SerializedName("critical_remaining_events")
    public long criticalRemainingEvents = 500000;

    /** Estimated catch-up seconds at/above which the backlog is graded WARNING. */
    @SerializedName("warning_eta_seconds")
    public long warningEtaSeconds = 300;

    /** Estimated catch-up seconds at/above which the backlog is graded CRITICAL. */
    @SerializedName("critical_eta_seconds")
    public long criticalEtaSeconds = 3600;

    /** Non-draining backlog lasting this long is treated as STUCK and escalated to CRITICAL. */
    @SerializedName("stuck_window")
    public String stuckWindow = "3m";

    /** Assumed sink throughput (events/sec) used to derive an ETA when IoTDB reports none. */
    @SerializedName("assumed_throughput_eps")
    public double assumedThroughputEps = 10000.0;

    /** Cooldown between repeated backlog alerts for the same pipe. */
    @SerializedName("cooldown")
    public String cooldown = "5m";
  }
}
