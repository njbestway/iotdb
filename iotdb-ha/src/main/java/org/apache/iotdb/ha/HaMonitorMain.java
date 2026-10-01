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

package org.apache.iotdb.ha;

import org.apache.iotdb.ha.alert.Alerter;
import org.apache.iotdb.ha.api.ApiServer;
import org.apache.iotdb.ha.audit.DataAuditor;
import org.apache.iotdb.ha.checker.NodeHealthChecker;
import org.apache.iotdb.ha.checker.NodeSessionManager;
import org.apache.iotdb.ha.checker.PipeInspector;
import org.apache.iotdb.ha.cluster.ClusterAggregator;
import org.apache.iotdb.ha.cluster.ClusterSyncWriter;
import org.apache.iotdb.ha.cluster.PeerNotifier;
import org.apache.iotdb.ha.config.HaConfig;
import org.apache.iotdb.ha.monitor.BacklogMonitor;
import org.apache.iotdb.ha.recovery.RecoveryManager;
import org.apache.iotdb.ha.util.HaScheduledExecutorUtil;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Main entry point for the IoTDB HA Monitor. */
public class HaMonitorMain {
  private static final Logger logger = LoggerFactory.getLogger(HaMonitorMain.class);

  // Prometheus metrics
  private static final Gauge NODE_UP =
      Gauge.build()
          .name("iotdb_ha_node_up")
          .help("Node health status (1=UP, 0=DOWN)")
          .labelNames("node")
          .register();
  private static final Gauge PIPE_STATE =
      Gauge.build()
          .name("iotdb_ha_pipe_state")
          .help("Pipe state (1=RUNNING, 0=other)")
          .labelNames("pipe", "status")
          .register();
  private static final Gauge CHANNEL_STATE =
      Gauge.build()
          .name("iotdb_ha_channel_state")
          .help("Channel state (1=HEALTHY, 0=other)")
          .labelNames("channel", "state")
          .register();
  private static final Gauge REMAINING_EVENTS =
      Gauge.build()
          .name("iotdb_ha_pipe_remaining_events")
          .help("Pipe remaining events")
          .labelNames("pipe")
          .register();
  private static final Counter ALERT_TOTAL =
      Counter.build()
          .name("iotdb_ha_alert_total")
          .help("Total alerts fired")
          .labelNames("type")
          .register();
  private static final Counter RECOVERY_TOTAL =
      Counter.build()
          .name("iotdb_ha_recovery_total")
          .help("Total recovery actions executed")
          .labelNames("action", "result")
          .register();
  private static final Gauge AUDIT_ROW_DIFF =
      Gauge.build()
          .name("iotdb_ha_audit_row_diff")
          .help("Data audit row-count difference (source - target) per table")
          .labelNames("database", "table")
          .register();
  private static final Gauge AUDIT_CONSISTENT =
      Gauge.build()
          .name("iotdb_ha_audit_consistent")
          .help("Data audit last pass consistency (1=consistent, 0=divergent)")
          .register();
  private static final Gauge BACKLOG_REMAINING =
      Gauge.build()
          .name("iotdb_ha_backlog_remaining_events")
          .help("Pipe backlog remaining events per pipe")
          .labelNames("node", "pipe")
          .register();
  private static final Gauge BACKLOG_ETA =
      Gauge.build()
          .name("iotdb_ha_backlog_eta_seconds")
          .help("Pipe backlog estimated catch-up seconds per pipe")
          .labelNames("node", "pipe")
          .register();
  private static final Gauge BACKLOG_DRAIN_RATE =
      Gauge.build()
          .name("iotdb_ha_backlog_drain_rate_eps")
          .help("Pipe backlog drain rate (events/sec, positive=draining) per pipe")
          .labelNames("node", "pipe")
          .register();
  private static final Gauge BACKLOG_SEVERITY =
      Gauge.build()
          .name("iotdb_ha_backlog_severity")
          .help("Pipe backlog severity (0=OK, 1=WARNING, 2=CRITICAL) per pipe")
          .labelNames("node", "pipe")
          .register();

  private final HaConfig config;
  private final NodeSessionManager sessionManager;
  private final NodeHealthChecker healthChecker;
  private final PipeInspector pipeInspector;
  private final ClusterAggregator clusterAggregator;
  private final Alerter alerter;
  private final RecoveryManager recoveryManager;
  private final DataAuditor dataAuditor;
  private final BacklogMonitor backlogMonitor;
  private final ClusterSyncWriter clusterSyncWriter;
  private final PeerNotifier peerNotifier;
  private final ApiServer apiServer;
  private final ScheduledExecutorService scheduler;
  private HTTPServer prometheusServer;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  public HaMonitorMain(HaConfig config) throws IOException {
    this.config = config;
    this.sessionManager =
        new NodeSessionManager(config.getSessionUsername(), config.getSessionPassword());
    this.healthChecker = new NodeHealthChecker(config);
    this.pipeInspector = new PipeInspector(config, sessionManager, healthChecker);
    this.clusterAggregator = new ClusterAggregator(config, healthChecker, pipeInspector);
    this.alerter = new Alerter(config);
    this.backlogMonitor =
        config.isBacklogEnabled()
            ? new BacklogMonitor(config, pipeInspector, healthChecker, alerter)
            : null;
    this.recoveryManager =
        config.isRecoveryEnabled()
            ? new RecoveryManager(
                config, pipeInspector, healthChecker, alerter, sessionManager, backlogMonitor)
            : null;
    this.dataAuditor =
        config.isAuditEnabled()
            ? new DataAuditor(config, sessionManager, healthChecker, alerter)
            : null;
    this.clusterSyncWriter = new ClusterSyncWriter(config, healthChecker, pipeInspector);
    // Best-effort UP-push layer on top of polling: when THIS node's DataNode recovers we nudge the
    // peer to re-probe immediately instead of waiting out its exponential backoff (≤60s → ~1 RTT).
    this.peerNotifier = new PeerNotifier(config, healthChecker, pipeInspector);
    this.healthChecker.setOnLocalNodeUp(this.peerNotifier::announceUp);
    this.apiServer =
        new ApiServer(
            config,
            clusterAggregator,
            alerter,
            recoveryManager,
            dataAuditor,
            backlogMonitor,
            pipeInspector,
            healthChecker,
            peerNotifier);
    this.scheduler = Executors.newScheduledThreadPool(4);
  }

  /**
   * Register an in-process supplier returning the LOCAL node's pipe status read directly from
   * ConfigNode memory. Injected by {@code HaMonitorBootstrap} (confignode module) via reflection so
   * that module keeps no compile-time dependency on iotdb-ha — only JDK types cross the boundary.
   * When set, {@link PipeInspector} bypasses {@code SHOW PIPES} for the local node entirely, which
   * removes the per-poll DataNode→ConfigNode client borrow that competes with schema transfers.
   *
   * <p>Each map carries keys {@code id}, {@code state}, {@code remainingEventCount}, {@code
   * estimatedRemainingSeconds}; returning {@code null} signals "unavailable, fall back to SHOW
   * PIPES".
   */
  public void setLocalPipeStatusProvider(Supplier<List<Map<String, Object>>> provider) {
    this.pipeInspector.setLocalPipeStatusProvider(provider);
  }

  public void start() throws IOException {
    logger.info(
        "IoTDB HA Monitor starting: cluster={}, nodes={}",
        config.getClusterId(),
        config.getNodes().size());

    // Start API server
    apiServer.start();

    // Start Prometheus metrics server
    if (config.isMetricsEnabled()) {
      prometheusServer = new HTTPServer(config.getMetricsPort());
      logger.info("Prometheus metrics server started on port {}", config.getMetricsPort());
    }

    // Schedule node health checks (delay to allow DataNode to start)
    HaScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        this::runNodeHealthCheck,
        10000,
        config.getNodeCheckIntervalMs(),
        TimeUnit.MILLISECONDS);

    // Schedule pipe inspections. This loop now ALSO drives remote liveness: each remote poll is a
    // single HTTP call that fetches pipes AND reports UP/DOWN back into the health checker, so it
    // runs at the (faster) node-check interval rather than the pipe-check interval.
    HaScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        this::runPipeInspection,
        5000,
        config.getNodeCheckIntervalMs(),
        TimeUnit.MILLISECONDS);

    // Schedule recovery checks (if enabled)
    if (recoveryManager != null) {
      logger.info(
          "Active recovery ENABLED: auto-restart wait={}s, check interval={}s",
          config.getAutoRestartWaitMs() / 1000,
          config.getRecoveryCheckIntervalMs() / 1000);
      HaScheduledExecutorUtil.safelyScheduleAtFixedRate(
          scheduler,
          this::runRecoveryCheck,
          10000,
          config.getRecoveryCheckIntervalMs(),
          TimeUnit.MILLISECONDS);
    } else {
      logger.info("Active recovery DISABLED (monitor-only mode)");
    }

    // Schedule periodic data audit (if enabled). Runs on the replication source node only; compares
    // row counts of every replicated table against the target and alerts on divergence.
    if (dataAuditor != null) {
      logger.info(
          "Data audit ENABLED: interval={}s, row_count_tolerance={}, critical_threshold={}",
          config.getAuditIntervalMs() / 1000,
          config.getAuditRowCountTolerance(),
          config.getAuditCriticalThreshold());
      HaScheduledExecutorUtil.safelyScheduleAtFixedRate(
          scheduler,
          this::runDataAudit,
          15000,
          config.getAuditIntervalMs(),
          TimeUnit.MILLISECONDS);
    }

    // Schedule intelligent pipe-backlog alerting (if enabled). Independent of recovery so backlog
    // grading works even in monitor-only mode.
    if (backlogMonitor != null) {
      logger.info(
          "Pipe backlog alerting ENABLED: interval={}s, warning>={} events / >={}s ETA, "
              + "critical>={} events / >={}s ETA, stuck_window={}s",
          config.getBacklogIntervalMs() / 1000,
          config.getBacklogWarningEvents(),
          config.getBacklogWarningEtaSeconds(),
          config.getBacklogCriticalEvents(),
          config.getBacklogCriticalEtaSeconds(),
          config.getBacklogStuckWindowMs() / 1000);
      HaScheduledExecutorUtil.safelyScheduleAtFixedRate(
          scheduler,
          this::runBacklogCheck,
          8000,
          config.getBacklogIntervalMs(),
          TimeUnit.MILLISECONDS);
    }

    // Resolve HA status file path to absolute (so DataNode can find it reliably)
    resolveStatusFileAbsolutePath();

    // Schedule cluster status sync (writes to local file + DB for SHOW CLUSTER)
    // Short delay — DataNode needs the file ASAP for SHOW CLUSTER
    HaScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        () -> {
          try {
            clusterSyncWriter.sync();
          } catch (Exception e) {
            logger.error("Cluster sync error", e);
          }
        },
        3000,
        config.getNodeCheckIntervalMs(),
        TimeUnit.MILLISECONDS);
    logger.info("Cluster status sync enabled: file={}", config.getHaStatusFilePath());

    logger.info("IoTDB HA Monitor started successfully");
  }

  public void stop() {
    logger.info("IoTDB HA Monitor stopping...");
    scheduler.shutdown();
    try {
      scheduler.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    apiServer.stop();
    peerNotifier.close();
    if (prometheusServer != null) {
      prometheusServer.close();
    }
    sessionManager.close();
    shutdownLatch.countDown();
    logger.info("IoTDB HA Monitor stopped");
  }

  /** Block the calling thread until {@link #stop()} is called. */
  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  private void runNodeHealthCheck() {
    try {
      healthChecker.checkAll();

      // Update Prometheus metrics
      for (HaConfig.NodeConfig node : config.getNodes()) {
        NODE_UP.labels(node.id).set(healthChecker.isUp(node.id) ? 1 : 0);
      }

      // Check for alerts
      boolean allDown = true;
      for (HaConfig.NodeConfig node : config.getNodes()) {
        if (healthChecker.isUp(node.id)) {
          allDown = false;
          // Clear cooldown when node recovers, so next failure fires immediately
          alerter.clearCooldown("node_down:" + node.id);
        }
      }
      if (allDown) {
        alerter.fire("node_down", "CRITICAL", "All nodes are DOWN", "node_down:all");
        ALERT_TOTAL.labels("node_down").inc();
      } else {
        for (HaConfig.NodeConfig node : config.getNodes()) {
          if (!healthChecker.isUp(node.id)) {
            alerter.fire(
                "node_down", "CRITICAL", "Node " + node.id + " is DOWN", "node_down:" + node.id);
            ALERT_TOTAL.labels("node_down").inc();
          }
        }
      }
    } catch (Exception e) {
      logger.error("Node health check error", e);
    }
  }

  private void runPipeInspection() {
    try {
      String localId = config.getLocalNode().id;
      for (HaConfig.NodeConfig node : config.getNodes()) {
        if (node.id.equals(localId)) {
          // Local node: liveness comes from the loopback TCP probe in runNodeHealthCheck; pipes come
          // from the in-memory provider (zero RPC). Skip only if this node's own RPC is not up yet.
          if (!healthChecker.isUp(node.id)) {
            continue;
          }
        } else {
          // Remote node: ONE HTTP call fetches pipes AND reports liveness back into healthChecker
          // (see PipeInspector.fetchPipes). Skip while in exponential backoff so a DOWN peer is not
          // hammered; when due, getPipes() triggers the real fetch (cache TTL < check interval).
          if (!healthChecker.dueForCheck(node.id)) {
            continue;
          }
        }
        List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
        for (PipeInspector.PipeInfo pipe : pipes) {
          // Update Prometheus metrics
          REMAINING_EVENTS.labels(pipe.pipeName).set(pipe.remainingEventCount);
          PIPE_STATE
              .labels(pipe.pipeName, pipe.status)
              .set("RUNNING".equalsIgnoreCase(pipe.status) ? 1 : 0);

          // Check for pipe failures
          if ("STOPPED".equalsIgnoreCase(pipe.status) || "FAILED".equalsIgnoreCase(pipe.status)) {
            alerter.fire(
                "pipe_failed",
                "CRITICAL",
                "Pipe " + pipe.pipeName + " on " + node.id + " is " + pipe.status,
                "pipe_failed:" + node.id + ":" + pipe.pipeName);
            ALERT_TOTAL.labels("pipe_failed").inc();
          }
        }

        // Check channel states — all pipes on this node belong to its replication channel
        PipeInspector.ChannelState channelState = pipeInspector.computeChannelState(pipes);
        CHANNEL_STATE
            .labels(node.id, channelState.state)
            .set("HEALTHY".equals(channelState.state) ? 1 : 0);
        if ("FAILED".equals(channelState.state)) {
          alerter.fire(
              "channel_failed",
              "CRITICAL",
              "Channel from " + node.id + " is FAILED",
              "channel_failed:" + node.id);
          ALERT_TOTAL.labels("channel_failed").inc();
        }
      }
    } catch (Exception e) {
      logger.error("Pipe inspection error", e);
    }
  }

  private void runRecoveryCheck() {
    if (recoveryManager == null) {
      return;
    }
    try {
      recoveryManager.check();
    } catch (Exception e) {
      logger.error("Recovery check error", e);
    }
  }

  private void runDataAudit() {
    if (dataAuditor == null) {
      return;
    }
    try {
      dataAuditor.audit();
      // Publish Prometheus metrics from the fresh report.
      for (DataAuditor.AuditResult r : dataAuditor.getAuditResults()) {
        AUDIT_ROW_DIFF.labels(r.database, r.table).set(r.diff);
      }
      AUDIT_CONSISTENT.set(dataAuditor.isLastRunConsistent() ? 1 : 0);
    } catch (Exception e) {
      logger.error("Data audit error", e);
    }
  }

  private void runBacklogCheck() {
    if (backlogMonitor == null) {
      return;
    }
    try {
      backlogMonitor.check();
      // Publish Prometheus metrics from the fresh report.
      for (BacklogMonitor.PipeBacklog b : backlogMonitor.getBacklogResults()) {
        BACKLOG_REMAINING.labels(b.node, b.pipe).set(b.remaining);
        BACKLOG_ETA.labels(b.node, b.pipe).set(b.etaSeconds);
        BACKLOG_DRAIN_RATE.labels(b.node, b.pipe).set(b.drainRate);
        int rank =
            "CRITICAL".equals(b.severity) ? 2 : ("WARNING".equals(b.severity) ? 1 : 0);
        BACKLOG_SEVERITY.labels(b.node, b.pipe).set(rank);
      }
    } catch (Exception e) {
      logger.error("Backlog check error", e);
    }
  }

  /**
   * Resolve the HA status file path to an absolute path. The file is written by ConfigNode JVM
   * (this process) and read by DataNode JVM (a different process). Using an absolute path ensures
   * both processes can find the file regardless of their working directories.
   *
   * <p>Strategy: resolve relative to CONFIGNODE_HOME if the path is not already absolute.
   */
  private void resolveStatusFileAbsolutePath() {
    String filePath = config.getHaStatusFilePath();
    java.nio.file.Path path = java.nio.file.Paths.get(filePath);
    if (path.isAbsolute()) {
      logger.info("HA status file path is already absolute: {}", filePath);
      return;
    }

    // Try CONFIGNODE_HOME first (set by start script)
    String configNodeHome = System.getProperty("CONFIGNODE_HOME");
    if (configNodeHome != null && !configNodeHome.isEmpty()) {
      java.nio.file.Path resolved =
          java.nio.file.Paths.get(configNodeHome).resolve(filePath).normalize();
      config.setHaStatusFilePath(resolved.toString());
      logger.info("HA status file resolved to absolute path (via CONFIGNODE_HOME): {}", resolved);
      return;
    }

    // Fallback: resolve against CWD and store as absolute
    java.nio.file.Path resolved = path.toAbsolutePath().normalize();
    config.setHaStatusFilePath(resolved.toString());
    logger.info("HA status file resolved to absolute path (via CWD): {}", resolved);
  }

  public static void main(String[] args) {
    String configPath = "config.json";
    if (args.length > 0) {
      configPath = args[0];
    }

    try {
      HaConfig config = HaConfig.load(configPath);
      HaMonitorMain monitor = new HaMonitorMain(config);

      // Shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(monitor::stop));

      monitor.start();

      // Keep main thread alive until shutdown
      monitor.awaitShutdown();
    } catch (Exception e) {
      logger.error("Failed to start IoTDB HA Monitor", e);
      System.exit(1);
    }
  }
}
