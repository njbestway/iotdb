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

package org.apache.iotdb.ha.recovery;

import org.apache.iotdb.ha.alert.Alerter;
import org.apache.iotdb.ha.checker.NodeHealthChecker;
import org.apache.iotdb.ha.checker.NodeJdbcConnectionManager;
import org.apache.iotdb.ha.checker.PipeInspector;
import org.apache.iotdb.ha.config.HaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Active recovery manager. Orchestrates automatic pipe restart, node recovery checks, long-offline
 * detection, and lag diagnosis.
 *
 * <p>Recovery philosophy: IoTDB auto-restart first, Monitor intervenes only when auto-restart fails
 * or is insufficient.
 */
public class RecoveryManager {
  private static final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);

  private final HaConfig config;
  private final PipeInspector pipeInspector;
  private final NodeHealthChecker healthChecker;
  private final Alerter alerter;
  private final NodeJdbcConnectionManager connManager;

  /** Tracks when a pipe was first seen in STOPPED/FAILED state. */
  private final Map<String, Long> pipeFailureDetectedAt = new ConcurrentHashMap<>();

  /** Tracks node UP transitions: nodeId → timestamp of first UP detection after DOWN. */
  private final Map<String, Long> nodeRecoveryDetectedAt = new ConcurrentHashMap<>();

  /** Tracks previous UP/DOWN state for transition detection. */
  private final Map<String, Boolean> previousNodeState = new ConcurrentHashMap<>();

  /** Lag tracking: pipeName → list of (timestamp, remainingEvents) for trend analysis. */
  private final Map<String, LagSample> lagTracking = new ConcurrentHashMap<>();

  public RecoveryManager(
      HaConfig config,
      PipeInspector pipeInspector,
      NodeHealthChecker healthChecker,
      Alerter alerter,
      NodeJdbcConnectionManager connManager) {
    this.config = config;
    this.pipeInspector = pipeInspector;
    this.healthChecker = healthChecker;
    this.alerter = alerter;
    this.connManager = connManager;

    // Initialize previous node state
    for (HaConfig.NodeConfig node : config.getNodes()) {
      previousNodeState.put(node.id, false);
    }
  }

  /**
   * Main recovery check. Called periodically by the scheduler. Evaluates pipe health, node
   * transitions, and triggers recovery actions.
   */
  public void check() {
    checkNodeTransitions();
    checkPipeRecovery();
    checkLongOffline();
    checkLagDiagnosis();
  }

  // ── 1. Pipe Auto-Restart Intervention ──────────────────────────────────

  /**
   * Checks all pipes on all UP nodes. If a pipe is STOPPED/FAILED and IoTDB auto-restart hasn't
   * recovered it within the configured timeout, actively restart it.
   */
  private void checkPipeRecovery() {
    long now = System.currentTimeMillis();
    long autoRestartWaitMs = config.getAutoRestartWaitMs();

    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (!healthChecker.isUp(node.id)) {
        continue;
      }

      List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
      if (pipes.isEmpty()) {
        // No pipes at all — this is unexpected
        alerter.fire(
            "pipe_missing",
            "CRITICAL",
            "No pipes found on node " + node.id + ", expected replication pipes",
            "pipe_missing:" + node.id);
        continue;
      }

      for (PipeInspector.PipeInfo pipe : pipes) {
        String key = node.id + ":" + pipe.pipeName;

        if ("STOPPED".equalsIgnoreCase(pipe.status) || "FAILED".equalsIgnoreCase(pipe.status)) {
          Long firstDetected = pipeFailureDetectedAt.get(key);

          if (firstDetected == null) {
            // First time seeing this pipe in failure state
            pipeFailureDetectedAt.put(key, now);
            logger.info(
                "Pipe {} on {} is {}, waiting for IoTDB auto-restart ({}s)",
                pipe.pipeName,
                node.id,
                pipe.status,
                autoRestartWaitMs / 1000);
            continue;
          }

          long elapsed = now - firstDetected;

          if (elapsed < autoRestartWaitMs) {
            // Still within auto-restart grace period
            logger.debug(
                "Pipe {} on {} still in auto-restart window ({}/{}s)",
                pipe.pipeName,
                node.id,
                elapsed / 1000,
                autoRestartWaitMs / 1000);
            continue;
          }

          // Auto-restart window exceeded — actively restart
          logger.warn(
              "IoTDB auto-restart did not recover pipe {} on {} after {}s, executing START PIPE",
              pipe.pipeName,
              node.id,
              autoRestartWaitMs / 1000);

          boolean restarted = executeStartPipe(node.rpcUrl, pipe.pipeName);
          if (restarted) {
            alerter.fire(
                "pipe_restarted",
                "WARNING",
                "Pipe "
                    + pipe.pipeName
                    + " on "
                    + node.id
                    + " was manually restarted after auto-restart timeout",
                "pipe_restarted:" + key);
            pipeFailureDetectedAt.remove(key);
          } else {
            alerter.fire(
                "pipe_restart_failed",
                "CRITICAL",
                "Failed to restart pipe " + pipe.pipeName + " on " + node.id,
                "pipe_restart_failed:" + key);
          }

        } else {
          // Pipe is not in failure state — clear tracking
          pipeFailureDetectedAt.remove(key);
        }
      }
    }
  }

  // ── 2. Node Recovery Check ────────────────────────────────────────────

  /** Detects DOWN→UP transitions and verifies pipes after node recovery. */
  private void checkNodeTransitions() {
    for (HaConfig.NodeConfig node : config.getNodes()) {
      boolean currentlyUp = healthChecker.isUp(node.id);
      Boolean previouslyUp = previousNodeState.get(node.id);

      if (currentlyUp && Boolean.FALSE.equals(previouslyUp)) {
        // Node just recovered from DOWN → UP
        Long recoveryTime = nodeRecoveryDetectedAt.get(node.id);
        if (recoveryTime == null) {
          nodeRecoveryDetectedAt.put(node.id, System.currentTimeMillis());
          logger.info("Node {} recovered (DOWN → UP), will verify pipes", node.id);
        }
      } else if (currentlyUp && nodeRecoveryDetectedAt.containsKey(node.id)) {
        // Node is UP and we detected a recent recovery — verify pipes
        verifyPipesAfterRecovery(node);
      }

      if (!currentlyUp && Boolean.TRUE.equals(previouslyUp)) {
        // Node just went DOWN
        logger.warn("Node {} went DOWN", node.id);
        nodeRecoveryDetectedAt.remove(node.id);
      }

      previousNodeState.put(node.id, currentlyUp);
    }
  }

  /** After node recovery, check all pipes and restart any that are stopped. */
  private void verifyPipesAfterRecovery(HaConfig.NodeConfig node) {
    Long recoveryTime = nodeRecoveryDetectedAt.get(node.id);
    if (recoveryTime == null) {
      return;
    }

    // Wait a bit for IoTDB to fully start up before checking pipes
    long waitMs = config.getNodeStartupWaitMs();
    if (System.currentTimeMillis() - recoveryTime < waitMs) {
      logger.debug("Node {} recovered, waiting {}s for IoTDB startup", node.id, waitMs / 1000);
      return;
    }

    logger.info("Node {} fully recovered, verifying pipes", node.id);
    List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);

    for (PipeInspector.PipeInfo pipe : pipes) {
      if ("STOPPED".equalsIgnoreCase(pipe.status) || "FAILED".equalsIgnoreCase(pipe.status)) {
        logger.info("Recovering pipe {} on recovered node {}", pipe.pipeName, node.id);
        boolean restarted = executeStartPipe(node.rpcUrl, pipe.pipeName);
        if (restarted) {
          alerter.fire(
              "pipe_restarted",
              "WARNING",
              "Pipe " + pipe.pipeName + " restarted on recovered node " + node.id,
              "pipe_restarted:" + node.id + ":" + pipe.pipeName);
        } else {
          alerter.fire(
              "pipe_restart_failed",
              "CRITICAL",
              "Failed to restart pipe " + pipe.pipeName + " on recovered node " + node.id,
              "pipe_restart_failed:" + node.id + ":" + pipe.pipeName);
        }
      }
    }

    // Clear recovery tracking — verification done
    nodeRecoveryDetectedAt.remove(node.id);
  }

  // ── 3. Long Offline Detection ─────────────────────────────────────────

  /** Tracks when the long-offline warning was last fired per node, to avoid log spam. */
  private final Map<String, Long> longOfflineWarnedAt = new ConcurrentHashMap<>();

  /**
   * Detects when a node has been offline long enough that pipe catch-up may be impractical.
   * Evaluates remaining events and estimates if pipe can handle it.
   *
   * <p>Cooldown: the same warning is only fired once per 10 minutes per node to avoid log spam
   * while the pipe is catching up.
   */
  private void checkLongOffline() {
    long now = System.currentTimeMillis();
    long cooldownMs = 10 * 60 * 1000L; // 10 minutes

    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (!healthChecker.isUp(node.id)) {
        continue;
      }

      List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
      PipeInspector.LongOfflineAssessment assessment =
          pipeInspector.assessLongOffline(pipes, node.id);

      if (assessment.needsIntervention) {
        String key = "long_offline:" + node.id;

        if (assessment.estimatedCatchUpHours > config.getLongOfflineThresholdHours()) {
          // Pipe catch-up would take too long — recommend TsFile copy
          String otherNodeId = getOtherNodeId(node.id);

          // Cooldown: only warn once per 10 minutes per node
          Long lastWarned = longOfflineWarnedAt.get(key);
          if (lastWarned == null || (now - lastWarned) > cooldownMs) {
            longOfflineWarnedAt.put(key, now);

            alerter.fire(
                "long_offline_tsfile_needed",
                "CRITICAL",
                String.format(
                    "Node %s has %d remaining events (~%.1fh catch-up). "
                        + "Pipe is still trying. If it does not converge, manual TsFile copy "
                        + "may be needed from %s to %s.",
                    node.id,
                    assessment.totalRemainingEvents,
                    assessment.estimatedCatchUpHours,
                    otherNodeId,
                    node.id),
                key);

            logger.warn(
                "Long offline on {}: {} remaining events (~{}h catch-up). "
                    + "Pipe is still running — wait for it to converge. "
                    + "If it does not converge after 30 min, manual recovery steps:\n"
                    + "  1. On source node ({}): run FLUSH to seal active TsFiles\n"
                    + "  2. Locate TsFiles under <{}-data-dir>/data/sequence/ and .../unsequence/\n"
                    + "  3. Copy the TsFile directories to the target node ({})\n"
                    + "  4. On target node ({}): LOAD '<tsfile-path>' for each file\n"
                    + "  Or use: LOAD '<directory>' RECURSIVE to load all files in a directory",
                node.id,
                assessment.totalRemainingEvents,
                String.format("%.1f", assessment.estimatedCatchUpHours),
                otherNodeId,
                otherNodeId,
                node.id,
                node.id);
          }
        } else {
          // Pipe can handle it, but warn about the lag (with cooldown)
          Long lastWarned = longOfflineWarnedAt.get(key);
          if (lastWarned == null || (now - lastWarned) > cooldownMs) {
            longOfflineWarnedAt.put(key, now);
            alerter.fire(
                "long_offline_catching_up",
                "WARNING",
                String.format(
                    "Node %s catching up: %d remaining events (~%.1fh)",
                    node.id, assessment.totalRemainingEvents, assessment.estimatedCatchUpHours),
                key);
          }
        }
      } else {
        // Node has caught up — clear the warning tracker
        longOfflineWarnedAt.remove("long_offline:" + node.id);
      }
    }
  }

  // ── 4. Lag Diagnosis ──────────────────────────────────────────────────

  /**
   * Tracks pipe lag over time. If remaining events are not decreasing despite pipe RUNNING,
   * diagnoses the issue and alerts.
   */
  private void checkLagDiagnosis() {
    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (!healthChecker.isUp(node.id)) {
        continue;
      }

      List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
      long now = System.currentTimeMillis();

      for (PipeInspector.PipeInfo pipe : pipes) {
        if (!"RUNNING".equalsIgnoreCase(pipe.status)) {
          lagTracking.remove(pipe.pipeName);
          continue;
        }

        if (pipe.remainingEventCount <= 0) {
          lagTracking.remove(pipe.pipeName);
          continue;
        }

        // Track lag sample
        LagSample prev = lagTracking.get(pipe.pipeName);
        if (prev == null) {
          lagTracking.put(pipe.pipeName, new LagSample(now, pipe.remainingEventCount));
          continue;
        }

        long timeDelta = now - prev.timestamp;
        long eventDelta = pipe.remainingEventCount - prev.remainingEvents;

        // Check every 60s at minimum
        if (timeDelta < 60000) {
          continue;
        }

        if (eventDelta > 0) {
          // Lag is INCREASING — pipe is not keeping up
          double eventsPerSec = (double) eventDelta / (timeDelta / 1000.0);
          alerter.fire(
              "lag_increasing",
              "WARNING",
              String.format(
                  "Pipe %s on %s: lag increasing at %.1f events/sec "
                      + "(remaining=%d, was=%d, delta=%ds)",
                  pipe.pipeName,
                  node.id,
                  eventsPerSec,
                  pipe.remainingEventCount,
                  prev.remainingEvents,
                  timeDelta / 1000),
              "lag_increasing:" + node.id + ":" + pipe.pipeName);
        }

        // Update tracking
        lagTracking.put(pipe.pipeName, new LagSample(now, pipe.remainingEventCount));
      }
    }
  }

  // ── JDBC Execution ────────────────────────────────────────────────────

  /** Execute START PIPE via JDBC. Returns true if successful. */
  private boolean executeStartPipe(String rpcUrl, String pipeName) {
    String sql = "START PIPE " + pipeName;
    return executeJdbc(rpcUrl, sql);
  }

  /** Execute LOAD TsFile via JDBC. Returns true if successful. */
  public boolean executeLoadTsFile(String rpcUrl, String tsFilePath) {
    // Validate path to prevent SQL injection
    if (tsFilePath == null || tsFilePath.isEmpty()) {
      logger.error("LOAD failed: tsFilePath is null or empty");
      return false;
    }
    if (tsFilePath.contains("'")
        || tsFilePath.contains(";")
        || tsFilePath.contains("--")
        || tsFilePath.contains("/*")) {
      logger.error("LOAD rejected: path contains suspicious characters: {}", tsFilePath);
      return false;
    }
    String sql = "LOAD '" + tsFilePath + "'";
    return executeJdbc(rpcUrl, sql);
  }

  private boolean executeJdbc(String rpcUrl, String sql) {
    try {
      // Reuse the shared persistent connection — do NOT close it
      Connection conn = connManager.getConnection(rpcUrl);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
        logger.info("Executed SQL on {}: {}", rpcUrl, sql);
        return true;
      }
    } catch (Exception e) {
      logger.error("SQL execution failed on {} [{}]: {}", rpcUrl, sql, e.getMessage());
      connManager.invalidate(rpcUrl);
      return false;
    }
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private String getOtherNodeId(String nodeId) {
    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (!node.id.equals(nodeId)) {
        return node.id;
      }
    }
    return null;
  }

  /** Get recovery status summary for API. */
  public Map<String, Object> getRecoveryStatus() {
    Map<String, Object> status = new java.util.LinkedHashMap<>();
    status.put("pending_pipe_restarts", pipeFailureDetectedAt.size());
    status.put("node_recoveries_pending", nodeRecoveryDetectedAt.size());
    status.put("lag_tracking_count", lagTracking.size());
    status.put("auto_restart_wait_ms", config.getAutoRestartWaitMs());
    return status;
  }

  /** Lag tracking sample. */
  private static class LagSample {
    final long timestamp;
    final long remainingEvents;

    LagSample(long timestamp, long remainingEvents) {
      this.timestamp = timestamp;
      this.remainingEvents = remainingEvents;
    }
  }
}
