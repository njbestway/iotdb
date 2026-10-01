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
import org.apache.iotdb.ha.checker.NodeSessionManager;
import org.apache.iotdb.ha.checker.PipeInspector;
import org.apache.iotdb.ha.config.HaConfig;
import org.apache.iotdb.ha.monitor.BacklogMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Active recovery manager. Orchestrates automatic pipe restart, node recovery checks, long-offline
 * recovery runbook logging, and proactive FLUSH. Backlog grading/alerting lives in {@code
 * BacklogMonitor}; this class focuses on recovery ACTIONS.
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
  private final NodeSessionManager sessionManager;

  /**
   * Optional source of rich per-pipe catch-up signals (drain rate, stuck, ETA). {@code null} when
   * backlog monitoring is disabled, in which case the FLUSH trigger falls back to {@code
   * PipeInspector.assessLongOffline}. Passed in by {@code HaMonitorMain}.
   */
  private final BacklogMonitor backlogMonitor;

  /** Tracks when a pipe was first seen in STOPPED/FAILED state. */
  private final Map<String, Long> pipeFailureDetectedAt = new ConcurrentHashMap<>();

  /** Tracks node UP transitions: nodeId → timestamp of first UP detection after DOWN. */
  private final Map<String, Long> nodeRecoveryDetectedAt = new ConcurrentHashMap<>();

  /** Tracks previous UP/DOWN state for transition detection. */
  private final Map<String, Boolean> previousNodeState = new ConcurrentHashMap<>();

  /** Tracks the last proactive FLUSH time per node, to enforce the flush cooldown window. */
  private final Map<String, Long> lastFlushAt = new ConcurrentHashMap<>();

  /** Remaining-event snapshot taken right after each FLUSH, to judge whether it helped drain. */
  private final Map<String, Long> postFlushRemaining = new ConcurrentHashMap<>();

  /** Consecutive-futile-FLUSH counter per node; drives the cooldown backoff multiplier. */
  private final Map<String, Integer> futileFlushStreak = new ConcurrentHashMap<>();

  /** Nodes for which the "FLUSH suppressed: sink DOWN" INFO line was already logged this outage. */
  private final Set<String> flushSkipLogged = ConcurrentHashMap.newKeySet();

  public RecoveryManager(
      HaConfig config,
      PipeInspector pipeInspector,
      NodeHealthChecker healthChecker,
      Alerter alerter,
      NodeSessionManager sessionManager,
      BacklogMonitor backlogMonitor) {
    this.config = config;
    this.pipeInspector = pipeInspector;
    this.healthChecker = healthChecker;
    this.alerter = alerter;
    this.sessionManager = sessionManager;
    this.backlogMonitor = backlogMonitor;

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
    checkFlushTrigger();
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

      // Backlog grading/alerting is owned by BacklogMonitor; here we only emit the manual TsFile
      // recovery runbook when the backlog is so large that pipe catch-up alone is impractical.
      if (assessment.needsIntervention
          && assessment.estimatedCatchUpHours > config.getLongOfflineThresholdHours()) {
        String key = "long_offline:" + node.id;
        String otherNodeId = getOtherNodeId(node.id);

        // Cooldown: only log the runbook once per 10 minutes per node
        Long lastWarned = longOfflineWarnedAt.get(key);
        if (lastWarned == null || (now - lastWarned) > cooldownMs) {
          longOfflineWarnedAt.put(key, now);
          logger.warn(
              "Long offline on {}: {} remaining events (~{}h catch-up). "
                  + "Pipe is still running — proactive FLUSH (checkFlushTrigger) seals active "
                  + "TsFiles so the hybrid pipe switches to batch transfer; wait for it to "
                  + "converge. If it still does not converge after 30 min, manual recovery:\n"
                  + "  1. Locate TsFiles under <{}-data-dir>/data/sequence/ and .../unsequence/\n"
                  + "  2. Copy the TsFile directories to the target node ({})\n"
                  + "  3. On target node ({}): LOAD '<tsfile-path>' for each file\n"
                  + "  Or use: LOAD '<directory>' RECURSIVE to load all files in a directory",
              node.id,
              assessment.totalRemainingEvents,
              String.format("%.1f", assessment.estimatedCatchUpHours),
              otherNodeId,
              node.id,
              node.id);
        }
      } else {
        // Node has caught up — clear the runbook tracker
        longOfflineWarnedAt.remove("long_offline:" + node.id);
      }
    }
  }

  // ── 4. Intelligent proactive FLUSH to accelerate catch-up ───────────────

  /**
   * Intelligently FLUSHes the LOCAL node to accelerate pipe catch-up. Sealing active TsFiles lets
   * the hybrid pipe switch from slow per-tablet transfer to fast whole-file batch transfer
   * (10-100x), so a FLUSH is warranted whenever catch-up is falling behind.
   *
   * <p>Compared with a blunt "remaining events >= N" gate, this decides from three things:
   *
   * <ul>
   *   <li><b>Multi-signal trigger</b>: flush when catch-up is slow by ANY signal - remaining events
   *       over threshold, ETA over {@code flush_eta_seconds}, or a STUCK (non-draining) backlog.
   *       Signals come from {@code BacklogMonitor} when available, else {@code assessLongOffline}.
   *   <li><b>Usefulness gating</b>: SKIP the flush when the outbound pipe's sink (remote node) is
   *       DOWN. An undeliverable backlog cannot be accelerated by sealing files, and flushing then
   *       only churns small files. That is exactly the max-backlog moment where the old blunt gate
   *       fired needlessly; the outage is already covered by node_down / backlog_stuck alerts.
   *   <li><b>Futile backoff</b>: if a flush did not make the backlog drain, the cooldown is
   *       multiplied (up to {@code flush_futile_backoff_max}) so repeated useless flushes stop.
   * </ul>
   *
   * <p>Only the local node is flushed: in a symmetric active-active deployment each monitor owns its
   * own node, avoiding two monitors flushing the same peer and avoiding remote FLUSH Sessions. This
   * is the automatic replacement for the old "manual FLUSH + LOAD TsFile" long-offline advice.
   */
  private void checkFlushTrigger() {
    if (!config.isFlushEnabled()) {
      return;
    }
    HaConfig.NodeConfig localNode = config.getLocalNode();
    if (!healthChecker.isUp(localNode.id)) {
      return;
    }

    FlushSignals sig = collectFlushSignals(localNode.id);

    // Nothing to accelerate - clear cooldown/backoff state so the next real backlog starts fresh.
    if (sig.remaining <= 0) {
      lastFlushAt.remove(localNode.id);
      postFlushRemaining.remove(localNode.id);
      futileFlushStreak.remove(localNode.id);
      flushSkipLogged.remove(localNode.id);
      return;
    }

    // ── Usefulness gating: a FLUSH cannot accelerate an undeliverable backlog ──
    if (config.getNodes().size() >= 2) {
      HaConfig.NodeConfig remoteNode = config.getRemoteNode();
      if (!healthChecker.isUp(remoteNode.id)) {
        // Log once per outage at INFO (operationally meaningful: we are deliberately NOT flushing),
        // then stay quiet to avoid a 15s-cadence log storm for the whole outage.
        if (flushSkipLogged.add(localNode.id)) {
          logger.info(
              "FLUSH suppressed on {}: sink {} is DOWN - sealing TsFiles cannot accelerate an "
                  + "undeliverable backlog ({} remaining events, eta {}s); deferring to "
                  + "node_down/backlog alerts until the peer returns",
              localNode.id,
              remoteNode.id,
              sig.remaining,
              sig.etaSeconds);
        } else {
          logger.debug(
              "FLUSH still suppressed on {}: sink {} DOWN ({} remaining events)",
              localNode.id,
              remoteNode.id,
              sig.remaining);
        }
        return;
      }
    }
    // Sink reachable again - allow a fresh INFO skip line if it goes down later.
    flushSkipLogged.remove(localNode.id);

    // ── Multi-signal trigger: catch-up too slow by events, ETA, or a stuck backlog ──
    boolean byEvents = sig.remaining >= config.getFlushRemainingEventsThreshold();
    boolean byEta = sig.etaSeconds >= config.getFlushEtaSeconds();
    boolean byStuck = config.isFlushOnStuck() && sig.stuck;
    if (!byEvents && !byEta && !byStuck) {
      // Backlog present but draining acceptably - no intervention needed.
      return;
    }

    // ── Cooldown with futile backoff ──
    long now = System.currentTimeMillis();
    Long last = lastFlushAt.get(localNode.id);
    int streak = futileFlushStreak.getOrDefault(localNode.id, 0);
    long multiplier = backoffMultiplier(streak);
    long cooldownMs = config.getFlushCooldownMs() * multiplier;
    if (last != null && (now - last) < cooldownMs) {
      logger.debug(
          "FLUSH suppressed for {} ({} remaining events, {}s cooldown left, futile backoff x{})",
          localNode.id,
          sig.remaining,
          (cooldownMs - (now - last)) / 1000,
          multiplier);
      return;
    }

    // ── Effectiveness feedback: did the previous FLUSH actually help the backlog drain? ──
    updateFutileStreak(localNode.id, sig.remaining);

    String reason = describeTrigger(byEvents, byEta, byStuck);
    logger.info(
        "Intelligent FLUSH on {} [{}]: remaining={}, eta={}s, stuck={}, drain={} ev/s (source={}) "
            + "-> sealing active TsFiles so the hybrid pipe switches to batch transfer",
        localNode.id,
        reason,
        sig.remaining,
        sig.etaSeconds,
        sig.stuck,
        String.format("%.1f", sig.drainRate),
        sig.source);

    boolean flushed = executeFlush(localNode.rpcUrl);
    lastFlushAt.put(localNode.id, now);
    if (flushed) {
      postFlushRemaining.put(localNode.id, sig.remaining);
      alerter.fire(
          "flush_triggered",
          "WARNING",
          String.format(
              "Intelligent FLUSH on %s to accelerate pipe catch-up [%s]: %d remaining events, "
                  + "eta %ds, stuck=%s",
              localNode.id, reason, sig.remaining, sig.etaSeconds, sig.stuck),
          "flush_triggered:" + localNode.id);
    } else {
      alerter.fire(
          "flush_failed",
          "CRITICAL",
          "Failed to execute FLUSH on " + localNode.id + " for pipe catch-up acceleration",
          "flush_failed:" + localNode.id);
    }
  }

  /**
   * Collects catch-up signals for a node. Prefers {@code BacklogMonitor}'s rich per-pipe assessment
   * (drain rate / stuck / ETA); falls back to {@code assessLongOffline} when backlog monitoring is
   * disabled. Across pipes the worst (max) values win, since one lagging pipe justifies a flush.
   */
  private FlushSignals collectFlushSignals(String nodeId) {
    FlushSignals s = new FlushSignals();
    if (backlogMonitor != null) {
      boolean any = false;
      for (BacklogMonitor.PipeBacklog b : backlogMonitor.getBacklogResults()) {
        if (!nodeId.equals(b.node)) {
          continue;
        }
        any = true;
        s.remaining = Math.max(s.remaining, Math.max(b.remaining, 0));
        s.etaSeconds = Math.max(s.etaSeconds, b.etaSeconds);
        s.drainRate = Math.max(s.drainRate, b.drainRate);
        s.stuck = s.stuck || b.stuck;
      }
      if (any) {
        s.source = "backlog-monitor";
        return s;
      }
    }
    // Fallback: derive from the long-offline assessment (event count + catch-up hours).
    List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(nodeId);
    PipeInspector.LongOfflineAssessment a = pipeInspector.assessLongOffline(pipes, nodeId);
    s.remaining = Math.max(a.totalRemainingEvents, 0);
    s.etaSeconds = (long) (a.estimatedCatchUpHours * 3600.0);
    s.source = "long-offline-assessment";
    return s;
  }

  /**
   * Judges whether the last FLUSH helped the backlog drain and updates the futile streak that drives
   * the cooldown backoff. No progress below the post-flush snapshot counts as futile.
   */
  private void updateFutileStreak(String nodeId, long currentRemaining) {
    Long baseline = postFlushRemaining.get(nodeId);
    if (baseline == null) {
      return; // no prior flush to judge yet
    }
    if (currentRemaining < baseline) {
      // Draining - the flush (and/or the sink) is working; reset backoff and track the new low.
      futileFlushStreak.remove(nodeId);
      postFlushRemaining.put(nodeId, currentRemaining);
    } else {
      futileFlushStreak.merge(nodeId, 1, Integer::sum);
    }
  }

  /** Exponential backoff multiplier (1, 2, 4, ...) capped by {@code flush_futile_backoff_max}. */
  private long backoffMultiplier(int futileStreak) {
    if (futileStreak <= 0) {
      return 1L;
    }
    long cap = config.getFlushFutileBackoffMax();
    long m = 1L << Math.min(futileStreak, 30);
    return Math.min(m, cap);
  }

  private static String describeTrigger(boolean byEvents, boolean byEta, boolean byStuck) {
    StringBuilder sb = new StringBuilder();
    if (byEvents) {
      sb.append("remaining>=threshold");
    }
    if (byEta) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("eta>=limit");
    }
    if (byStuck) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("stuck");
    }
    return sb.toString();
  }

  /** Catch-up signals distilled for the FLUSH decision. */
  private static class FlushSignals {
    long remaining = 0;
    long etaSeconds = 0;
    double drainRate = 0.0;
    boolean stuck = false;
    String source = "unknown";
  }

  // ── SQL Execution ────────────────────────────────────────────────────

  /** Execute START PIPE via Session. Returns true if successful. */
  private boolean executeStartPipe(String rpcUrl, String pipeName) {
    String sql = "START PIPE " + pipeName;
    return executeSql(rpcUrl, sql);
  }

  /** Execute a global FLUSH via Session to seal active TsFiles. Returns true if successful. */
  private boolean executeFlush(String rpcUrl) {
    return executeSql(rpcUrl, "FLUSH");
  }

  /** Execute LOAD TsFile via Session. Returns true if successful. */
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
    return executeSql(rpcUrl, sql);
  }

  private boolean executeSql(String rpcUrl, String sql) {
    return sessionManager.executeNonQuery(rpcUrl, sql);
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
    status.put("auto_restart_wait_ms", config.getAutoRestartWaitMs());
    status.put("flush_enabled", config.isFlushEnabled());
    status.put("flush_nodes_tracked", lastFlushAt.size());
    return status;
  }
}
