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

package org.apache.iotdb.ha.monitor;

import org.apache.iotdb.ha.alert.Alerter;
import org.apache.iotdb.ha.checker.NodeHealthChecker;
import org.apache.iotdb.ha.checker.PipeInspector;
import org.apache.iotdb.ha.config.HaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Intelligent pipe-backlog alerting. Grades each RUNNING pipe's backlog into OK / WARNING /
 * CRITICAL from two dimensions — remaining-event count and estimated catch-up time (ETA) — and adds
 * a trend dimension: a backlog that stops draining while the pipe is still RUNNING is escalated to
 * CRITICAL as STUCK, because a shrinking backlog is benign while a frozen one signals a real
 * problem (sink degraded, type mismatch, resource starvation).
 *
 * <p>This unifies and supersedes the older hardcoded checks (a single {@code lag_increasing}
 * warning and a 500k-event long-offline gate) with configurable thresholds, hysteresis via the
 * alerter cooldown, and automatic clearing when a pipe drains back to healthy. It runs independently
 * of {@code recovery.enabled} so backlog observability is available even in monitor-only mode.
 *
 * <p>Only the LOCAL node's pipes are graded: in a symmetric active-active deployment each monitor
 * owns its own node, which avoids two monitors firing duplicate alerts for the same peer pipe.
 */
public class BacklogMonitor {
  private static final Logger logger = LoggerFactory.getLogger(BacklogMonitor.class);

  private final HaConfig config;
  private final PipeInspector pipeInspector;
  private final NodeHealthChecker healthChecker;
  private final Alerter alerter;

  /**
   * Per-pipe reference sample for drain-rate / stuck detection, keyed {@code nodeId:pipeName}. The
   * sample is refreshed only while the backlog is draining; when it stops draining the original
   * timestamp is retained so {@code now - timestamp} measures how long the backlog has been frozen.
   */
  private final Map<String, Sample> samples = new ConcurrentHashMap<>();

  private volatile List<PipeBacklog> lastReport = new ArrayList<>();
  private volatile long lastRunAt = 0L;
  private volatile String worstSeverity = "OK";

  public BacklogMonitor(
      HaConfig config,
      PipeInspector pipeInspector,
      NodeHealthChecker healthChecker,
      Alerter alerter) {
    this.config = config;
    this.pipeInspector = pipeInspector;
    this.healthChecker = healthChecker;
    this.alerter = alerter;
  }

  /** Run one backlog-monitor pass across the LOCAL node's pipes. */
  public void check() {
    if (!config.isBacklogEnabled()) {
      return;
    }
    long now = System.currentTimeMillis();
    List<PipeBacklog> report = new ArrayList<>();
    String worst = "OK";

    HaConfig.NodeConfig node = config.getLocalNode();
    if (healthChecker.isUp(node.id)) {
      List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
      for (PipeInspector.PipeInfo pipe : pipes) {
        PipeBacklog b = evaluate(node.id, pipe, now);
        report.add(b);
        if (severityRank(b.severity) > severityRank(worst)) {
          worst = b.severity;
        }
      }
    }

    this.lastReport = report;
    this.lastRunAt = now;
    this.worstSeverity = worst;
  }

  private PipeBacklog evaluate(String nodeId, PipeInspector.PipeInfo pipe, long now) {
    String key = nodeId + ":" + pipe.pipeName;
    boolean running = "RUNNING".equalsIgnoreCase(pipe.status);
    long remaining = Math.max(pipe.remainingEventCount, 0);

    PipeBacklog b = new PipeBacklog();
    b.node = nodeId;
    b.pipe = pipe.pipeName;
    b.status = pipe.status;
    b.remaining = remaining;

    // ETA: prefer IoTDB's own estimate; otherwise derive from the assumed sink throughput.
    long eta;
    if (pipe.estimatedRemainingSeconds > 0) {
      eta = pipe.estimatedRemainingSeconds;
    } else if (remaining > 0) {
      eta = (long) (remaining / config.getBacklogAssumedThroughputEps());
    } else {
      eta = 0;
    }
    b.etaSeconds = eta;

    // ── Drain-rate / stuck tracking ──
    long stuckWindowMs = config.getBacklogStuckWindowMs();
    boolean stuck = false;
    Sample prev = samples.get(key);
    if (running && pipe.remainingEventCount > 0) {
      if (prev == null) {
        samples.put(key, new Sample(now, pipe.remainingEventCount));
      } else {
        long dt = now - prev.timestamp;
        if (dt > 0) {
          b.drainRate = (prev.remaining - pipe.remainingEventCount) / (dt / 1000.0);
        }
        if (pipe.remainingEventCount < prev.remaining) {
          // Draining — reset the reference window.
          samples.put(key, new Sample(now, pipe.remainingEventCount));
        } else if (dt >= stuckWindowMs) {
          // Not draining for at least the stuck window — frozen backlog.
          stuck = true;
        }
        // else: not draining yet but within the window — keep the old sample so dt keeps growing.
      }
    } else {
      samples.remove(key);
    }
    b.stuck = stuck;

    // ── Grade severity ──
    String severity = grade(remaining, eta);
    if (stuck && severityRank(severity) < severityRank("CRITICAL")) {
      severity = "CRITICAL";
    }
    b.severity = severity;

    // ── Fire or clear ──
    String cooldownKey = "pipe_backlog:" + key;
    if ("OK".equals(severity)) {
      alerter.clearCooldown(cooldownKey);
      b.message = "healthy (remaining=" + remaining + ", eta=" + eta + "s)";
    } else {
      b.message = buildMessage(nodeId, pipe.pipeName, remaining, eta, b.drainRate, stuck);
      String type =
          stuck
              ? "pipe_backlog_stuck"
              : ("CRITICAL".equals(severity) ? "pipe_backlog_critical" : "pipe_backlog_warning");
      alerter.fire(type, severity, b.message, cooldownKey);
      if ("CRITICAL".equals(severity)) {
        logger.warn("Pipe backlog CRITICAL: {}", b.message);
      } else {
        logger.info("Pipe backlog WARNING: {}", b.message);
      }
    }
    return b;
  }

  /** Grade a backlog from remaining events and ETA; the more severe of the two wins. */
  private String grade(long remaining, long etaSeconds) {
    if (remaining >= config.getBacklogCriticalEvents()
        || etaSeconds >= config.getBacklogCriticalEtaSeconds()) {
      return "CRITICAL";
    }
    if (remaining >= config.getBacklogWarningEvents()
        || etaSeconds >= config.getBacklogWarningEtaSeconds()) {
      return "WARNING";
    }
    return "OK";
  }

  private static String buildMessage(
      String nodeId, String pipe, long remaining, long eta, double drainRate, boolean stuck) {
    StringBuilder sb = new StringBuilder();
    sb.append("Pipe ").append(pipe).append(" on ").append(nodeId).append(": ");
    if (stuck) {
      sb.append("backlog STUCK (not draining) — ");
    }
    sb.append(remaining)
        .append(" remaining events, ~")
        .append(eta)
        .append("s catch-up, drain rate ")
        .append(String.format("%.1f", drainRate))
        .append(" events/sec");
    return sb.toString();
  }

  private static int severityRank(String severity) {
    if ("CRITICAL".equals(severity)) {
      return 2;
    }
    if ("WARNING".equals(severity)) {
      return 1;
    }
    return 0;
  }

  /** Last pass as typed results (for Prometheus metric publishing). */
  public List<PipeBacklog> getBacklogResults() {
    return lastReport;
  }

  /** Last pass rendered as JSON-friendly maps (for the REST API). */
  public List<Map<String, Object>> getBacklogReport() {
    List<Map<String, Object>> out = new ArrayList<>();
    for (PipeBacklog b : lastReport) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("node", b.node);
      m.put("pipe", b.pipe);
      m.put("status", b.status);
      m.put("remaining_events", b.remaining);
      m.put("eta_seconds", b.etaSeconds);
      m.put("drain_rate_eps", Math.round(b.drainRate * 10.0) / 10.0);
      m.put("stuck", b.stuck);
      m.put("severity", b.severity);
      m.put("message", b.message);
      out.add(m);
    }
    return out;
  }

  public long getLastRunAt() {
    return lastRunAt;
  }

  public String getLastRunIso() {
    return lastRunAt == 0 ? null : Instant.ofEpochMilli(lastRunAt).toString();
  }

  public String getWorstSeverity() {
    return worstSeverity;
  }

  /** Drain-rate / stuck reference sample. */
  private static class Sample {
    final long timestamp;
    final long remaining;

    Sample(long timestamp, long remaining) {
      this.timestamp = timestamp;
      this.remaining = remaining;
    }
  }

  /** Per-pipe backlog assessment result. */
  public static class PipeBacklog {
    public String node;
    public String pipe;
    public String status = "UNKNOWN";
    public long remaining = 0;
    public long etaSeconds = 0;
    public double drainRate = 0.0;
    public boolean stuck = false;
    public String severity = "OK";
    public String message = "";
  }
}
