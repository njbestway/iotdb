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

package org.apache.iotdb.ha.checker;

import org.apache.iotdb.ha.config.HaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Inspects Pipe status via SHOW PIPE. Computes logical Channel state. */
public class PipeInspector {
  private static final Logger logger = LoggerFactory.getLogger(PipeInspector.class);

  private final HaConfig config;
  private final NodeJdbcConnectionManager connManager;

  public PipeInspector(HaConfig config, NodeJdbcConnectionManager connManager) {
    this.config = config;
    this.connManager = connManager;
  }

  /** Get pipe info from a specific node. Uses the shared JDBC connection. */
  public List<PipeInfo> getPipes(String nodeId) {
    HaConfig.NodeConfig nodeConfig = findNode(nodeId);
    if (nodeConfig == null) {
      return List.of();
    }
    return queryPipes(nodeConfig.rpcUrl);
  }

  /**
   * Compute channel state from pipe list. Evaluates all provided pipes as belonging to the channel
   * — callers are responsible for passing the correct pipe set (typically all pipes on the source
   * node).
   */
  public ChannelState computeChannelState(List<PipeInfo> pipes) {
    boolean historyRunning = false;
    boolean realtimeRunning = false;
    long totalRemaining = 0;

    for (PipeInfo pipe : pipes) {
      totalRemaining += pipe.remainingEventCount;
      String lowerName = pipe.pipeName.toLowerCase();
      if (lowerName.endsWith("_history") || lowerName.contains("history")) {
        historyRunning = "RUNNING".equalsIgnoreCase(pipe.status);
      }
      if (lowerName.endsWith("_realtime") || lowerName.contains("realtime")) {
        realtimeRunning = "RUNNING".equalsIgnoreCase(pipe.status);
      }
    }

    if (historyRunning && realtimeRunning) {
      return new ChannelState("HEALTHY", totalRemaining);
    } else if (historyRunning || realtimeRunning) {
      return new ChannelState("DEGRADED", totalRemaining);
    } else {
      return new ChannelState("FAILED", totalRemaining);
    }
  }

  private List<PipeInfo> queryPipes(String rpcUrl) {
    List<PipeInfo> result = new ArrayList<>();
    try {
      // Reuse the shared persistent connection — do NOT close it
      Connection conn = connManager.getConnection(rpcUrl);
      try (Statement stmt = conn.createStatement()) {
        boolean hasResultSet = stmt.execute("SHOW PIPES");
        ResultSet rs = hasResultSet ? stmt.getResultSet() : null;
        if (rs == null) {
          logger.warn("SHOW PIPES returned no result for {}", rpcUrl);
          return result;
        }

        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        // Build column name → index map (case-insensitive)
        Map<String, Integer> colIndex = new LinkedHashMap<>();
        for (int i = 1; i <= colCount; i++) {
          colIndex.put(meta.getColumnName(i).toLowerCase(), i);
        }

        while (rs.next()) {
          PipeInfo info = new PipeInfo();
          info.pipeName = getStringSafe(rs, colIndex, "id", "pipe name", "pipe_name", "pipe");
          info.status = getStringSafe(rs, colIndex, "state", "status", "pipestatus");
          info.remainingEventCount =
              parseLongSafe(
                  getStringSafe(rs, colIndex, "remainingeventcount", "remaining_event_count"));
          info.estimatedRemainingSeconds =
              parseLongSafe(
                  getStringSafe(
                      rs,
                      colIndex,
                      "estimatedremainingseconds",
                      "estimated_remaining_seconds",
                      "estimated_remaining_time"));
          result.add(info);
        }
      }
    } catch (Exception e) {
      logger.warn("SHOW PIPES failed for {}: {}", rpcUrl, e.getMessage());
      // Invalidate the broken connection so next call reconnects
      connManager.invalidate(rpcUrl);
    }
    return result;
  }

  private HaConfig.NodeConfig findNode(String nodeId) {
    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (node.id.equals(nodeId)) {
        return node;
      }
    }
    return null;
  }

  private static String getStringSafe(ResultSet rs, Map<String, Integer> cols, String... names) {
    for (String name : names) {
      Integer idx = cols.get(name.toLowerCase());
      if (idx != null) {
        try {
          return rs.getString(idx);
        } catch (Exception ignored) {
        }
      }
    }
    return "UNKNOWN";
  }

  /** Parse a string value as long, returning -1 if parsing fails or value is "Unknown". */
  private static long parseLongSafe(String value) {
    if (value == null || "Unknown".equalsIgnoreCase(value) || value.isEmpty()) {
      return -1;
    }
    try {
      // Handle decimal values like "0.00"
      if (value.contains(".")) {
        return (long) Double.parseDouble(value);
      }
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * Assess whether a node's pipes indicate a long-offline situation that needs manual intervention.
   *
   * <p>Logic: only flag intervention when the pipe has a large backlog AND the estimated catch-up
   * time exceeds a practical threshold. If remaining events are unknown (-1), do not count them. If
   * the pipe reports 0 remaining events, it has caught up — no intervention needed.
   */
  public LongOfflineAssessment assessLongOffline(List<PipeInfo> pipes, String nodeId) {
    long totalRemaining = 0;
    long maxEstimatedSeconds = 0;
    boolean hasKnownRemaining = false;

    for (PipeInfo pipe : pipes) {
      if (pipe.remainingEventCount > 0) {
        totalRemaining += pipe.remainingEventCount;
        hasKnownRemaining = true;
      }
      if (pipe.estimatedRemainingSeconds > 0) {
        maxEstimatedSeconds = Math.max(maxEstimatedSeconds, pipe.estimatedRemainingSeconds);
      }
    }

    // If no pipe has any remaining events, the node has caught up
    if (!hasKnownRemaining && maxEstimatedSeconds <= 0) {
      return new LongOfflineAssessment(0, 0, false);
    }

    // Estimate catch-up time: use IoTDB's estimate if available, otherwise rough estimate
    double estimatedHours;
    if (maxEstimatedSeconds > 0) {
      estimatedHours = maxEstimatedSeconds / 3600.0;
    } else if (totalRemaining > 0) {
      // Rough estimate: assume 10000 events/sec throughput
      estimatedHours = (totalRemaining / 10000.0) / 3600.0;
    } else {
      estimatedHours = 0;
    }

    // Only flag intervention when catch-up time is genuinely long (> 1 hour)
    // AND there is a significant backlog (> 500k events)
    boolean needsIntervention = totalRemaining > 500000 && estimatedHours > 1.0;
    return new LongOfflineAssessment(totalRemaining, estimatedHours, needsIntervention);
  }

  /** Single pipe info. */
  public static class PipeInfo {
    public String pipeName = "";
    public String status = "UNKNOWN";
    public long remainingEventCount = -1;
    public long estimatedRemainingSeconds = -1;
  }

  /** Logical channel state. */
  public static class ChannelState {
    public final String state;
    public final long remainingEvents;

    public ChannelState(String state, long remainingEvents) {
      this.state = state;
      this.remainingEvents = remainingEvents;
    }
  }

  /** Assessment result for long-offline situation. */
  public static class LongOfflineAssessment {
    public final long totalRemainingEvents;
    public final double estimatedCatchUpHours;
    public final boolean needsIntervention;

    public LongOfflineAssessment(
        long totalRemainingEvents, double estimatedCatchUpHours, boolean needsIntervention) {
      this.totalRemainingEvents = totalRemainingEvents;
      this.estimatedCatchUpHours = estimatedCatchUpHours;
      this.needsIntervention = needsIntervention;
    }
  }
}
