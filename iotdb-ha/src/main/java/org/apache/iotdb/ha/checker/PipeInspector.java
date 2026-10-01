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
import org.apache.iotdb.isession.SessionDataSet;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Inspects Pipe status via SHOW PIPE. Computes logical Channel state. */
public class PipeInspector {
  private static final Logger logger = LoggerFactory.getLogger(PipeInspector.class);

  private final HaConfig config;
  private final NodeSessionManager sessionManager;

  /** Reports remote liveness back into the health state machine (see {@link #fetchPipes}). */
  private final NodeHealthChecker healthChecker;

  /**
   * L2: in-process supplier of the LOCAL node's pipe status, read directly from ConfigNode memory
   * (injected by {@code HaMonitorBootstrap}). When present, the local node bypasses {@code SHOW
   * PIPES} entirely — no Session, no RPC, no DataNode→ConfigNode client borrow. Uses JDK types only
   * so the confignode module needs no compile-time dependency on iotdb-ha. Each map carries keys
   * {@code id}, {@code state}, {@code remainingEventCount}, {@code estimatedRemainingSeconds}.
   */
  private volatile Supplier<List<Map<String, Object>>> localPipeStatusProvider;

  /** L1: short-TTL cache collapsing redundant getPipes calls into one real fetch per node. */
  private final Map<String, CachedPipes> pipeCache = new ConcurrentHashMap<>();

  /** Per-node locks so a slow fetch for one node does not block fetches for the other. */
  private final Map<String, Object> nodeLocks = new ConcurrentHashMap<>();

  private final long cacheTtlMs;
  private final long httpTimeoutMs;

  public PipeInspector(
      HaConfig config, NodeSessionManager sessionManager, NodeHealthChecker healthChecker) {
    this.config = config;
    this.sessionManager = sessionManager;
    this.healthChecker = healthChecker;
    this.cacheTtlMs = config.getPipeCacheTtlMs();
    this.httpTimeoutMs = config.getRemoteHttpTimeoutMs();
  }

  /**
   * Register the in-process local pipe-status supplier (see {@link #localPipeStatusProvider}).
   * Called once at startup by the embedded bootstrap; safe to call with {@code null} to disable.
   */
  public void setLocalPipeStatusProvider(Supplier<List<Map<String, Object>>> provider) {
    this.localPipeStatusProvider = provider;
    if (provider != null) {
      logger.info(
          "Local in-memory pipe status provider registered (SHOW PIPES bypassed for local node {})",
          config.getLocalNode().id);
    }
  }

  /** Get pipe info from a specific node. Cached for {@code pipe_cache_ttl}; shares one Session. */
  public List<PipeInfo> getPipes(String nodeId) {
    HaConfig.NodeConfig nodeConfig = findNode(nodeId);
    if (nodeConfig == null) {
      return List.of();
    }

    CachedPipes cached = pipeCache.get(nodeId);
    if (cached != null && System.currentTimeMillis() - cached.fetchedAt < cacheTtlMs) {
      return cached.pipes;
    }

    synchronized (lockFor(nodeId)) {
      // Re-check under the lock: a concurrent caller may have just refreshed the cache.
      cached = pipeCache.get(nodeId);
      if (cached != null && System.currentTimeMillis() - cached.fetchedAt < cacheTtlMs) {
        return cached.pipes;
      }
      List<PipeInfo> fresh = fetchPipes(nodeId, nodeConfig);
      if (fresh != null) {
        pipeCache.put(nodeId, new CachedPipes(System.currentTimeMillis(), fresh));
        return fresh;
      }
      // Fetch failed (e.g. peer HA monitor unreachable and the remote-Session fallback disabled):
      // serve the last known-good snapshot regardless of TTL rather than reporting a false empty
      // state that would trip channel/pipe alarms. Only our cache is stale — we deliberately never
      // touch the peer's DataNode to refresh it.
      return cached != null ? cached.pipes : List.of();
    }
  }

  /**
   * Force an out-of-band fetch for {@code nodeId}, bypassing the short-TTL cache, and fold the
   * result into the health state machine exactly like {@link #getPipes} does. Used when the peer
   * pushes a "I'm back UP" hint: it lets us confirm liveness and refresh pipes within ~1 RTT instead
   * of serving a stale (DOWN) cached snapshot or waiting for the next scheduled poll. For a REMOTE
   * node this reuses the same merged HA↔HA HTTP call (pipe status + {@code data_node_up} self-check),
   * so no extra TCP probe or peer DataNode client borrow is introduced.
   */
  public List<PipeInfo> getPipesNow(String nodeId) {
    HaConfig.NodeConfig nodeConfig = findNode(nodeId);
    if (nodeConfig == null) {
      return List.of();
    }
    synchronized (lockFor(nodeId)) {
      List<PipeInfo> fresh = fetchPipes(nodeId, nodeConfig);
      if (fresh != null) {
        pipeCache.put(nodeId, new CachedPipes(System.currentTimeMillis(), fresh));
        return fresh;
      }
      // Fetch failed — keep the last known-good snapshot rather than reporting a false empty state.
      CachedPipes cached = pipeCache.get(nodeId);
      return cached != null ? cached.pipes : List.of();
    }
  }

  /**
   * Fetch pipe status for a node, returning {@code null} when the status could not be obtained so
   * the caller can serve a stale snapshot:
   *
   * <ul>
   *   <li>LOCAL node: in-memory provider (zero RPC) when registered, else a local {@code SHOW
   *       PIPES} Session against this node's own DataNode.
   *   <li>REMOTE node: HA↔HA HTTP exchange ({@code GET <api_url>/api/v1/pipes}) when {@code
   *       api_url} is configured. The peer answers from its OWN in-memory read, so this never
   *       borrows a DataNode→ConfigNode client on the peer. A remote {@code SHOW PIPES} Session is
   *       used only if HTTP is unavailable AND {@code remote_pipe_status_via_session} is enabled.
   * </ul>
   */
  private List<PipeInfo> fetchPipes(String nodeId, HaConfig.NodeConfig nodeConfig) {
    boolean isLocal = nodeId.equals(config.getLocalNode().id);

    if (isLocal) {
      Supplier<List<Map<String, Object>>> provider = localPipeStatusProvider;
      if (provider != null) {
        try {
          List<Map<String, Object>> raw = provider.get();
          if (raw != null) {
            return fromProvider(raw);
          }
          // null signals the in-memory read was unavailable (e.g. not leader yet) — fall back.
        } catch (Exception e) {
          logger.warn(
              "Local in-memory pipe status failed for {}, falling back to SHOW PIPES: {}",
              nodeId,
              e.getMessage());
        }
      }
      // Local SHOW PIPES hits this node's own DataNode only — no cross-node pressure.
      return queryPipes(nodeConfig.rpcUrl);
    }

    // Remote node: ONE HA↔HA HTTP call both fetches pipe status AND decides liveness, replacing the
    // old separate TCP probe of the peer's DataNode RPC port.
    String apiUrl = nodeConfig.apiUrl;
    if (apiUrl != null && !apiUrl.isEmpty()) {
      HttpPipeResult viaHttp = fetchPipesViaHttp(nodeId, apiUrl);
      if (viaHttp.ok) {
        // HTTP 200 proves the peer JVM (HA monitor) is alive. Mark the node UP only when the peer's
        // own data_node_up self-check also passed, so recovery (START PIPE → peer rpc port) is never
        // attempted before the data plane is truly ready.
        if (healthChecker != null) {
          healthChecker.recordProbeResult(nodeId, viaHttp.dataNodeUp);
        }
        return viaHttp.pipes;
      }
      // HTTP failed/timed out → peer is DOWN (this is now the sole remote liveness signal).
      if (healthChecker != null) {
        healthChecker.recordProbeResult(nodeId, false);
      }
      if (!config.isRemotePipeStatusViaSessionEnabled()) {
        // Peer HA monitor unreachable and the legacy fallback disabled — signal failure so the
        // caller serves the stale snapshot instead of opening a remote SHOW PIPES Session.
        return null;
      }
    }
    return queryPipes(nodeConfig.rpcUrl);
  }

  private Object lockFor(String nodeId) {
    return nodeLocks.computeIfAbsent(nodeId, k -> new Object());
  }

  /** Map the in-process provider rows into {@link PipeInfo}, mirroring {@link #queryPipes}. */
  private static List<PipeInfo> fromProvider(List<Map<String, Object>> raw) {
    List<PipeInfo> result = new ArrayList<>(raw.size());
    for (Map<String, Object> m : raw) {
      PipeInfo info = new PipeInfo();
      Object id = m.get("id");
      info.pipeName = id != null ? id.toString() : "UNKNOWN";
      Object state = m.get("state");
      info.status = state != null ? state.toString() : "UNKNOWN";
      info.remainingEventCount = toLong(m.get("remainingEventCount"), -1);
      info.estimatedRemainingSeconds = toLong(m.get("estimatedRemainingSeconds"), -1);
      result.add(info);
    }
    return result;
  }

  private static long toLong(Object v, long def) {
    if (v instanceof Number) {
      return ((Number) v).longValue();
    }
    return v != null ? parseLongSafe(v.toString()) : def;
  }

  /** A cached pipe-status snapshot with its fetch timestamp. */
  private static final class CachedPipes {
    final long fetchedAt;
    final List<PipeInfo> pipes;

    CachedPipes(long fetchedAt, List<PipeInfo> pipes) {
      this.fetchedAt = fetchedAt;
      this.pipes = pipes;
    }
  }

  /**
   * Outcome of one HA↔HA pipe-status HTTP call: whether it succeeded, the peer's self-reported
   * data-plane liveness, and the parsed pipes. A single result drives BOTH the pipe cache and the
   * remote node's UP/DOWN state.
   */
  private static final class HttpPipeResult {
    final boolean ok;
    final boolean dataNodeUp;
    final List<PipeInfo> pipes;

    HttpPipeResult(boolean ok, boolean dataNodeUp, List<PipeInfo> pipes) {
      this.ok = ok;
      this.dataNodeUp = dataNodeUp;
      this.pipes = pipes;
    }

    static HttpPipeResult failed() {
      return new HttpPipeResult(false, false, List.of());
    }
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
    SessionDataSet dataSet = null;
    try {
      dataSet = sessionManager.executeQuery(rpcUrl, "SHOW PIPES");

      List<String> columnNames = dataSet.getColumnNames();
      if (columnNames == null || columnNames.isEmpty()) {
        logger.warn("SHOW PIPES returned no columns for {}", rpcUrl);
        return result;
      }

      // Build column name → 1-based index map (case-insensitive)
      Map<String, Integer> colIndex = new LinkedHashMap<>();
      for (int i = 0; i < columnNames.size(); i++) {
        colIndex.put(columnNames.get(i).toLowerCase(), i + 1);
      }

      SessionDataSet.DataIterator it = dataSet.iterator();
      while (it.next()) {
        PipeInfo info = new PipeInfo();
        info.pipeName = getIterString(it, colIndex, "id", "pipe name", "pipe_name", "pipe");
        info.status = getIterString(it, colIndex, "state", "status", "pipestatus");
        info.remainingEventCount =
            parseLongSafe(
                getIterString(it, colIndex, "remainingeventcount", "remaining_event_count"));
        info.estimatedRemainingSeconds =
            parseLongSafe(
                getIterString(
                    it,
                    colIndex,
                    "estimatedremainingseconds",
                    "estimated_remaining_seconds",
                    "estimated_remaining_time"));
        result.add(info);
      }
    } catch (Exception e) {
      logger.warn("SHOW PIPES failed for {}: {}", rpcUrl, e.getMessage());
    } finally {
      if (dataSet != null) {
        try {
          dataSet.closeOperationHandle();
        } catch (Exception ignored) {
          // quiet
        }
      }
    }
    return result;
  }

  /**
   * Fetch a peer node's pipe status over the HA↔HA HTTP exchange ({@code GET
   * <api_url>/api/v1/pipes}). The peer's monitor answers from its own in-memory ConfigNode read, so
   * this avoids opening a remote {@code SHOW PIPES} Session (which would borrow a DataNode→
   * ConfigNode client on the peer and compete with schema transfers). This single call ALSO carries
   * the peer's liveness (HTTP reachability + its {@code data_node_up} self-check), so no separate
   * TCP probe of the peer is needed.
   *
   * @return an {@link HttpPipeResult}; {@code ok=false} on any failure so the caller can mark the
   *     peer DOWN and serve the stale snapshot.
   */
  private HttpPipeResult fetchPipesViaHttp(String nodeId, String apiUrl) {
    String base = apiUrl.endsWith("/") ? apiUrl.substring(0, apiUrl.length() - 1) : apiUrl;
    String url = base + "/api/v1/pipes";
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) new URL(url).openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout((int) httpTimeoutMs);
      conn.setReadTimeout((int) httpTimeoutMs);
      conn.setRequestProperty("Accept", "application/json");
      int code = conn.getResponseCode();
      if (code != HttpURLConnection.HTTP_OK) {
        logger.warn("Peer HA pipe endpoint {} returned HTTP {} for node {}", url, code, nodeId);
        return HttpPipeResult.failed();
      }
      return parsePipesPayload(readAll(conn.getInputStream()));
    } catch (Exception e) {
      logger.warn(
          "HA-HA pipe status exchange failed for node {} ({}): {}", nodeId, url, e.getMessage());
      return HttpPipeResult.failed();
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Parse the peer payload {@code {"node":..,"data_node_up":bool,"pipes":[{"id","state",
   * "remainingEventCount","estimatedRemainingSeconds"}]}}. The optional {@code data_node_up} is the
   * peer's own DataNode RPC self-check; when absent (older peer) it defaults to {@code true} so HTTP
   * 200 alone still means UP.
   */
  private static HttpPipeResult parsePipesPayload(String body) {
    List<PipeInfo> result = new ArrayList<>();
    if (body == null || body.isEmpty()) {
      return new HttpPipeResult(true, true, result);
    }
    JsonObject root = JsonParser.parseString(body).getAsJsonObject();
    JsonElement dnUp = root.get("data_node_up");
    boolean dataNodeUp = dnUp == null || dnUp.isJsonNull() || dnUp.getAsBoolean();
    JsonArray arr = root.getAsJsonArray("pipes");
    if (arr != null) {
      for (JsonElement el : arr) {
        JsonObject o = el.getAsJsonObject();
        PipeInfo info = new PipeInfo();
        info.pipeName = jsonStr(o, "id", "UNKNOWN");
        info.status = jsonStr(o, "state", "UNKNOWN");
        info.remainingEventCount = jsonLong(o, "remainingEventCount", -1);
        info.estimatedRemainingSeconds = jsonLong(o, "estimatedRemainingSeconds", -1);
        result.add(info);
      }
    }
    return new HttpPipeResult(true, dataNodeUp, result);
  }

  private static String jsonStr(JsonObject o, String key, String def) {
    JsonElement e = o.get(key);
    return e != null && !e.isJsonNull() ? e.getAsString() : def;
  }

  private static long jsonLong(JsonObject o, String key, long def) {
    JsonElement e = o.get(key);
    if (e == null || e.isJsonNull()) {
      return def;
    }
    try {
      return e.getAsLong();
    } catch (Exception ex) {
      return def;
    }
  }

  private static String readAll(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] chunk = new byte[4096];
    int n;
    while ((n = in.read(chunk)) != -1) {
      buf.write(chunk, 0, n);
    }
    return new String(buf.toByteArray(), StandardCharsets.UTF_8);
  }

  private HaConfig.NodeConfig findNode(String nodeId) {
    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (node.id.equals(nodeId)) {
        return node;
      }
    }
    return null;
  }

  /** Get string value from DataIterator by trying multiple possible column names. */
  private static String getIterString(
      SessionDataSet.DataIterator it, Map<String, Integer> cols, String... names) {
    for (String name : names) {
      Integer idx = cols.get(name.toLowerCase());
      if (idx != null) {
        try {
          if (!it.isNull(idx)) {
            return it.getString(idx);
          }
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
