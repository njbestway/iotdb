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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Checks IoTDB node health via TCP socket probe. Uses exponential backoff when node is DOWN. */
public class NodeHealthChecker {
  private static final Logger logger = LoggerFactory.getLogger(NodeHealthChecker.class);

  private final HaConfig config;
  private final Map<String, NodeStatus> statusMap = new ConcurrentHashMap<>();

  public NodeHealthChecker(HaConfig config) {
    this.config = config;
    for (HaConfig.NodeConfig node : config.getNodes()) {
      statusMap.put(node.id, new NodeStatus());
    }
  }

  /**
   * Probe a single node over TCP and update its status. After the HA↔HA HTTP merge this is used for
   * the LOCAL node only; the remote node is no longer TCP-probed here — its liveness is reported
   * back by {@code PipeInspector} from the pipe-status HTTP exchange (see {@link
   * #recordProbeResult}). Returns true if healthy.
   */
  public boolean check(String nodeId) {
    HaConfig.NodeConfig nodeConfig = findNode(nodeId);
    if (nodeConfig == null) {
      logger.warn("Unknown node: {}", nodeId);
      return false;
    }

    NodeStatus status = statusMap.get(nodeId);
    long now = System.currentTimeMillis();

    // Exponential backoff: skip check if within backoff interval
    if (now - status.lastCheckTime < status.currentBackoffMs) {
      return status.isUp;
    }

    boolean healthy = doCheck(nodeConfig.rpcUrl);
    recordProbeResult(nodeId, healthy);
    return healthy;
  }

  /**
   * Update a node's health state machine from an externally-obtained probe result, without
   * performing the probe itself. The remote node's liveness is fed in here by {@code PipeInspector}:
   * a single {@code GET /api/v1/pipes} HTTP call both fetches pipe status AND decides UP/DOWN (UP
   * requires HTTP 200 plus the peer's own {@code data_node_up} self-check), so one request replaces
   * the old separate cross-node TCP liveness probe.
   */
  public void recordProbeResult(String nodeId, boolean healthy) {
    NodeStatus status = statusMap.get(nodeId);
    if (status == null) {
      return;
    }
    long now = System.currentTimeMillis();
    status.lastCheckTime = now;

    if (healthy) {
      if (!status.isUp) {
        logger.info("Node {} is UP (was DOWN for {}ms)", nodeId, now - status.lastDownTime);
      }
      status.isUp = true;
      status.lastUpTime = now;
      status.currentBackoffMs = config.getNodeCheckIntervalMs();
      status.consecutiveFailures = 0;
    } else {
      if (status.isUp) {
        logger.warn("Node {} is DOWN", nodeId);
        status.lastDownTime = now;
      }
      status.isUp = false;
      status.consecutiveFailures++;
      // Exponential backoff: 5s → 10s → 20s → 40s → 60s (cap)
      status.currentBackoffMs =
          Math.min(
              config.getNodeCheckIntervalMs() * (1L << Math.min(status.consecutiveFailures, 4)),
              60000);
    }
  }

  /**
   * Whether {@code nodeId} is due for a probe, honoring exponential backoff. The remote poll loop
   * uses this so a node known to be DOWN is not hammered every cycle.
   */
  public boolean dueForCheck(String nodeId) {
    NodeStatus status = statusMap.get(nodeId);
    if (status == null) {
      return false;
    }
    return System.currentTimeMillis() - status.lastCheckTime >= status.currentBackoffMs;
  }

  /** TCP-probe the LOCAL node only (loopback, zero cross-node traffic). */
  public void checkLocal() {
    check(config.getLocalNode().id);
  }

  /**
   * Probe every node that still relies on a direct TCP check. After the HA↔HA HTTP merge this is
   * only the LOCAL node: the remote node's liveness is reported back from the pipe-status HTTP
   * exchange, so we no longer open a TCP connection to the peer's DataNode RPC port (which used to
   * appear as harmless {@code Session-0-null ... is closing} noise in the peer's log).
   */
  public void checkAll() {
    checkLocal();
  }

  public boolean isUp(String nodeId) {
    NodeStatus status = statusMap.get(nodeId);
    return status != null && status.isUp;
  }

  public Map<String, NodeStatus> getStatusMap() {
    return statusMap;
  }

  private boolean doCheck(String rpcUrl) {
    // Use TCP socket check - more reliable than JDBC in ConfigNode process
    String[] parts = rpcUrl.split(":");
    if (parts.length != 2) {
      logger.warn("Invalid rpcUrl format: {}", rpcUrl);
      return false;
    }
    String host = parts[0];
    int port;
    try {
      port = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      logger.warn("Invalid port in rpcUrl: {}", rpcUrl);
      return false;
    }
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), 3000);
      return true;
    } catch (IOException e) {
      logger.warn("Health check failed for {}:{} - {}", host, port, e.getMessage());
      return false;
    }
  }

  private HaConfig.NodeConfig findNode(String nodeId) {
    for (HaConfig.NodeConfig node : config.getNodes()) {
      if (node.id.equals(nodeId)) {
        return node;
      }
    }
    return null;
  }

  /** Node status with backoff tracking. */
  public static class NodeStatus {
    public volatile boolean isUp = false;
    public volatile long lastCheckTime = 0;
    public volatile long lastUpTime = 0;
    public volatile long lastDownTime = 0;
    public volatile long currentBackoffMs = 5000;
    public volatile int consecutiveFailures = 0;

    public long getUptimeMs() {
      if (!isUp) {
        return 0;
      }
      return System.currentTimeMillis() - lastUpTime;
    }
  }
}
