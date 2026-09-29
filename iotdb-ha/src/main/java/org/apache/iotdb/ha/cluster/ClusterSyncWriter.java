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

package org.apache.iotdb.ha.cluster;

import org.apache.iotdb.ha.checker.NodeHealthChecker;
import org.apache.iotdb.ha.checker.PipeInspector;
import org.apache.iotdb.ha.config.HaConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Syncs HA cluster status to a local file. The file is read by ShowClusterTask to augment SHOW
 * CLUSTER output with HA peer node information.
 *
 * <p>File location: configurable via HaConfig, default {@code data/ha-cluster-status.json}
 *
 * <p>Note: Database write (root._ha) was disabled because it generates continuous WAL events that
 * are captured by replication Pipes, causing phantom reference count to grow indefinitely. The JSON
 * file is sufficient for SHOW CLUSTER augmentation.
 */
public class ClusterSyncWriter {
  private static final Logger logger = LoggerFactory.getLogger(ClusterSyncWriter.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private final HaConfig config;
  private final NodeHealthChecker healthChecker;
  private final PipeInspector pipeInspector;

  public ClusterSyncWriter(
      HaConfig config, NodeHealthChecker healthChecker, PipeInspector pipeInspector) {
    this.config = config;
    this.healthChecker = healthChecker;
    this.pipeInspector = pipeInspector;
  }

  /**
   * Main sync method. Called periodically. Writes HA cluster status to local JSON file (for
   * ShowClusterTask to read).
   *
   * <p>Database write was intentionally removed to avoid generating pipe events that cause phantom
   * reference growth. See class-level javadoc for details.
   */
  public void sync() {
    try {
      Map<String, Object> status = buildStatusMap();
      writeToFile(status);
    } catch (Exception e) {
      logger.error("Cluster sync write error", e);
    }
  }

  /** Build the HA cluster status map. */
  private Map<String, Object> buildStatusMap() {
    Map<String, Object> status = new LinkedHashMap<>();
    status.put("cluster_id", config.getClusterId());
    status.put("timestamp", Instant.now().toString());
    status.put("managed_by", "iotdb-ha");

    List<Map<String, Object>> haNodes = new ArrayList<>();
    for (HaConfig.NodeConfig node : config.getNodes()) {
      Map<String, Object> nodeInfo = new LinkedHashMap<>();
      nodeInfo.put("node_id", node.id);
      nodeInfo.put("node_type", "HA_Peer");
      nodeInfo.put("status", healthChecker.isUp(node.id) ? "Running" : "Unreachable");

      // Parse host and port from rpc_url (format: host:port)
      String[] parts = node.rpcUrl.split(":");
      nodeInfo.put("host", parts.length > 0 ? parts[0] : node.rpcUrl);
      nodeInfo.put("port", parts.length > 1 ? Integer.parseInt(parts[1]) : 6667);

      // Additional HA info
      NodeHealthChecker.NodeStatus nodeStatus = healthChecker.getStatusMap().get(node.id);
      if (nodeStatus != null && nodeStatus.isUp) {
        nodeInfo.put("uptime_ms", nodeStatus.getUptimeMs());
      }

      // Pipe info
      if (healthChecker.isUp(node.id)) {
        List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
        List<Map<String, Object>> pipeList = new ArrayList<>();
        for (PipeInspector.PipeInfo pipe : pipes) {
          Map<String, Object> pipeInfo = new LinkedHashMap<>();
          pipeInfo.put("name", pipe.pipeName);
          pipeInfo.put("status", pipe.status);
          pipeInfo.put("remaining_events", pipe.remainingEventCount);
          pipeList.add(pipeInfo);
        }
        nodeInfo.put("pipes", pipeList);
      }

      haNodes.add(nodeInfo);
    }
    status.put("ha_nodes", haNodes);

    return status;
  }

  /** Write status to JSON file. This file is read by ShowClusterTask in the DataNode JVM. */
  private void writeToFile(Map<String, Object> status) {
    String filePath = config.getHaStatusFilePath();
    Path path = Paths.get(filePath);

    // Create parent directories if needed
    try {
      if (path.getParent() != null) {
        java.nio.file.Files.createDirectories(path.getParent());
      }
    } catch (Exception e) {
      logger.debug("Could not create directories for {}: {}", filePath, e.getMessage());
    }

    try (FileWriter writer = new FileWriter(path.toFile())) {
      GSON.toJson(status, writer);
      logger.debug("HA cluster status written to {}", filePath);
    } catch (Exception e) {
      logger.warn("Failed to write HA status file {}: {}", filePath, e.getMessage());
    }
  }
}
