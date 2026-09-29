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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Aggregates cluster view from both nodes. Provides the enhanced SHOW CLUSTER functionality. */
public class ClusterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(ClusterAggregator.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private final HaConfig config;
  private final NodeHealthChecker healthChecker;
  private final PipeInspector pipeInspector;

  public ClusterAggregator(
      HaConfig config, NodeHealthChecker healthChecker, PipeInspector pipeInspector) {
    this.config = config;
    this.healthChecker = healthChecker;
    this.pipeInspector = pipeInspector;
  }

  /** Build the aggregated cluster view as JSON string. */
  public String buildClusterViewJson() {
    Map<String, Object> view = buildClusterView();
    return GSON.toJson(view);
  }

  /** Build the aggregated cluster view as a Map. */
  public Map<String, Object> buildClusterView() {
    Map<String, Object> view = new LinkedHashMap<>();
    view.put("cluster_id", config.getClusterId());
    view.put("timestamp", Instant.now().toString());

    // Nodes
    Map<String, Object> nodes = new LinkedHashMap<>();
    for (HaConfig.NodeConfig node : config.getNodes()) {
      nodes.put(node.id, buildNodeView(node));
    }
    view.put("nodes", nodes);

    // Replication
    view.put("replication", buildReplicationView());

    // Cluster state
    view.put("cluster_state", computeClusterState());

    return view;
  }

  private Map<String, Object> buildNodeView(HaConfig.NodeConfig node) {
    Map<String, Object> nodeView = new LinkedHashMap<>();

    boolean isLocal = node.equals(config.getLocalNode());
    nodeView.put("role", isLocal ? "local" : "remote");

    // Node health
    boolean up = healthChecker.isUp(node.id);
    NodeHealthChecker.NodeStatus status = healthChecker.getStatusMap().get(node.id);

    Map<String, Object> configNode = new LinkedHashMap<>();
    configNode.put("status", up ? "Running" : "Unreachable");
    configNode.put("address", node.rpcUrl);
    nodeView.put("node_status", up ? "UP" : "DOWN");
    if (up && status != null) {
      nodeView.put("uptime_ms", status.getUptimeMs());
    }
    nodeView.put("config_node", configNode);

    Map<String, Object> dataNode = new LinkedHashMap<>();
    dataNode.put("status", up ? "Running" : "Unreachable");
    dataNode.put("address", node.rpcUrl);
    nodeView.put("data_node", dataNode);

    // Pipes on this node
    if (up) {
      List<PipeInspector.PipeInfo> pipes = pipeInspector.getPipes(node.id);
      Map<String, Object> pipeMap = new LinkedHashMap<>();
      for (PipeInspector.PipeInfo pipe : pipes) {
        Map<String, Object> pipeInfo = new LinkedHashMap<>();
        pipeInfo.put("status", pipe.status);
        pipeInfo.put("remaining_events", pipe.remainingEventCount);
        pipeMap.put(pipe.pipeName, pipeInfo);
      }
      nodeView.put("pipes", pipeMap);
    }

    return nodeView;
  }

  private Map<String, Object> buildReplicationView() {
    Map<String, Object> replication = new LinkedHashMap<>();

    String localId = config.getLocalNode().id;
    String remoteId = config.getRemoteNode().id;

    // Channel A_TO_B: pipes on local node with prefix matching local node id
    List<PipeInspector.PipeInfo> localPipes =
        healthChecker.isUp(localId) ? pipeInspector.getPipes(localId) : List.of();
    List<PipeInspector.PipeInfo> remotePipes =
        healthChecker.isUp(remoteId) ? pipeInspector.getPipes(remoteId) : List.of();

    // A_TO_B channel: all pipes on local node (source = local)
    PipeInspector.ChannelState abState = pipeInspector.computeChannelState(localPipes);
    Map<String, Object> abView = new LinkedHashMap<>();
    abView.put("channel_state", abState.state);
    abView.put("remaining_events", abState.remainingEvents);
    replication.put(localId + "_TO_" + remoteId, abView);

    // B_TO_A channel: all pipes on remote node (source = remote)
    PipeInspector.ChannelState baState = pipeInspector.computeChannelState(remotePipes);
    Map<String, Object> baView = new LinkedHashMap<>();
    baView.put("channel_state", baState.state);
    baView.put("remaining_events", baState.remainingEvents);
    replication.put(remoteId + "_TO_" + localId, baView);

    return replication;
  }

  private String computeClusterState() {
    boolean localUp = healthChecker.isUp(config.getLocalNode().id);
    boolean remoteUp = healthChecker.isUp(config.getRemoteNode().id);

    if (localUp && remoteUp) {
      return "NORMAL";
    } else if (localUp || remoteUp) {
      return "DEGRADED";
    } else {
      return "CRITICAL";
    }
  }
}
