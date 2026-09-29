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

  @SerializedName("jdbc")
  private JdbcConfig jdbc = new JdbcConfig();

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

  public long getConsistencyCheckIntervalMs() {
    return parseDuration(monitor.consistencyCheckInterval);
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

  public String getHaStatusFilePath() {
    return recovery.haStatusFilePath != null
        ? recovery.haStatusFilePath
        : "data/ha-cluster-status.json";
  }

  /** Override the HA status file path (e.g. to set an absolute path). */
  public void setHaStatusFilePath(String absolutePath) {
    recovery.haStatusFilePath = absolutePath;
  }

  public String getJdbcUsername() {
    return jdbc.username;
  }

  public String getJdbcPassword() {
    return jdbc.password;
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
  }

  public static class MonitorConfig {
    @SerializedName("node_check_interval")
    public String nodeCheckInterval = "5s";

    @SerializedName("pipe_check_interval")
    public String pipeCheckInterval = "10s";

    @SerializedName("consistency_check_interval")
    public String consistencyCheckInterval = "10m";
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
  }

  public static class JdbcConfig {
    @SerializedName("username")
    public String username = "root";

    @SerializedName("password")
    public String password = "root";
  }
}
