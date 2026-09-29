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

package org.apache.iotdb.ha.api;

import org.apache.iotdb.ha.alert.Alerter;
import org.apache.iotdb.ha.cluster.ClusterAggregator;
import org.apache.iotdb.ha.config.HaConfig;
import org.apache.iotdb.ha.recovery.RecoveryManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/** Lightweight HTTP API server using JDK built-in HttpServer. */
public class ApiServer {
  private static final Logger logger = LoggerFactory.getLogger(ApiServer.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private final HaConfig config;
  private final ClusterAggregator clusterAggregator;
  private final Alerter alerter;
  private final RecoveryManager recoveryManager;
  private HttpServer server;

  public ApiServer(
      HaConfig config,
      ClusterAggregator clusterAggregator,
      Alerter alerter,
      RecoveryManager recoveryManager) {
    this.config = config;
    this.clusterAggregator = clusterAggregator;
    this.alerter = alerter;
    this.recoveryManager = recoveryManager;
  }

  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(config.getApiPort()), 0);
    server.createContext("/api/v1/cluster", this::handleCluster);
    server.createContext("/api/v1/status", this::handleStatus);
    server.createContext("/api/v1/alerts", this::handleAlerts);
    server.createContext("/api/v1/health", this::handleHealth);
    server.createContext("/api/v1/recovery", this::handleRecovery);
    server.setExecutor(null);
    server.start();
    logger.info("API server started on port {}", config.getApiPort());
  }

  public void stop() {
    if (server != null) {
      server.stop(0);
      logger.info("API server stopped");
    }
  }

  private void handleCluster(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
      return;
    }
    String json = clusterAggregator.buildClusterViewJson();
    sendResponse(exchange, 200, json);
  }

  private void handleStatus(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
      return;
    }
    // Simplified status: delegates to cluster aggregator
    String json = clusterAggregator.buildClusterViewJson();
    sendResponse(exchange, 200, json);
  }

  private void handleAlerts(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
      return;
    }
    String json = GSON.toJson(alerter.getRecentAlerts());
    sendResponse(exchange, 200, json);
  }

  private void handleHealth(HttpExchange exchange) throws IOException {
    sendResponse(exchange, 200, "{\"status\":\"ok\"}");
  }

  private void handleRecovery(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
      return;
    }
    if (recoveryManager == null) {
      sendResponse(exchange, 200, "{\"recovery_enabled\":false}");
      return;
    }
    String json = GSON.toJson(recoveryManager.getRecoveryStatus());
    sendResponse(exchange, 200, json);
  }

  private void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    exchange.sendResponseHeaders(code, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
