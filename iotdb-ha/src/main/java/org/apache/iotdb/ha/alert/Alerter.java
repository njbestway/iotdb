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

package org.apache.iotdb.ha.alert;

import org.apache.iotdb.ha.config.HaConfig;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Sends alerts via webhook and logs. Maintains recent alert history with cooldown. */
public class Alerter {
  private static final Logger logger = LoggerFactory.getLogger(Alerter.class);
  private static final Gson GSON = new Gson();
  private static final int MAX_ALERTS = 100;

  private final HaConfig config;
  private final List<Map<String, Object>> recentAlerts =
      Collections.synchronizedList(new ArrayList<>());

  /** Cooldown tracker: key → last fire timestamp (ms). Prevents alert spam. */
  private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();

  public Alerter(HaConfig config) {
    this.config = config;
  }

  /** Fire an alert with automatic cooldown. Logs it and optionally sends a webhook. */
  public void fire(String type, String severity, String message) {
    fire(type, severity, message, type);
  }

  /**
   * Fire an alert with a custom cooldown key.
   *
   * @param type alert type
   * @param severity severity level
   * @param message alert message
   * @param cooldownKey key used for cooldown deduplication
   */
  public void fire(String type, String severity, String message, String cooldownKey) {
    long now = System.currentTimeMillis();
    long cooldownMs = config.getAlertCooldownMs();

    // Check cooldown
    Long lastFire = cooldownMap.get(cooldownKey);
    if (lastFire != null && (now - lastFire) < cooldownMs) {
      logger.debug(
          "Alert suppressed (cooldown): type={}, key={}, remaining={}s",
          type,
          cooldownKey,
          (cooldownMs - (now - lastFire)) / 1000);
      return;
    }
    cooldownMap.put(cooldownKey, now);

    Map<String, Object> alert = new LinkedHashMap<>();
    alert.put("type", type);
    alert.put("severity", severity);
    alert.put("message", message);
    alert.put("timestamp", Instant.now().toString());

    // Log
    if ("CRITICAL".equals(severity)) {
      logger.error("ALERT [{}] {}: {}", severity, type, message);
    } else {
      logger.warn("ALERT [{}] {}: {}", severity, type, message);
    }

    // Store recent
    recentAlerts.add(0, alert);
    while (recentAlerts.size() > MAX_ALERTS) {
      recentAlerts.remove(recentAlerts.size() - 1);
    }

    // Webhook
    String webhookUrl = config.getWebhookUrl();
    if (webhookUrl != null && !webhookUrl.isEmpty()) {
      sendWebhook(webhookUrl, alert);
    }
  }

  /** Clear cooldown for a key (e.g., when node recovers). */
  public void clearCooldown(String key) {
    cooldownMap.remove(key);
  }

  public List<Map<String, Object>> getRecentAlerts() {
    return new ArrayList<>(recentAlerts);
  }

  private void sendWebhook(String url, Map<String, Object> alert) {
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) new URL(url).openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);

      byte[] body = GSON.toJson(alert).getBytes(StandardCharsets.UTF_8);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(body);
      }
      int code = conn.getResponseCode();
      if (code >= 200 && code < 300) {
        logger.debug("Webhook sent: {}", alert.get("type"));
      } else {
        logger.warn("Webhook failed: HTTP {}", code);
      }
    } catch (Exception e) {
      logger.warn("Webhook failed: {}", e.getMessage());
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }
}
