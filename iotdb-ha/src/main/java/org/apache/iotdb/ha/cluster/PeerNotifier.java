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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Best-effort "peer is back UP" push notification, layered on top of — never replacing — the polling
 * liveness protocol.
 *
 * <p><b>Why:</b> the remote poll loop honors an exponential backoff (5→10→20→40→60s cap) so a DOWN
 * peer is not hammered. The side effect is that once the peer actually recovers, this node may not
 * re-probe (and therefore not notice it is UP) for up to 60s — delaying pipe auto-restart, FLUSH
 * gating release and audit resumption that all key off {@code healthChecker.isUp(remote)}.
 *
 * <p><b>Send side</b> ({@link #announceUp()}): fired once per LOCAL-node DOWN→UP transition (see
 * {@code NodeHealthChecker.setOnLocalNodeUp}); asynchronously POSTs a hint to the peer's {@code
 * /api/v1/peer-up}. Fire-and-forget with a couple of retries — if the peer is still unreachable the
 * hint is simply dropped and normal polling remains authoritative.
 *
 * <p><b>Receive side</b> ({@link #onPeerUpAnnounced(String)}): the hint is treated as a wake-up
 * signal, NOT as proof of liveness. We only collapse the peer's backoff and trigger an immediate
 * out-of-band probe ({@code PipeInspector.getPipesNow}); UP is still set solely by our own confirmed
 * HTTP 200 + {@code data_node_up} exchange. This preserves the existing evidence model (and guards
 * against asymmetric-network false positives) while cutting perception lag from ≤60s to ~1 RTT.
 */
public class PeerNotifier {
  private static final Logger logger = LoggerFactory.getLogger(PeerNotifier.class);

  private static final String PEER_UP_PATH = "/api/v1/peer-up";
  private static final int MAX_SEND_ATTEMPTS = 2;
  private static final long RETRY_DELAY_MS = 500;

  private final HaConfig config;
  private final NodeHealthChecker healthChecker;
  private final PipeInspector pipeInspector;
  private final long httpTimeoutMs;

  /** Single daemon thread serializing both outbound hints and inbound-triggered probes. */
  private final ExecutorService executor;

  public PeerNotifier(
      HaConfig config, NodeHealthChecker healthChecker, PipeInspector pipeInspector) {
    this.config = config;
    this.healthChecker = healthChecker;
    this.pipeInspector = pipeInspector;
    this.httpTimeoutMs = config.getRemoteHttpTimeoutMs();
    this.executor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "ha-peer-notifier");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Asynchronously announce to the peer that THIS node's DataNode just came UP. Must not block the
   * caller (it runs on the health-check scheduler thread). No-op when the feature is disabled, there
   * is no peer, or the peer has no {@code api_url}.
   */
  public void announceUp() {
    if (!config.isNotifyPeerOnUp() || config.getNodes().size() < 2) {
      return;
    }
    HaConfig.NodeConfig local = config.getLocalNode();
    HaConfig.NodeConfig remote = config.getRemoteNode();
    String apiUrl = remote.apiUrl;
    if (apiUrl == null || apiUrl.isEmpty()) {
      logger.debug(
          "Peer {} has no api_url configured - skipping UP announcement (polling still applies)",
          remote.id);
      return;
    }
    String base = apiUrl.endsWith("/") ? apiUrl.substring(0, apiUrl.length() - 1) : apiUrl;
    String url = base + PEER_UP_PATH;
    String json =
        "{\"node\":\"" + local.id + "\",\"event\":\"up\",\"ts\":\"" + Instant.now() + "\"}";
    executor.submit(() -> postWithRetry(url, local.id, json));
    logger.debug("Queued UP announcement to peer {} at {}", remote.id, url);
  }

  private void postWithRetry(String url, String localId, String json) {
    for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt++) {
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout((int) httpTimeoutMs);
        conn.setReadTimeout((int) httpTimeoutMs);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        conn.setFixedLengthStreamingMode(payload.length);
        try (OutputStream os = conn.getOutputStream()) {
          os.write(payload);
        }
        int code = conn.getResponseCode();
        if (code >= 200 && code < 300) {
          logger.info("Announced local UP ({}) to peer endpoint {} (HTTP {})", localId, url, code);
          return;
        }
        logger.warn(
            "Peer UP announcement to {} returned HTTP {} (attempt {}/{})",
            url,
            code,
            attempt,
            MAX_SEND_ATTEMPTS);
      } catch (Exception e) {
        logger.debug(
            "Peer UP announcement to {} failed (attempt {}/{}): {}",
            url,
            attempt,
            MAX_SEND_ATTEMPTS,
            e.getMessage());
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      if (attempt < MAX_SEND_ATTEMPTS) {
        try {
          Thread.sleep(RETRY_DELAY_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    logger.info(
        "Peer UP announcement to {} gave up after {} attempts; the peer will notice via normal "
            + "polling (hint is best-effort, never authoritative)",
        url,
        MAX_SEND_ATTEMPTS);
  }

  /**
   * Handle an inbound UP hint from the peer (called by {@code ApiServer} for {@code POST
   * /api/v1/peer-up}). Validates the claimed node id is our configured peer (basic anti-spoof: only
   * the single known peer may announce), then collapses its backoff and schedules an immediate
   * confirming probe. Liveness is NOT flipped here — only our own probe may do that.
   *
   * @return {@code true} if the hint was accepted (caller should reply 200), {@code false} if
   *     rejected (caller should reply 400).
   */
  public boolean onPeerUpAnnounced(String claimedNodeId) {
    if (config.getNodes().size() < 2) {
      return false;
    }
    HaConfig.NodeConfig remote = config.getRemoteNode();
    if (claimedNodeId == null || !claimedNodeId.equals(remote.id)) {
      logger.warn(
          "Rejected peer-up hint for unknown node '{}' (expected peer '{}')",
          claimedNodeId,
          remote.id);
      return false;
    }
    logger.info(
        "Peer {} announced it is UP - collapsing backoff and re-probing immediately instead of "
            + "waiting out the backoff window",
        claimedNodeId);
    healthChecker.resetBackoff(remote.id);
    executor.submit(
        () -> {
          try {
            pipeInspector.getPipesNow(remote.id);
          } catch (Exception e) {
            logger.warn(
                "Immediate probe after peer-up hint failed for {}: {}", remote.id, e.getMessage());
          }
        });
    return true;
  }

  /** Shut down the notifier thread. */
  public void close() {
    executor.shutdownNow();
  }
}
