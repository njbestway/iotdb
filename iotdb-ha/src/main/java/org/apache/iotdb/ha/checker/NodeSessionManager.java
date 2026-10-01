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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages persistent IoTDB Session connections per node. All HA Monitor components (PipeInspector,
 * RecoveryManager) share the same Session to avoid creating a new connection for every query.
 *
 * <p>Each node gets exactly one long-lived Session that is lazily created and automatically
 * reconnected on failure (Session has built-in retry/reconnect logic). Access is synchronized
 * because the underlying Thrift client is not thread-safe.
 *
 * <p>Sessions use {@code sqlDialect("table")} so that {@code SHOW PIPES} and other pipe management
 * commands return table-mode pipe metadata, matching the default CLI behaviour ({@code
 * start-cli-table.bat}).
 */
public class NodeSessionManager {
  private static final Logger logger = LoggerFactory.getLogger(NodeSessionManager.class);

  private final String username;
  private final String password;

  /** Per-node session holder. */
  private final Map<String, SessionEntry> sessions = new ConcurrentHashMap<>();

  /** Create with default credentials (root/root). */
  public NodeSessionManager() {
    this("root", "root");
  }

  /** Create with configurable credentials from HaConfig. */
  public NodeSessionManager(String username, String password) {
    this.username = username;
    this.password = password;
  }

  /**
   * Get a valid Session for the given rpcUrl (host:port). The session is created lazily on first
   * call and reused on subsequent calls. If the session is dead, it is silently replaced.
   *
   * <p>The returned session is owned by this manager — callers must NOT close it.
   */
  public synchronized Session getSession(String rpcUrl) throws Exception {
    SessionEntry entry = sessions.get(rpcUrl);

    if (entry != null && entry.session != null) {
      logger.debug("Reusing Session for {}", rpcUrl);
      return entry.session;
    }

    logger.debug("No existing Session for {}, creating new one", rpcUrl);

    // Parse host:port
    String[] parts = rpcUrl.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid rpcUrl format (expected host:port): " + rpcUrl);
    }
    String host = parts[0];
    int port;
    try {
      port = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port in rpcUrl: " + rpcUrl);
    }

    // Create new session (table mode — matches start-cli-table.bat)
    Session session =
        new Session.Builder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .sqlDialect("table")
            .build();
    session.open(false);

    sessions.put(rpcUrl, new SessionEntry(session, rpcUrl));
    logger.info("Session established to {} (table mode)", rpcUrl);
    return session;
  }

  /**
   * Execute a query SQL and return the SessionDataSet. Caller is responsible for closing the
   * SessionDataSet via {@code closeOperationHandle()} or try-with-resources.
   *
   * <p>On failure the session is invalidated so the next call reconnects.
   */
  public synchronized SessionDataSet executeQuery(String rpcUrl, String sql) throws Exception {
    Session session = getSession(rpcUrl);
    try {
      return session.executeQueryStatement(sql);
    } catch (Exception e) {
      logger.warn("Query failed on {} [{}]: {}, invalidating session", rpcUrl, sql, e.getMessage());
      invalidate(rpcUrl);
      throw e;
    }
  }

  /**
   * Execute a non-query SQL (DDL/DML). Returns true if successful. On failure the session is
   * invalidated so the next call reconnects.
   */
  public synchronized boolean executeNonQuery(String rpcUrl, String sql) {
    try {
      Session session = getSession(rpcUrl);
      session.executeNonQueryStatement(sql);
      logger.info("Executed SQL on {}: {}", rpcUrl, sql);
      return true;
    } catch (Exception e) {
      logger.error("SQL execution failed on {} [{}]: {}", rpcUrl, sql, e.getMessage());
      invalidate(rpcUrl);
      return false;
    }
  }

  /** Close all managed sessions. Called on shutdown. */
  public synchronized void close() {
    for (Map.Entry<String, SessionEntry> e : sessions.entrySet()) {
      closeQuietly(e.getValue().session);
      logger.debug("Session closed for {}", e.getValue().rpcUrl);
    }
    sessions.clear();
    logger.info("All sessions closed");
  }

  /** Invalidate a specific session (e.g. after a detected failure). */
  public synchronized void invalidate(String rpcUrl) {
    SessionEntry entry = sessions.remove(rpcUrl);
    if (entry != null) {
      closeQuietly(entry.session);
      logger.warn("Session invalidated for {}", rpcUrl);
    }
  }

  private static void closeQuietly(Session session) {
    try {
      if (session != null) {
        session.close();
      }
    } catch (Exception ignored) {
      // quiet
    }
  }

  private static class SessionEntry {
    final Session session;
    final String rpcUrl;

    SessionEntry(Session session, String rpcUrl) {
      this.session = session;
      this.rpcUrl = rpcUrl;
    }
  }
}
