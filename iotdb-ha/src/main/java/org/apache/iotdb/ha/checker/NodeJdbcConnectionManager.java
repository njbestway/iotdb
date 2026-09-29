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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages persistent JDBC connections per node. All HA Monitor components (PipeInspector,
 * ClusterSyncWriter, RecoveryManager) share the same connection to avoid creating a new JDBC
 * session for every query.
 *
 * <p>Each node gets exactly one long-lived connection that is lazily created, validated before use,
 * and automatically reconnected on failure. Access is synchronized because JDBC connections are not
 * thread-safe.
 *
 * <p>Connections use {@code sql_dialect=table} so that {@code SHOW PIPES} and other pipe management
 * commands return table-mode pipe metadata, matching the default CLI behaviour ({@code
 * start-cli-table.bat}).
 */
public class NodeJdbcConnectionManager {
  private static final Logger logger = LoggerFactory.getLogger(NodeJdbcConnectionManager.class);
  private static final String JDBC_PREFIX = "jdbc:iotdb://";
  private static final String JDBC_SUFFIX = "?sql_dialect=table";

  private final String username;
  private final String password;

  /** Create with default credentials (root/root). */
  public NodeJdbcConnectionManager() {
    this("root", "root");
  }

  /** Create with configurable credentials from HaConfig. */
  public NodeJdbcConnectionManager(String username, String password) {
    this.username = username;
    this.password = password;
  }

  /** Per-node connection holder. */
  private final Map<String, ConnectionEntry> connections = new ConcurrentHashMap<>();

  /**
   * Get a valid JDBC connection for the given rpcUrl. The connection is created lazily on first
   * call and reused on subsequent calls. If the connection is dead, it is silently replaced.
   *
   * <p>The returned connection is owned by this manager — callers must NOT close it.
   */
  public synchronized Connection getConnection(String rpcUrl) throws SQLException {
    ConnectionEntry entry = connections.get(rpcUrl);

    // Validate existing connection
    if (entry != null && entry.connection != null) {
      try {
        boolean closed = entry.connection.isClosed();
        boolean valid = !closed && entry.connection.isValid(3);
        if (!closed && valid) {
          logger.debug("Reusing JDBC connection to {}", rpcUrl);
          return entry.connection;
        }
        logger.warn(
            "JDBC connection to {} is dead (isClosed={}, isValid={}), replacing",
            rpcUrl,
            closed,
            valid);
        // Connection is dead — close it quietly
        closeQuietly(entry.connection);
      } catch (SQLException e) {
        logger.warn("Exception checking JDBC connection to {}: {}", rpcUrl, e.getMessage());
        closeQuietly(entry.connection);
      }
    } else {
      logger.debug("No existing JDBC connection to {}, creating new one", rpcUrl);
    }

    // Create new connection (table mode — matches start-cli-table.bat)
    String url = JDBC_PREFIX + rpcUrl + JDBC_SUFFIX;
    Connection conn = DriverManager.getConnection(url, username, password);
    connections.put(rpcUrl, new ConnectionEntry(conn, rpcUrl));
    logger.info("JDBC connection established to {} (table mode)", rpcUrl);
    return conn;
  }

  /** Close all managed connections. Called on shutdown. */
  public synchronized void close() {
    for (Map.Entry<String, ConnectionEntry> e : connections.entrySet()) {
      closeQuietly(e.getValue().connection);
      logger.debug("JDBC connection closed for {}", e.getValue().rpcUrl);
    }
    connections.clear();
    logger.info("All JDBC connections closed");
  }

  /** Invalidate a specific connection (e.g. after a detected failure). */
  public synchronized void invalidate(String rpcUrl) {
    ConnectionEntry entry = connections.remove(rpcUrl);
    if (entry != null) {
      closeQuietly(entry.connection);
      logger.warn("JDBC connection invalidated for {}", rpcUrl);
    }
  }

  private static void closeQuietly(Connection conn) {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException ignored) {
      // quiet
    }
  }

  private static class ConnectionEntry {
    final Connection connection;
    final String rpcUrl;

    ConnectionEntry(Connection connection, String rpcUrl) {
      this.connection = connection;
      this.rpcUrl = rpcUrl;
    }
  }
}
