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

package org.apache.iotdb.ha.audit;

import org.apache.iotdb.ha.alert.Alerter;
import org.apache.iotdb.ha.checker.NodeHealthChecker;
import org.apache.iotdb.ha.checker.NodeSessionManager;
import org.apache.iotdb.ha.config.HaConfig;
import org.apache.iotdb.isession.SessionDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Periodic data audit. Compares the row count of every replicated table between the local (source)
 * node and the remote (target) node, alerting when the divergence exceeds the configured tolerance.
 *
 * <p>This closes the correctness loop of the replication pipeline: while {@code PipeInspector}
 * reports whether the pipe is <i>running</i> and how far it <i>lags</i>, the auditor verifies that
 * the data actually <i>arrived</i> — catching silent gaps (dropped events, schema/table missing on
 * the target, partial transfers) that a RUNNING pipe would not surface.
 *
 * <p>Audit runs on the replication SOURCE node only (see {@code audit.enabled}); comparing {@code
 * local → remote} matches the pipe direction. When either node is DOWN the pass is skipped rather
 * than reported as a mismatch, since {@code node_down} already covers that case and an unreachable
 * target would otherwise produce spurious CRITICAL alerts.
 */
public class DataAuditor {
  private static final Logger logger = LoggerFactory.getLogger(DataAuditor.class);

  /** System databases that never carry replicated user data and are always skipped. */
  private static final Set<String> SYSTEM_DATABASES =
      new LinkedHashSet<>(Arrays.asList("information_schema", "flow"));

  private final HaConfig config;
  private final NodeSessionManager sessionManager;
  private final NodeHealthChecker healthChecker;
  private final Alerter alerter;

  /** Most recent audit pass result, exposed via the REST API and Prometheus. */
  private volatile List<AuditResult> lastReport = new ArrayList<>();
  private volatile long lastRunAt = 0L;
  private volatile boolean lastRunConsistent = true;

  public DataAuditor(
      HaConfig config,
      NodeSessionManager sessionManager,
      NodeHealthChecker healthChecker,
      Alerter alerter) {
    this.config = config;
    this.sessionManager = sessionManager;
    this.healthChecker = healthChecker;
    this.alerter = alerter;
  }

  /**
   * Run one audit pass. Discovers the databases/tables to audit (or uses the configured lists),
   * counts rows on both nodes, evaluates divergence, fires alerts, and stores the report.
   */
  public void audit() {
    if (!config.isAuditEnabled()) {
      return;
    }
    HaConfig.NodeConfig source = config.getLocalNode();
    HaConfig.NodeConfig target = config.getRemoteNode();

    if (!healthChecker.isUp(source.id) || !healthChecker.isUp(target.id)) {
      logger.debug(
          "Data audit skipped: {} or {} is DOWN (node_down alert covers this)",
          source.id,
          target.id);
      return;
    }

    long tolerance = config.getAuditRowCountTolerance();
    long criticalThreshold = config.getAuditCriticalThreshold();
    List<String> databases = resolveDatabases(source.rpcUrl);
    List<AuditResult> report = new ArrayList<>();
    boolean consistent = true;

    for (String db : databases) {
      List<String> tables = resolveTables(source.rpcUrl, db);
      for (String table : tables) {
        String fqn = db + "." + table;
        Long srcCount = countRows(source.rpcUrl, fqn);
        Long tgtCount = countRows(target.rpcUrl, fqn);

        AuditResult r = new AuditResult();
        r.database = db;
        r.table = table;
        r.sourceCount = srcCount == null ? -1 : srcCount;
        r.targetCount = tgtCount == null ? -1 : tgtCount;

        if (srcCount == null) {
          // Cannot read the source — nothing meaningful to compare; log but do not alert.
          r.status = "ERROR";
          r.message = "source query failed on " + source.id;
          logger.warn("Data audit: source COUNT(*) failed for {}: {}", fqn, r.message);
          report.add(r);
          continue;
        }

        if (tgtCount == null) {
          // Source has the table but the target does not (or is unqueryable) — a real data gap.
          r.status = "CRITICAL";
          r.diff = srcCount;
          r.message =
              "table " + fqn + " missing/unqueryable on target " + target.id + " (" + srcCount
                  + " rows on source)";
          consistent = false;
          alerter.fire(
              "data_audit_gap", "CRITICAL", r.message, "data_audit_gap:" + fqn);
          logger.error("Data audit CRITICAL: {}", r.message);
          report.add(r);
          continue;
        }

        long diff = srcCount - tgtCount;
        long absDiff = Math.abs(diff);
        r.diff = diff;

        if (absDiff <= tolerance) {
          r.status = "OK";
          r.message = "consistent (" + srcCount + " rows both sides)";
          // Clear cooldown so a fresh divergence fires immediately next time.
          alerter.clearCooldown("data_audit_mismatch:" + fqn);
          report.add(r);
          continue;
        }

        consistent = false;
        if (diff < 0) {
          // Target holds MORE rows than the source — unexpected in a primary→replica topology.
          r.status = "CRITICAL";
          r.message =
              "target " + target.id + " has " + absDiff + " MORE rows than source " + source.id
                  + " (" + tgtCount + " vs " + srcCount + ")";
          alerter.fire("data_audit_mismatch", "CRITICAL", r.message, "data_audit_mismatch:" + fqn);
          logger.error("Data audit CRITICAL: {}", r.message);
        } else {
          String severity = absDiff >= criticalThreshold ? "CRITICAL" : "WARNING";
          r.status = severity;
          r.message =
              "target " + target.id + " lags source " + source.id + " by " + diff + " rows ("
                  + tgtCount + " vs " + srcCount + ")";
          alerter.fire(
              "data_audit_mismatch", severity, r.message, "data_audit_mismatch:" + fqn);
          if ("CRITICAL".equals(severity)) {
            logger.error("Data audit CRITICAL: {}", r.message);
          } else {
            logger.warn("Data audit WARNING: {}", r.message);
          }
        }
        report.add(r);
      }
    }

    this.lastReport = report;
    this.lastRunAt = System.currentTimeMillis();
    this.lastRunConsistent = consistent;
    logger.info(
        "Data audit pass complete: {} tables audited ({} → {}), consistent={}",
        report.size(),
        source.id,
        target.id,
        consistent);
  }

  /** Databases to audit: the configured list, or all user databases discovered on the source. */
  private List<String> resolveDatabases(String sourceRpcUrl) {
    List<String> configured = config.getAuditDatabases();
    if (configured != null && !configured.isEmpty()) {
      return configured;
    }
    List<String> discovered = queryColumn(sourceRpcUrl, "SHOW DATABASES", "database");
    List<String> result = new ArrayList<>();
    for (String db : discovered) {
      if (db != null && !db.isEmpty() && !SYSTEM_DATABASES.contains(db.toLowerCase())) {
        result.add(db);
      }
    }
    return result;
  }

  /** Tables to audit within a database: the configured list, or all tables discovered. */
  private List<String> resolveTables(String sourceRpcUrl, String db) {
    List<String> configured = config.getAuditTables();
    if (configured != null && !configured.isEmpty()) {
      return configured;
    }
    return queryColumn(sourceRpcUrl, "SHOW TABLES FROM " + db, "tablename");
  }

  /**
   * Count rows of a fully-qualified table via {@code SELECT COUNT(*) FROM <db>.<table>}. Returns
   * {@code null} when the query fails (e.g. table absent on that node).
   */
  private Long countRows(String rpcUrl, String fqn) {
    SessionDataSet dataSet = null;
    try {
      dataSet = sessionManager.executeQuery(rpcUrl, "SELECT COUNT(*) FROM " + fqn);
      SessionDataSet.DataIterator it = dataSet.iterator();
      if (it.next()) {
        if (it.isNull(1)) {
          return 0L;
        }
        return parseLongSafe(it.getString(1));
      }
      return 0L;
    } catch (Exception e) {
      logger.warn("COUNT(*) failed on {} for {}: {}", rpcUrl, fqn, e.getMessage());
      return null;
    } finally {
      closeQuietly(dataSet);
    }
  }

  /**
   * Run a query and collect the values of a single named column (case-insensitive) across all rows.
   * Returns an empty list on failure.
   */
  private List<String> queryColumn(String rpcUrl, String sql, String columnName) {
    List<String> values = new ArrayList<>();
    SessionDataSet dataSet = null;
    try {
      dataSet = sessionManager.executeQuery(rpcUrl, sql);
      List<String> columnNames = dataSet.getColumnNames();
      int idx = -1;
      if (columnNames != null) {
        for (int i = 0; i < columnNames.size(); i++) {
          if (columnNames.get(i).toLowerCase().equals(columnName.toLowerCase())) {
            idx = i + 1;
            break;
          }
        }
      }
      if (idx < 0) {
        // Fall back to the first column when the expected name is absent.
        idx = 1;
      }
      SessionDataSet.DataIterator it = dataSet.iterator();
      while (it.next()) {
        if (!it.isNull(idx)) {
          values.add(it.getString(idx));
        }
      }
    } catch (Exception e) {
      logger.warn("Discovery query failed on {} [{}]: {}", rpcUrl, sql, e.getMessage());
    } finally {
      closeQuietly(dataSet);
    }
    return values;
  }

  private static Long parseLongSafe(String value) {
    if (value == null || value.isEmpty() || "Unknown".equalsIgnoreCase(value)) {
      return null;
    }
    try {
      if (value.contains(".")) {
        return (long) Double.parseDouble(value.trim());
      }
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static void closeQuietly(SessionDataSet dataSet) {
    if (dataSet != null) {
      try {
        dataSet.closeOperationHandle();
      } catch (Exception ignored) {
        // quiet
      }
    }
  }

  /** Last audit pass as typed results (for Prometheus metric publishing). */
  public List<AuditResult> getAuditResults() {
    return lastReport;
  }

  /** Last audit pass rendered as JSON-friendly maps (for the REST API). */
  public List<Map<String, Object>> getAuditReport() {
    List<Map<String, Object>> out = new ArrayList<>();
    for (AuditResult r : lastReport) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("database", r.database);
      m.put("table", r.table);
      m.put("source_count", r.sourceCount);
      m.put("target_count", r.targetCount);
      m.put("diff", r.diff);
      m.put("status", r.status);
      m.put("message", r.message);
      out.add(m);
    }
    return out;
  }

  public long getLastRunAt() {
    return lastRunAt;
  }

  public String getLastRunIso() {
    return lastRunAt == 0 ? null : Instant.ofEpochMilli(lastRunAt).toString();
  }

  public boolean isLastRunConsistent() {
    return lastRunConsistent;
  }

  /** Result of auditing a single table. */
  public static class AuditResult {
    public String database;
    public String table;
    public long sourceCount = -1;
    public long targetCount = -1;
    public long diff = 0;
    public String status = "OK";
    public String message = "";
  }
}
