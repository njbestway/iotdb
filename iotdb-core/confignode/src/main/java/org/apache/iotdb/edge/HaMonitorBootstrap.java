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

package org.apache.iotdb.edge;

import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Bootstrap for embedding the HA Monitor into the EdgeNode JVM. Uses reflection so that the
 * confignode module keeps no compile-time dependency on the {@code iotdb-ha} module; the iotdb-ha
 * jar only needs to be present on the runtime classpath (i.e. in the distribution's {@code lib/}
 * directory).
 *
 * <p>Enable via JVM args: {@code -Diotdb.ha.enabled=true -Diotdb.ha.config=<path-to-config.json>}.
 * When disabled (the default) this class does nothing, so a standard edge deployment is completely
 * unaffected even if the iotdb-ha jar happens to sit in {@code lib/}.
 *
 * <p>The HA Monitor talks to the nodes through the IoTDB Session API. The {@code iotdb-session},
 * {@code slf4j} and {@code logback} classes it needs are already provided by the EdgeNode
 * classpath, which is why the iotdb-ha jar declares them as {@code provided} scope and stays small
 * (~480 KB) instead of becoming a fat jar that would clash with the distribution's own jars.
 */
public final class HaMonitorBootstrap {
  private static final Logger logger = LoggerFactory.getLogger(HaMonitorBootstrap.class);

  /** The HaMonitorMain instance, held via reflection. Null when the monitor is not running. */
  private static volatile Object monitor;

  private HaMonitorBootstrap() {}

  /**
   * Start the HA Monitor if the iotdb-ha jar is on the classpath and {@code
   * -Diotdb.ha.enabled=true}. Safe to call when HA is not configured — it simply returns.
   *
   * <p>Should be invoked after the DataNode is up so that the Session API can reach the local RPC
   * port without racing the node startup.
   */
  public static void maybeStart() {
    String enabled = System.getProperty("iotdb.ha.enabled", "false");
    if (!"true".equalsIgnoreCase(enabled)) {
      logger.info("HA Monitor disabled (set -Diotdb.ha.enabled=true to enable)");
      return;
    }

    try {
      // Reflection avoids a compile-time dependency on the iotdb-ha module.
      Class<?> mainClass = Class.forName("org.apache.iotdb.ha.HaMonitorMain");
      Class<?> configClass = Class.forName("org.apache.iotdb.ha.config.HaConfig");

      String configPath = System.getProperty("iotdb.ha.config", "conf/ha-config.json");
      Method loadMethod = configClass.getMethod("load", String.class);
      Object config = loadMethod.invoke(null, configPath);

      Object instance = mainClass.getConstructor(configClass).newInstance(config);
      Method startMethod = mainClass.getMethod("start");
      startMethod.invoke(instance);

      monitor = instance;
      logger.info("HA Monitor started (embedded in EdgeNode, config={})", configPath);

      // Inject an in-process pipe-status supplier so the monitor reads the LOCAL node's pipe state
      // straight from ConfigNode memory instead of issuing SHOW PIPES on every poll. Each SHOW
      // PIPES
      // borrows a DataNode→ConfigNode client and triggers a ConfigNode consensus read, competing
      // with schema-pipe transfers for the same client pool (the pool-exhaustion incident).
      registerLocalPipeStatusProvider(mainClass, instance);

      // Ensure the monitor's scheduler / API server / session resources are released on JVM exit.
      Runtime.getRuntime()
          .addShutdownHook(new Thread(HaMonitorBootstrap::stop, "HA-Monitor-Shutdown"));
    } catch (ClassNotFoundException e) {
      logger.warn("iotdb-ha jar not found on classpath, HA Monitor disabled");
    } catch (Exception e) {
      logger.error("Failed to start HA Monitor", e);
    }
  }

  /** Stop the HA Monitor if it was started. Idempotent — safe to call multiple times. */
  public static void stop() {
    Object instance = monitor;
    if (instance != null) {
      try {
        Method stopMethod = instance.getClass().getMethod("stop");
        stopMethod.invoke(instance);
        logger.info("HA Monitor stopped");
      } catch (Exception e) {
        logger.error("Failed to stop HA Monitor", e);
      } finally {
        monitor = null;
      }
    }
  }

  /**
   * Register the in-process local pipe-status supplier on the HaMonitorMain instance via
   * reflection. Only JDK types ({@link Supplier}, {@link List}, {@link Map}) cross the module
   * boundary, so iotdb-ha stays free of any confignode compile-time dependency.
   */
  private static void registerLocalPipeStatusProvider(Class<?> mainClass, Object instance) {
    try {
      Supplier<List<Map<String, Object>>> provider = HaMonitorBootstrap::readLocalPipeStatus;
      Method setter = mainClass.getMethod("setLocalPipeStatusProvider", Supplier.class);
      setter.invoke(instance, provider);
      logger.info(
          "HA Monitor local pipe status provider registered (in-memory read for local node)");
    } catch (NoSuchMethodException e) {
      logger.warn(
          "HaMonitorMain.setLocalPipeStatusProvider not present; local pipe status will use SHOW PIPES");
    } catch (Exception e) {
      logger.warn("Failed to register local pipe status provider; falling back to SHOW PIPES", e);
    }
  }

  /**
   * Read the local node's pipe status directly from the in-process ConfigNode. Returns {@code null}
   * when the ConfigNode is not ready / not leader (signalling the monitor to fall back to SHOW
   * PIPES), otherwise the list of pipes (possibly empty). Issues the very same table-mode {@code
   * showPipe} request the monitor would otherwise send over a Session, so results are identical by
   * construction — only the transport (in-process call vs. RPC + client borrow) differs.
   */
  private static List<Map<String, Object>> readLocalPipeStatus() {
    try {
      ConfigNode configNode = ConfigNode.getInstance();
      if (configNode == null) {
        return null;
      }
      TShowPipeResp resp =
          configNode
              .getConfigManager()
              .showPipe(new TShowPipeReq().setIsTableModel(true).setUserName("root"));
      // A non-leader / error response carries no pipe list — fall back to the Session path.
      if (resp == null || !resp.isSetPipeInfoList()) {
        return null;
      }
      List<Map<String, Object>> out = new ArrayList<>();
      for (TShowPipeInfo info : resp.getPipeInfoList()) {
        long remainingEventCount =
            info.isSetRemainingEventCount() ? info.getRemainingEventCount() : -1L;
        double remainingTime =
            info.isSetEstimatedRemainingTime() ? info.getEstimatedRemainingTime() : -1d;
        // Mirror the DataNode SHOW PIPES path (ShowPipeTask.buildTSBlock): when the ConfigNode
        // cannot compute the remaining count locally — a standalone data-only pipe yields -1/-1
        // via canCalculateOnLocal — the true value comes from the DataNode's own single-pipe
        // metrics. Without this the in-memory read would report -1 where SHOW PIPES reports the
        // real local lag, diverging the local node's remaining_events from the remote node's.
        if (remainingEventCount == -1 && remainingTime == -1) {
          try {
            Pair<Long, Double> local =
                PipeDataNodeSinglePipeMetrics.getInstance()
                    .getRemainingEventAndTime(info.getId(), info.getCreationTime());
            if (local != null) {
              if (local.getLeft() != null) {
                remainingEventCount = local.getLeft();
              }
              if (local.getRight() != null) {
                remainingTime = local.getRight();
              }
            }
          } catch (Throwable ignore) {
            // DataNode metrics not ready yet — keep the ConfigNode-reported value.
          }
        }
        Map<String, Object> m = new HashMap<>();
        m.put("id", info.getId());
        m.put("state", info.getState());
        m.put("remainingEventCount", remainingEventCount);
        m.put("estimatedRemainingSeconds", (long) remainingTime);
        out.add(m);
      }
      return out;
    } catch (Throwable t) {
      logger.warn("In-memory local pipe status read failed: {}", t.getMessage());
      return null;
    }
  }
}
