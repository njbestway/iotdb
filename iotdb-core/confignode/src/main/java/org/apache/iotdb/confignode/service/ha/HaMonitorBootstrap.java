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

package org.apache.iotdb.confignode.service.ha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Bootstrap for embedding HA Monitor into ConfigNode. Uses reflection to avoid compile-time
 * dependency on iotdb-ha module. The iotdb-ha.jar only needs to be on the runtime classpath (i.e.
 * in the lib/ directory).
 *
 * <p>Enable via JVM args: {@code -Diotdb.ha.enabled=true -Diotdb.ha.config=conf/ha-config.json}
 */
public class HaMonitorBootstrap {
  private static final Logger logger = LoggerFactory.getLogger(HaMonitorBootstrap.class);

  private static volatile Object monitor; // HaMonitorMain instance (held via reflection)

  /**
   * Start the HA Monitor if iotdb-ha.jar is on classpath and -Diotdb.ha.enabled=true. Safe to call
   * even if HA is not configured — will simply return.
   */
  public static void maybeStart() {
    String enabled = System.getProperty("iotdb.ha.enabled", "false");
    if (!"true".equalsIgnoreCase(enabled)) {
      logger.info("HA Monitor disabled (set -Diotdb.ha.enabled=true to enable)");
      return;
    }

    try {
      // Use reflection to load HaMonitorMain — avoids compile-time dependency
      Class<?> mainClass = Class.forName("org.apache.iotdb.ha.HaMonitorMain");
      Class<?> configClass = Class.forName("org.apache.iotdb.ha.config.HaConfig");

      String configPath = System.getProperty("iotdb.ha.config", "conf/ha-config.json");
      Method loadMethod = configClass.getMethod("load", String.class);
      Object config = loadMethod.invoke(null, configPath);

      Object instance = mainClass.getConstructor(configClass).newInstance(config);
      Method startMethod = mainClass.getMethod("start");
      startMethod.invoke(instance);

      monitor = instance;
      logger.info("HA Monitor started (embedded in ConfigNode, config={})", configPath);
    } catch (ClassNotFoundException e) {
      logger.warn("iotdb-ha.jar not found on classpath, HA Monitor disabled");
    } catch (Exception e) {
      logger.error("Failed to start HA Monitor", e);
    }
  }

  public static void stop() {
    if (monitor != null) {
      try {
        Method stopMethod = monitor.getClass().getMethod("stop");
        stopMethod.invoke(monitor);
        monitor = null;
        logger.info("HA Monitor stopped");
      } catch (Exception e) {
        logger.error("Failed to stop HA Monitor", e);
      }
    }
  }
}
