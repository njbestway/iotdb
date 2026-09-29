#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# IoTDB HA Monitor start script

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HA_HOME="$(dirname "$SCRIPT_DIR")"

# Config
CONFIG_FILE="${HA_HOME}/config.json"
if [ -n "$1" ]; then
  CONFIG_FILE="$1"
fi

# JVM options
JVM_OPTS="${JVM_OPTS:--Xms256m -Xmx512m}"

# PID file
PID_FILE="${HA_HOME}/ha-monitor.pid"
LOG_DIR="${HA_HOME}/logs"

# Check if already running
if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    echo "HA Monitor is already running (PID: $PID)"
    exit 1
  fi
  rm -f "$PID_FILE"
fi

# Create log directory
mkdir -p "$LOG_DIR"

# Find the jar
JAR=$(ls "${HA_HOME}"/target/iotdb-ha-*.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
  JAR=$(ls "${HA_HOME}"/*.jar 2>/dev/null | head -1)
fi
if [ -z "$JAR" ]; then
  echo "ERROR: Cannot find iotdb-ha jar in ${HA_HOME}"
  exit 1
fi

echo "Starting IoTDB HA Monitor..."
echo "  Home:   $HA_HOME"
echo "  Config: $CONFIG_FILE"
echo "  Jar:    $JAR"
echo "  JVM:    $JVM_OPTS"

# Start
nohup java $JVM_OPTS \
  -Dlog.dir="$LOG_DIR" \
  -cp "$JAR" \
  org.apache.iotdb.ha.HaMonitorMain \
  "$CONFIG_FILE" \
  > "$LOG_DIR/ha-monitor.out" 2>&1 &

PID=$!
echo "$PID" > "$PID_FILE"
echo "HA Monitor started (PID: $PID)"
echo "  Log: $LOG_DIR/ha-monitor.out"
echo "  API: http://localhost:8080/api/v1/cluster"
