@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@REM IoTDB HA Monitor start script (Windows)

@echo off
setlocal

set SCRIPT_DIR=%~dp0
set HA_HOME=%SCRIPT_DIR%..

set CONFIG_FILE=%HA_HOME%\config.json
if not "%~1"=="" set CONFIG_FILE=%~1

set JVM_OPTS=-Xms256m -Xmx512m

if not exist "%HA_HOME%\logs" mkdir "%HA_HOME%\logs"

@REM Find the jar
set JAR=
for %%f in ("%HA_HOME%\target\iotdb-ha-*.jar") do set JAR=%%f
if not defined JAR for %%f in ("%HA_HOME%\*.jar") do set JAR=%%f
if not defined JAR (
  echo ERROR: Cannot find iotdb-ha jar
  exit /b 1
)

echo Starting IoTDB HA Monitor...
echo   Home:   %HA_HOME%
echo   Config: %CONFIG_FILE%
echo   Jar:    %JAR%

java %JVM_OPTS% -cp "%JAR%" org.apache.iotdb.ha.HaMonitorMain "%CONFIG_FILE%"

endlocal
