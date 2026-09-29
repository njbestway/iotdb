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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeTransferTabletBatchReqV2 extends TPipeTransferReq {

  private static final int BATCH_REQUEST_COUNT_SERIALIZED_SIZE =
      Integer.BYTES // legacy binary request count
          + Integer.BYTES // insert node request count
          + Integer.BYTES; // raw tablet request count

  private final transient List<PipeTransferTabletInsertNodeReqV2> insertNodeReqs =
      new ArrayList<>();
  private final transient List<PipeTransferTabletRawReqV2> tabletReqs = new ArrayList<>();

  private PipeTransferTabletBatchReqV2() {
    // Empty constructor
  }

  public List<InsertBaseStatement> constructStatements() {
    final List<InsertBaseStatement> statements =
        new ArrayList<>(insertNodeReqs.size() + tabletReqs.size());

    // Keep permission checks, schema validation, and redirect metadata scoped to one table.
    final Map<String, Map<String, List<InsertRowStatement>>>
        tableModelDatabaseInsertRowStatementMap = new LinkedHashMap<>();
    final Map<String, List<InsertRowStatement>> treeModelDatabaseInsertRowStatementMap =
        new LinkedHashMap<>();
    final Map<String, List<InsertTabletStatement>> treeModelDatabaseInsertTabletStatementMap =
        new LinkedHashMap<>();

    for (final PipeTransferTabletInsertNodeReqV2 insertNodeReq : insertNodeReqs) {
      final InsertBaseStatement statement = insertNodeReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement.isWriteToTable()) {
        if (statement instanceof InsertRowStatement) {
          addTableModelInsertRowStatement(
              tableModelDatabaseInsertRowStatementMap, (InsertRowStatement) statement);
        } else if (statement instanceof InsertTabletStatement) {
          statements.add(statement);
        } else if (statement instanceof InsertRowsStatement) {
          for (final InsertRowStatement insertRowStatement :
              ((InsertRowsStatement) statement).getInsertRowStatementList()) {
            addTableModelInsertRowStatement(
                tableModelDatabaseInsertRowStatementMap, insertRowStatement);
          }
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  DataNodePipeMessages
                      .PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQV2_06D274D2,
                  insertNodeReq));
        }
        continue;
      }
      if (statement instanceof InsertRowStatement) {
        treeModelDatabaseInsertRowStatementMap
            .computeIfAbsent(statement.getDatabaseName().orElse(null), k -> new ArrayList<>())
            .add((InsertRowStatement) statement);
      } else if (statement instanceof InsertTabletStatement) {
        treeModelDatabaseInsertTabletStatementMap
            .computeIfAbsent(statement.getDatabaseName().orElse(null), k -> new ArrayList<>())
            .add((InsertTabletStatement) statement);
      } else if (statement instanceof InsertRowsStatement) {
        // Use the parent statement's database name (set by V2 constructStatement()) as the
        // grouping key for all children.  This prevents database-name loss for InsertRowsNode
        // whose own targetPath is null — the V2 request carries the correct database name
        // which constructStatement() sets on both the parent and each child, but falling back
        // to the parent here guards against any edge case where a child's databaseName was
        // not propagated.
        final String parentDatabaseName = statement.getDatabaseName().orElse(null);
        for (final InsertRowStatement insertRowStatement :
            ((InsertRowsStatement) statement).getInsertRowStatementList()) {
          treeModelDatabaseInsertRowStatementMap
              .computeIfAbsent(parentDatabaseName, k -> new ArrayList<>())
              .add(insertRowStatement);
        }
      } else {
        throw new UnsupportedOperationException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQV2_16F399B6,
                statement));
      }
    }

    for (final PipeTransferTabletRawReqV2 tabletReq : tabletReqs) {
      final InsertTabletStatement statement = tabletReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement.isWriteToTable()) {
        statements.add(statement);
        continue;
      }
      treeModelDatabaseInsertTabletStatementMap
          .computeIfAbsent(statement.getDatabaseName().orElse(null), k -> new ArrayList<>())
          .add(statement);
    }

    addTreeModelInsertRowsStatements(statements, treeModelDatabaseInsertRowStatementMap);
    addTreeModelInsertTabletsStatements(statements, treeModelDatabaseInsertTabletStatementMap);

    for (final Map.Entry<String, Map<String, List<InsertRowStatement>>> insertRows :
        tableModelDatabaseInsertRowStatementMap.entrySet()) {
      for (final Map.Entry<String, List<InsertRowStatement>> tableInsertRows :
          insertRows.getValue().entrySet()) {
        final InsertRowsStatement statement = new InsertRowsStatement();
        statement.setWriteToTable(true);
        statement.setDatabaseName(insertRows.getKey());
        statement.setInsertRowStatementList(tableInsertRows.getValue());
        statements.add(statement);
      }
    }

    return statements;
  }

  private static void addTableModelInsertRowStatement(
      final Map<String, Map<String, List<InsertRowStatement>>> databaseInsertRowStatementMap,
      final InsertRowStatement insertRowStatement) {
    databaseInsertRowStatementMap
        .computeIfAbsent(insertRowStatement.getDatabaseName().get(), k -> new LinkedHashMap<>())
        .computeIfAbsent(insertRowStatement.getTableName(), k -> new ArrayList<>())
        .add(insertRowStatement);
  }

  private void addTreeModelInsertRowsStatements(
      final List<InsertBaseStatement> statements,
      final Map<String, List<InsertRowStatement>> databaseInsertRowStatementMap) {
    for (final Map.Entry<String, List<InsertRowStatement>> insertRows :
        databaseInsertRowStatementMap.entrySet()) {
      final InsertRowsStatement statement = new InsertRowsStatement();
      statement.setInsertRowStatementList(insertRows.getValue());
      String databaseName = insertRows.getKey();
      if (databaseName == null) {
        // Defensive fallback: derive the database name from the first child's device path.
        // For tree model the database is always the first two segments of the path
        // (e.g. "root._ha.cluster.node.n1" -> "root._ha").
        final List<InsertRowStatement> rowStmts = insertRows.getValue();
        if (rowStmts != null && !rowStmts.isEmpty()) {
          final org.apache.iotdb.commons.path.PartialPath devicePath =
              rowStmts.get(0).getDevicePath();
          if (devicePath != null && devicePath.getNodes().length >= 2) {
            databaseName = devicePath.getNodes()[0] + "." + devicePath.getNodes()[1];
          }
        }
      }
      if (databaseName != null) {
        statement.setDatabaseName(databaseName);
      }
      statements.add(statement);
    }
  }

  private void addTreeModelInsertTabletsStatements(
      final List<InsertBaseStatement> statements,
      final Map<String, List<InsertTabletStatement>> databaseInsertTabletStatementMap) {
    for (final Map.Entry<String, List<InsertTabletStatement>> insertTablets :
        databaseInsertTabletStatementMap.entrySet()) {
      final InsertMultiTabletsStatement statement = new InsertMultiTabletsStatement();
      statement.setInsertTabletStatementList(insertTablets.getValue());
      String databaseName = insertTablets.getKey();
      if (databaseName == null) {
        // Defensive fallback: derive the database name from the first child's device path.
        final List<InsertTabletStatement> tabletStmts = insertTablets.getValue();
        if (tabletStmts != null && !tabletStmts.isEmpty()) {
          final org.apache.iotdb.commons.path.PartialPath devicePath =
              tabletStmts.get(0).getDevicePath();
          if (devicePath != null && devicePath.getNodes().length >= 2) {
            databaseName = devicePath.getNodes()[0] + "." + devicePath.getNodes()[1];
          }
        }
      }
      if (databaseName != null) {
        statement.setDatabaseName(databaseName);
      }
      statements.add(statement);
    }
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletBatchReqV2 toTPipeTransferReq(
      final List<ByteBuffer> insertNodeBuffers,
      final List<ByteBuffer> tabletBuffers,
      final List<String> insertNodeDataBases,
      final List<String> tabletDataBases)
      throws IOException {
    final PipeTransferTabletBatchReqV2 batchReq = new PipeTransferTabletBatchReqV2();

    batchReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeRequestType.TRANSFER_TABLET_BATCH_V2.getType();
    try (final PublicBAOS byteArrayOutputStream =
            new PublicBAOS(
                calculateSerializedSize(
                    insertNodeBuffers, tabletBuffers, insertNodeDataBases, tabletDataBases));
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      // Binary buffer, for rolling upgrade
      ReadWriteIOUtils.write(0, outputStream);

      // Insert-node and raw-tablet serializations are self-delimiting, so their lengths are not
      // written separately.
      ReadWriteIOUtils.write(insertNodeBuffers.size(), outputStream);
      for (int i = 0; i < insertNodeBuffers.size(); i++) {
        final ByteBuffer insertNodeBuffer = insertNodeBuffers.get(i);
        outputStream.write(
            insertNodeBuffer.array(),
            insertNodeBuffer.arrayOffset() + insertNodeBuffer.position(),
            insertNodeBuffer.remaining());
        ReadWriteIOUtils.write(insertNodeDataBases.get(i), outputStream);
      }

      ReadWriteIOUtils.write(tabletBuffers.size(), outputStream);
      for (int i = 0; i < tabletBuffers.size(); i++) {
        final ByteBuffer tabletBuffer = tabletBuffers.get(i);
        outputStream.write(
            tabletBuffer.array(),
            tabletBuffer.arrayOffset() + tabletBuffer.position(),
            tabletBuffer.remaining());
        ReadWriteIOUtils.write(tabletDataBases.get(i), outputStream);
      }

      batchReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return batchReq;
  }

  static int calculateSerializedSize(
      final List<ByteBuffer> insertNodeBuffers,
      final List<ByteBuffer> tabletBuffers,
      final List<String> insertNodeDataBases,
      final List<String> tabletDataBases) {
    int size = BATCH_REQUEST_COUNT_SERIALIZED_SIZE;
    for (int i = 0; i < insertNodeBuffers.size(); i++) {
      size += insertNodeBuffers.get(i).remaining();
      size += ReadWriteIOUtils.sizeToWrite(insertNodeDataBases.get(i));
    }
    for (int i = 0; i < tabletBuffers.size(); i++) {
      size += tabletBuffers.get(i).remaining();
      size += ReadWriteIOUtils.sizeToWrite(tabletDataBases.get(i));
    }
    return size;
  }

  public static PipeTransferTabletBatchReqV2 fromTPipeTransferReq(
      final org.apache.iotdb.service.rpc.thrift.TPipeTransferReq transferReq) {
    final PipeTransferTabletBatchReqV2 batchReq = new PipeTransferTabletBatchReqV2();

    // Binary req, for rolling upgrade
    ReadWriteIOUtils.readInt(transferReq.body);

    final int insertNodeCount = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < insertNodeCount; ++i) {
      batchReq.insertNodeReqs.add(
          PipeTransferTabletInsertNodeReqV2.toTabletInsertNodeReq(
              (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null),
              ReadWriteIOUtils.readString(transferReq.body)));
    }

    final int rawTabletCount = ReadWriteIOUtils.readInt(transferReq.body);
    final TabletStringInternPool tabletStringInternPool =
        rawTabletCount > 1 ? new TabletStringInternPool() : null;
    for (int i = 0; i < rawTabletCount; ++i) {
      batchReq.tabletReqs.add(
          PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
              transferReq.body, tabletStringInternPool));
    }

    batchReq.version = transferReq.version;
    batchReq.type = transferReq.type;
    batchReq.body = transferReq.body;

    return batchReq;
  }

  /////////////////////////////// TestOnly ///////////////////////////////

  @TestOnly
  public List<PipeTransferTabletInsertNodeReqV2> getInsertNodeReqs() {
    return insertNodeReqs;
  }

  @TestOnly
  public List<PipeTransferTabletRawReqV2> getTabletReqs() {
    return tabletReqs;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTransferTabletBatchReqV2 that = (PipeTransferTabletBatchReqV2) obj;
    return Objects.equals(insertNodeReqs, that.insertNodeReqs)
        && Objects.equals(tabletReqs, that.tabletReqs)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(insertNodeReqs, tabletReqs, version, type, body);
  }
}
