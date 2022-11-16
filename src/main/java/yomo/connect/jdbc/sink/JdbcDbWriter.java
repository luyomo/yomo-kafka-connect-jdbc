/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.yomo-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package yomo.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

//import org.json.JSONArray;
//import org.json.JSONException;
import org.json.JSONObject;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
//import java.util.Objects;

import yomo.connect.jdbc.dialect.DatabaseDialect;
import yomo.connect.jdbc.util.CachedConnectionProvider;
import yomo.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    log.debug("JdbcDbWriter -> new:  config: {}, dbDialect: {}, dbStructure: {}",
            config, dbDialect, dbStructure);
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
    
        final TableId tableId = destinationTable(record.topic());
        log.debug("JdbcDbWriter->write: {}", tableId);
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }

        // Skip the record whose valueSchema is null or no op type
        if (record.valueSchema() == null) {
          return;
        }
        Struct valueStruct = (Struct) record.value();
        String op = valueStruct.getString("op");
        if (op == null) {
          return;
        }

        // Prepare key schema and objects
        Struct keyStruct = (Struct) record.key();
        String keyID = (String) keyStruct.get("id");
        JSONObject jsonKeyObj = new JSONObject(keyID);
        Schema convert2KeySchema = SchemaBuilder.struct()
            .field("$oid", Schema.STRING_SCHEMA)
            .build();
        Struct keyObject = new Struct(convert2KeySchema).put("$oid", jsonKeyObj.get("$oid"));

        // Prepare value schema and objects
        Struct valueObject = null;
        // Define the value schema for downstream DB
        Schema convert2ValueSchema = SchemaBuilder.struct()
            .field("$oid", Schema.STRING_SCHEMA)
            .field("t_json", Schema.STRING_SCHEMA)
            .build();
        if (!op.equals("d")) {
          String afterStruct = (String) valueStruct.get("after");
          JSONObject jsonObj = new JSONObject(afterStruct);
          valueObject = new Struct(convert2ValueSchema)
              .put("$oid", jsonKeyObj.get("$oid"))
              .put("t_json", afterStruct);
        }

        buffer.add(new SinkRecord(record.topic(),
                                  record.kafkaPartition(),
                                  convert2KeySchema,
                                  keyObject,
                                  convert2ValueSchema,
                                  valueObject,
                                  record.kafkaOffset()));
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
