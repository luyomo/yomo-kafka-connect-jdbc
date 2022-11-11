/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
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
        //Object testValues = new Object();
        //log.debug("JdbcDbWriter->write: {}", record.value());
        //record.put("t_json", testValues);

        // Replace PG data
        // Object testValue = record.value();
        // log.debug("jdbc:write: {} and {} ", testValue, record.getClass().getName());

        // Struct simpleStruct = new Struct(record.valueSchema()).put("pk_col", 1000L)
        //         .put("t_json", "{\"key\":\"Value96\"}")
        //         .put("pg_timestamp", "2022-11-09 11:26:57.696245");
        // log.debug("jdbc:write: simple struct {}", simpleStruct);
        // log.debug("jdbc:write: {}", record.valueSchema().fields());
        // END REPLACEMENT

        //Struct testStruct = testValue.getStruct("t_json");
        //testValue.put("t_json", "{\"key\":\"Value96\"}");
        //testValue.put("t_json", testValue);
        //record.setValue(testValue);
        //String topic, int partition, Schema keySchema, Object key,
        //Schema valueSchema, Object value, long kafkaOffset

        final Schema convert2KeySchema = SchemaBuilder.struct()
            .field("$oid", Schema.STRING_SCHEMA)
            .build();
        Schema convert2ValueSchema = SchemaBuilder.struct()
            .field("$oid", Schema.STRING_SCHEMA)
            .field("t_json", Schema.STRING_SCHEMA)
            .build();
        log.debug("------- New record from mongo ");
        log.debug("JdbcDBWriter: record: {}", record);
        //log.debug("JdbcDBWriter: record key: {}", record.key());
        Struct valueStruct = (Struct) record.value();
        Struct valueObject = null;

        Struct keyStruct = (Struct) record.key();
        String keyID = (String) keyStruct.get("id");
        //log.debug("JdbcDBWriter: key string: {}", keyID);
        JSONObject jsonKeyObj = new JSONObject(keyID);
        final Struct keyObject = new Struct(convert2KeySchema).put("$oid", jsonKeyObj.get("$oid"));

        if (record.valueSchema() == null) {
          return;
        }
        String op = valueStruct.getString("op");
        if (op == null) {
          return;
        }

        log.debug("JdbcDBWriter: value schema list: {}", record.valueSchema().fields());
        log.debug("JdbcDBWriter: record value: {}", valueStruct.toString());
        //log.debug("JdbcDBWriter: op: {} ", op);
        if (!op.equals("d")) {
          String afterStruct = (String) valueStruct.get("after");
          JSONObject jsonObj = new JSONObject(afterStruct);
          valueObject = new Struct(convert2ValueSchema)
              .put("$oid", jsonKeyObj.get("$oid"))
              .put("t_json", afterStruct);
          log.debug("JdbcDBWriter: record value $oid: {}",
              jsonObj.getJSONObject("_id").get("$oid"));
          log.debug("JdbcDBWriter: ValueObjects: {}", valueObject);
        }

        log.debug("JdbcDBWriter: record key: {}", jsonKeyObj.get("$oid"));
        log.debug("JdbcDBWriter: record value: {}", record.value());
        // log.debug("JdbcDBWriter: keySchema: {}", convert2KeySchema);
        // log.debug("JdbcDBWriter: keyObject: {}", keyObject);
        // log.debug("JdbcDBWriter: valueSchema: {}", convert2ValueSchema);
        
        buffer.add(new SinkRecord(record.topic(),
                                  record.kafkaPartition(),
                                  convert2KeySchema,
                                  keyObject,
                                  convert2ValueSchema,
                                  valueObject,
                                  record.kafkaOffset()));
        //buffer.add(new SinkRecord(record.topic(),
        //                          record.kafkaPartition(),
        //                          record.keySchema(),
        //                          record.key(),
        //                          record.valueSchema(),
        //                          simpleStruct,
        //                          record.kafkaOffset()));
        //buffer.add(record);
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
