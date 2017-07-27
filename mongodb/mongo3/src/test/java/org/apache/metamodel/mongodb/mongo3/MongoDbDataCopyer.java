/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.mongodb.mongo3;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.FileHelper;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;


/**
 * Simple example program that can copy data to a MongoDB collection
 */
public class MongoDbDataCopyer {

    private final DataContext _sourceDataContext;
    private final MongoDatabase _mongoDb;
    private final String _collectionName;
    private final String _sourceSchemaName;
    private final String _sourceTableName;

    // example copy job that will populate the mongodb with Derby data
    public static void main(String[] args) throws Exception {
        System.setProperty("derby.storage.tempDirector", FileHelper.getTempDir().getAbsolutePath());
        System.setProperty("derby.stream.error.file", File.createTempFile("metamodel-derby", ".log").getAbsolutePath());

        File dbFile = new File("../jdbc/src/test/resources/derby_testdb.jar");
        dbFile = dbFile.getCanonicalFile();
        if (!dbFile.exists()) {
            throw new IllegalStateException("File does not exist: " + dbFile);
        }

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection connection = DriverManager.getConnection("jdbc:derby:jar:(" + dbFile.getAbsolutePath()
                + ")derby_testdb;territory=en");
        connection.setReadOnly(true);

        MongoClient client =  new MongoClient();
        MongoDatabase mongoDb = client.getDatabase("orderdb_copy");

        DataContext sourceDataContext = new JdbcDataContext(connection);

        new MongoDbDataCopyer(mongoDb, "orders", sourceDataContext, "APP", "orders").copy();
        new MongoDbDataCopyer(mongoDb, "offices", sourceDataContext, "APP", "offices").copy();
        new MongoDbDataCopyer(mongoDb, "payments", sourceDataContext, "APP", "payments").copy();
        new MongoDbDataCopyer(mongoDb, "orderfact", sourceDataContext, "APP", "orderfact").copy();
        new MongoDbDataCopyer(mongoDb, "products", sourceDataContext, "APP", "products").copy();

        connection.close();
        client.close();
    }

    public MongoDbDataCopyer(MongoDatabase mongoDb, String collectionName, DataContext sourceDataContext, String sourceSchemaName,
            String sourceTableName) {
        _mongoDb = mongoDb;
        _collectionName = collectionName;
        _sourceDataContext = sourceDataContext;
        _sourceSchemaName = sourceSchemaName;
        _sourceTableName = sourceTableName;
    }

    public void copy() {
        final MongoDbDataContext targetDataContext = new MongoDbDataContext(_mongoDb);
        targetDataContext.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                final Table sourceTable = getSourceTable();
                final Table targetTable = callback.createTable(targetDataContext.getDefaultSchema(), _collectionName)
                        .like(sourceTable).execute();
                final Column[] sourceColumns = sourceTable.getColumns().toArray(new Column[sourceTable.getColumns().size()]);
                final DataSet dataSet = _sourceDataContext.query().from(sourceTable).select(sourceColumns).execute();
                while (dataSet.next()) {
                    final Row row = dataSet.getRow();

                    RowInsertionBuilder insertBuilder = callback.insertInto(targetTable);
                    for (Column column : sourceColumns) {
                        insertBuilder = insertBuilder.value(column.getName(), row.getValue(column));
                    }
                    insertBuilder.execute();
                }
                dataSet.close();
            }
        });
    }

    private Table getSourceTable() {
        final Schema schema;
        if (_sourceSchemaName != null) {
            schema = _sourceDataContext.getSchemaByName(_sourceSchemaName);
        } else {
            schema = _sourceDataContext.getDefaultSchema();
        }

        return schema.getTableByName(_sourceTableName);
    }
}
