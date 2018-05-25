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
package org.apache.metamodel.hbase;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metamodel.MetaModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can perform client-operations on a HBase datastore
 */
public final class HBaseClient {

    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private final Connection _connection;

    public HBaseClient(Connection connection) {
        this._connection = connection;
    }

    /**
     * Write a single row of values to a HBase table
     * @param hBaseTable
     * @param columns
     * @param values
     * @throws IOException
     */
    public void writeRow(HBaseTable hBaseTable, HBaseColumn[] columns, Object[] values) throws IOException {
        try (final Table table = _connection.getTable(TableName.valueOf(hBaseTable.getName()))) {
            int indexOfIdColumn = getIndexOfIdColumn(columns);

            // Create a put with the values of indexOfIdColumn as rowkey
            final Put put = new Put(Bytes.toBytes(values[indexOfIdColumn].toString()));

            // Add the other values to the put
            for (int i = 0; i < columns.length; i++) {
                if (i != indexOfIdColumn) {
                    put.addColumn(Bytes.toBytes(columns[i].getColumnFamily()), Bytes.toBytes(columns[i].getQualifier()),
                            Bytes.toBytes(values[i].toString()));
                }
            }
            // Add the put to the table
            table.put(put);
        }
    }

    /**
     * Gets the index of the ID-column
     * Throws an {@link MetaModelException} when no ID-column is found.
     * @param columns
     * @return 
     */
    private int getIndexOfIdColumn(HBaseColumn[] columns) {
        int indexOfIdColumn = 0;
        boolean idColumnFound = false;
        while (!idColumnFound && indexOfIdColumn < columns.length) {
            if (columns[indexOfIdColumn].getColumnFamily().equals(HBaseDataContext.FIELD_ID)) {
                idColumnFound = true;
            } else {
                indexOfIdColumn++;
            }
        }
        if (!idColumnFound) {
            throw new MetaModelException("The ID Column family was not found");
        }
        return indexOfIdColumn;
    }

    /**
     * Delete 1 row based on the key
     * @param hBaseTable
     * @param key
     * @throws IOException
     */
    public void deleteRow(HBaseTable hBaseTable, Object key) throws IOException {
        try (final Table table = _connection.getTable(TableName.valueOf(hBaseTable.getName()));) {
            if (rowExists(table, key) == true) {
                table.delete(new Delete(Bytes.toBytes(key.toString())));
            } else {
                logger.warn("Rowkey with value " + key.toString() + " doesn't exist in the table");
            }
        }
    }

    /**
     * Checks in the HBase datastore if a row exists based on the key
     * @param table
     * @param key
     * @return boolean
     * @throws IOException
     */
    private boolean rowExists(Table table, Object key) throws IOException {
        final Get get = new Get(Bytes.toBytes(key.toString()));
        return !table.get(get).isEmpty();
    }

    /**
     * Creates a HBase table based on a tableName and it's columnFamilies
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void createTable(String tableName, Set<String> columnFamilies) throws IOException {
        try (final Admin admin = _connection.getAdmin()) {
            final TableName hBasetableName = TableName.valueOf(tableName);
            final HTableDescriptor tableDescriptor = new HTableDescriptor(hBasetableName);
            // Add all columnFamilies to the tableDescriptor.
            for (final String columnFamilie : columnFamilies) {
                // The ID-column isn't needed because, it will automatically be created.
                if (!columnFamilie.equals(HBaseDataContext.FIELD_ID)) {
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamilie));
                }
            }
            admin.createTable(tableDescriptor);
        }
    }

    /**
     * Disable and drop a table from a HBase datastore
     * @param tableName
     * @throws IOException
     */
    public void dropTable(String tableName) throws IOException {
        try (final Admin admin = _connection.getAdmin()) {
            final TableName hBasetableName = TableName.valueOf(tableName);
            admin.disableTable(hBasetableName); // A table must be disabled first, before it can be deleted
            admin.deleteTable(hBasetableName);
        }
    }
}
