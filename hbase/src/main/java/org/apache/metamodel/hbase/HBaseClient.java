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
final class HBaseClient {

    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private final Connection _connection;

    public HBaseClient(final Connection connection) {
        this._connection = connection;
    }

    /**
     * Insert a single row of values to a HBase table.
     * @param tableName
     * @param columns
     * @param values
     * @throws IllegalArgumentException when any parameter is null or the indexOfIdColumn is impossible
     * @throws MetaModelException when no ID-column is found.
     * @throws MetaModelException when a {@link IOException} is caught
     */
    public void insertRow(final String tableName, final HBaseColumn[] columns, final Object[] values,
            final int indexOfIdColumn) {
        if (tableName == null || columns == null || values == null || indexOfIdColumn >= values.length
                || values[indexOfIdColumn] == null) {
            throw new IllegalArgumentException(
                    "Can't insert a row without having (correct) tableName, columns, values or indexOfIdColumn");
        }
        if (columns.length != values.length) {
            throw new IllegalArgumentException("The amount of columns don't match the amount of values");
        }
        try (final Table table = _connection.getTable(TableName.valueOf(tableName))) {
            // Create a put with the values of indexOfIdColumn as rowkey
            final Put put = new Put(getValueAsByteArray(values[indexOfIdColumn]));

            // Add the other values to the put
            for (int i = 0; i < columns.length; i++) {
                if (i != indexOfIdColumn) {
                    // NullChecker is already forced within the HBaseColumn class
                    final byte[] columnFamily = Bytes.toBytes(columns[i].getColumnFamily());
                    // An HBaseColumn doesn't need a qualifier, this only works when the qualifier is empty (not
                    // null). Otherwise NullPointer exceptions will happen
                    byte[] qualifier = null;
                    if (columns[i].getQualifier() != null) {
                        qualifier = Bytes.toBytes(columns[i].getQualifier());
                    } else {
                        qualifier = Bytes.toBytes(new String(""));
                    }
                    final byte[] value = getValueAsByteArray(values[i]);
                    // A NULL value, doesn't get inserted in HBase
                    // TODO: Do we delete the cell (and therefore the qualifier) if the table get's updated by a NULL
                    // value?
                    if (value == null) {
                        logger.info("The value of column '{}' is null. This insertion is skipped", columns[i]
                                .getName());
                    } else {
                        put.addColumn(columnFamily, qualifier, value);
                    }
                }
            }
            // Add the put to the table
            table.put(put);
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    /**
     * Delete 1 row based on the key
     * @param tableName
     * @param rowKey
     * @throws IllegalArgumentException when any parameter is null
     * @throws MetaModelException when a {@link IOException} is caught
     */
    public void deleteRow(final String tableName, final Object rowKey) {
        if (tableName == null || rowKey == null) {
            throw new IllegalArgumentException("Can't delete a row without having tableName or rowKey");
        }
        byte[] rowKeyAsByteArray = getValueAsByteArray(rowKey);
        if (rowKeyAsByteArray.length > 0) {
            try (final Table table = _connection.getTable(TableName.valueOf(tableName))) {
                if (rowExists(table, rowKeyAsByteArray)) {
                    table.delete(new Delete(rowKeyAsByteArray));
                } else {
                    logger.warn("Rowkey with value {} doesn't exist in the table", rowKey.toString());
                }
            } catch (IOException e) {
                throw new MetaModelException(e);
            }
        } else {
            throw new IllegalArgumentException("Can't delete a row without an empty rowKey.");
        }
    }

    /**
     * Checks in the HBase datastore if a row exists based on the key
     * @param table
     * @param rowKey
     * @return boolean
     * @throws IOException
     */
    private boolean rowExists(final Table table, final byte[] rowKey) throws IOException {
        final Get get = new Get(rowKey);
        return !table.get(get).isEmpty();
    }

    /**
     * Creates a HBase table based on a tableName and it's columnFamilies
     * @param tableName
     * @param columnFamilies
     * @throws IllegalArgumentException when any parameter is null
     * @throws MetaModelException when a {@link IOException} is caught
     */
    public void createTable(final String tableName, final Set<String> columnFamilies) {
        if (tableName == null || columnFamilies == null || columnFamilies.isEmpty()) {
            throw new IllegalArgumentException("Can't create a table without having the tableName or columnFamilies");
        }
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
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    /**
     * Disable and drop a table from a HBase datastore
     * @param tableName
     * @throws IllegalArgumentException when tableName is null
     * @throws MetaModelException when a {@link IOException} is caught
     */
    public void dropTable(final String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Can't drop a table without having the tableName");
        }
        try (final Admin admin = _connection.getAdmin()) {
            final TableName hBasetableName = TableName.valueOf(tableName);
            admin.disableTable(hBasetableName); // A table must be disabled first, before it can be deleted
            admin.deleteTable(hBasetableName);
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    /**
     * Converts a Object value into a byte array, if it isn't a byte array already
     * @param value
     * @return value as a byte array
     */
    private byte[] getValueAsByteArray(final Object value) {
        byte[] valueAsByteArray;
        if (value == null) {
            valueAsByteArray = null;
        } else if (value instanceof byte[]) {
            valueAsByteArray = (byte[]) value;
        } else {
            valueAsByteArray = Bytes.toBytes(value.toString());
        }
        return valueAsByteArray;
    }
}
