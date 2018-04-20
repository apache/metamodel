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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metamodel.MetaModelException;

public final class HBaseWriter extends Configured {

    static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
    static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
    static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
    static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

    private final Connection _connection;

    public HBaseWriter(Configuration configuration) throws IOException {
        _connection = ConnectionFactory.createConnection(configuration);
    }

    public void writeRow(HBaseTable hBaseTable, HBaseColumn[] outputColumns, Object[] values) throws IOException {
        try {
            Table table = _connection.getTable(TableName.valueOf(hBaseTable.getName()));
            try {
                int indexOfIdColumn = 0;
                boolean idColumnFound = false;
                while (!idColumnFound && indexOfIdColumn < outputColumns.length) {
                    if (outputColumns[indexOfIdColumn].getColumnFamily().equals(HBaseDataContext.FIELD_ID)) {
                        idColumnFound = true;
                    } else {
                        indexOfIdColumn++;
                    }
                }
                if (!idColumnFound) {
                    throw new MetaModelException("The ID Column family was not found");
                }

                Put put = new Put(Bytes.toBytes(values[indexOfIdColumn].toString()));

                for (int i = 0; i < outputColumns.length; i++) {
                    if (!outputColumns[i].getColumnFamily().equals(HBaseDataContext.FIELD_ID)) {
                        put.addColumn(Bytes.toBytes(outputColumns[i].getColumnFamily()), Bytes.toBytes(outputColumns[i]
                                .getQualifier()), Bytes.toBytes(values[i].toString()));
                    }
                }
                table.put(put);
            } finally {
                table.close();
            }
        } finally {
            _connection.close();
        }
    }

    public void deleteRow(HBaseTable hBaseTable, Object key) throws IOException {
        try {
            Table table = _connection.getTable(TableName.valueOf(hBaseTable.getName()));
            try {
                table.delete(new Delete(Bytes.toBytes(key.toString())));
            } finally {
                table.close();
            }
        } finally {
            _connection.close();
        }
    }

    public void createTable(String tableName, Set<String> columnFamilies) throws IOException {
        try {
            // Create table
            Admin admin = _connection.getAdmin();
            try {
                TableName hBasetableName = TableName.valueOf(tableName);
                HTableDescriptor tableDescriptor = new HTableDescriptor(hBasetableName);
                for (String columnFamilie : columnFamilies) {
                    if (!columnFamilie.equals(HBaseDataContext.FIELD_ID)) {
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamilie));
                    }
                }
                admin.createTable(tableDescriptor);
                HTableDescriptor[] tables = admin.listTables();
                if (tables.length != 1 && Bytes.equals(hBasetableName.getName(), tables[0].getTableName().getName())) {
                    throw new IOException("Failed create of table");
                }
            } finally {
                admin.close();
            }
        } finally {
            _connection.close();
        }

    }

    public void dropTable(String tableName) throws IOException {
        try {
            Admin admin = _connection.getAdmin();
            try {
                TableName hBasetableName = TableName.valueOf(tableName);
                admin.disableTable(hBasetableName);
                admin.deleteTable(hBasetableName);
            } finally {
                admin.close();
            }
        } finally {
            _connection.close();
        }
    }
}
