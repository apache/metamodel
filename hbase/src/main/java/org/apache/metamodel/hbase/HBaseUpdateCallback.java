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

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class HBaseUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    private final HBaseConfiguration _configuration;

    private final HBaseDataContext _dataContext;

    public HBaseUpdateCallback(HBaseDataContext dataContext) {
        super(dataContext);
        _configuration = dataContext.getConfiguration();
        _dataContext = dataContext;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        throw new UnsupportedOperationException(
                "Use createTable(Schema schema, String name, HBaseColumn[] outputColumns)");
    }

    public HBaseCreateTableBuilder createTable(Schema schema, String name, HBaseColumn[] outputColumns)
            throws IllegalArgumentException, IllegalStateException {
        return new HBaseCreateTableBuilder(this, schema, name, outputColumns);
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new HBaseTableDropBuilder(table, this);
    }

    public void dropTableExecute(Table table) {
        try {
            final HBaseWriter HbaseWriter = new HBaseWriter(HBaseDataContext.createConfig(_configuration));
            HbaseWriter.dropTable(table.getName());
            MutableSchema schema = (MutableSchema) table.getSchema();
            schema.removeTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        throw new UnsupportedOperationException("Use insertInto(String tableName, HBaseColumn[] outputColumns)");
    }

    public HBaseRowInsertionBuilder insertInto(String tableName, HBaseColumn[] outputColumns)
            throws IllegalArgumentException, IllegalStateException, UnsupportedOperationException {
        Table table = getTable(tableName);
        return insertInto(table, outputColumns);
    }

    public HBaseRowInsertionBuilder insertInto(Table table, HBaseColumn[] outputColumns)
            throws IllegalArgumentException, IllegalStateException, UnsupportedOperationException {
        validateTable(table);
        return new HBaseRowInsertionBuilder(this, table, outputColumns);
    }

    private void validateTable(Table table) {
        if (!(table instanceof HBaseTable)) {
            throw new IllegalArgumentException("Not a valid HBase table: " + table);
        }
    }

    protected synchronized void writeRow(HBaseTable hBaseTable, HBaseColumn[] outputColumns, Object[] values) {
        try {
            final HBaseWriter HbaseWriter = new HBaseWriter(HBaseDataContext.createConfig(_configuration));
            HbaseWriter.writeRow(hBaseTable, outputColumns, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isDeleteSupported() {
        return false;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public boolean tableAlreadyExists(String tableName) {
        return _dataContext.getMainSchema().getTableByName(tableName) == null ? false : true;
    }

    public HBaseConfiguration getConfiguration() {
        return _configuration;
    }

    public HBaseDataContext getDataContext() {
        return _dataContext;
    }
}
