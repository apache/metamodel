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
package org.apache.metamodel;

import java.util.Arrays;

import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;

/**
 * Abstract implementation of the {@link UpdateCallback} interface. Implements
 * only the data store agnostic methods.
 */
public abstract class AbstractUpdateCallback implements UpdateCallback {

    private final DataContext _dataContext;

    public AbstractUpdateCallback(DataContext dataContext) {
        _dataContext = dataContext;
    }

    @Override
    public TableCreationBuilder createTable(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException {
        final Schema schema = getSchema(schemaName);
        return createTable(schema, tableName);
    }

    @Override
    public final TableDropBuilder dropTable(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        final Table table = getTable(schemaName, tableName);
        return dropTable(table);
    }

    @Override
    public final TableDropBuilder dropTable(Schema schema, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        final Table table = schema.getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Nu such table '" + tableName + "' found in schema: " + schema
                    + ". Available tables are: " + Arrays.toString(schema.getTableNames().toArray()));
        }
        return dropTable(table);
    }

    @Override
    public final RowInsertionBuilder insertInto(String tableName) throws IllegalArgumentException,
            IllegalStateException {
        return insertInto(getTable(tableName));
    }

    @Override
    public final RowInsertionBuilder insertInto(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        return insertInto(getTable(schemaName, tableName));
    }

    private Table getTable(String schemaName, String tableName) {
        final Schema schema = getSchema(schemaName);
        final Table table = schema.getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Nu such table '" + tableName + "' found in schema: " + schema
                    + ". Available tables are: " + Arrays.toString(schema.getTableNames().toArray()));
        }
        return table;
    }

    private Schema getSchema(String schemaName) {
        final Schema schema = _dataContext.getSchemaByName(schemaName);
        if (schema == null) {
            throw new IllegalArgumentException("No such schema: " + schemaName);
        }
        return schema;
    }

    @Override
    public final RowDeletionBuilder deleteFrom(String tableName) {
        return deleteFrom(getTable(tableName));
    }

    @Override
    public final RowDeletionBuilder deleteFrom(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        final Table table = getTable(schemaName, tableName);
        return deleteFrom(table);
    }

    @Override
    public final TableDropBuilder dropTable(String tableName) {
        return dropTable(getTable(tableName));
    }

    @Override
    public final RowUpdationBuilder update(String tableName) {
        return update(getTable(tableName));
    }

    private Table getTable(String tableName) {
        Table table = getDataContext().getTableByQualifiedLabel(tableName);
        if (table == null) {
            throw new IllegalArgumentException("No such table: " + tableName);
        }
        return table;
    }

    @Override
    public DataContext getDataContext() {
        return _dataContext;
    }

    @Override
    public boolean isCreateTableSupported() {
        // since 2.0 all updateable datacontext have create table support
        return true;
    }

    @Override
    public boolean isInsertSupported() {
        // since 2.0 all updateable datacontext have insert into table support
        return true;
    }

    @Override
    public boolean isUpdateSupported() {
        return isInsertSupported() && isDeleteSupported();
    }

    @Override
    public final RowUpdationBuilder update(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        final Table table = getTable(schemaName, tableName);
        return update(table);
    }

    @Override
    public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new DeleteAndInsertBuilder(this, table);
    }
    
    public UpdateSummary getUpdateSummary() {
        return DefaultUpdateSummary.unknownUpdates();
    }
}
