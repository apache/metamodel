/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel;

import java.util.Arrays;

import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;

/**
 * Abstract implementation of the {@link UpdateCallback} interface. Implements
 * only the data store agnostic methods.
 * 
 * @author Kasper Sørensen
 */
public abstract class AbstractUpdateCallback implements UpdateCallback {

    private final DataContext _dataContext;

    public AbstractUpdateCallback(DataContext dataContext) {
        _dataContext = dataContext;
    }

    @Override
    public TableCreationBuilder createTable(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException {
        Schema schema = getSchema(schemaName);
        return createTable(schema, tableName);
    }

    @Override
    public TableDropBuilder dropTable(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        Table table = getTable(schemaName, tableName);
        return dropTable(table);
    }

    @Override
    public TableDropBuilder dropTable(Schema schema, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        Table table = schema.getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Nu such table '" + tableName + "' found in schema: " + schema
                    + ". Available tables are: " + Arrays.toString(schema.getTableNames()));
        }
        return dropTable(table);
    }

    @Override
    public final RowInsertionBuilder insertInto(String tableName) throws IllegalArgumentException,
            IllegalStateException {
        return insertInto(getTable(tableName));
    }

    @Override
    public RowInsertionBuilder insertInto(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        return insertInto(getTable(schemaName, tableName));
    }

    private Table getTable(String schemaName, String tableName) {
        final Schema schema = getSchema(schemaName);
        final Table table = schema.getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Nu such table '" + tableName + "' found in schema: " + schema
                    + ". Available tables are: " + Arrays.toString(schema.getTableNames()));
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
    public RowDeletionBuilder deleteFrom(String schemaName, String tableName) throws IllegalArgumentException,
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
        // since 2.0 all updateable datacontext have create table support
        return true;
    }

    @Override
    public boolean isUpdateSupported() {
        return isInsertSupported() && isDeleteSupported();
    }

    @Override
    public RowUpdationBuilder update(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException {
        final Table table = getTable(schemaName, tableName);
        return update(table);
    }

    @Override
    public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new DeleteAndInsertBuilder(this, table);
    }
}
