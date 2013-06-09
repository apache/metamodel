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
package org.eobjects.metamodel.intercept;

import org.eobjects.metamodel.AbstractUpdateCallback;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;

/**
 * {@link UpdateCallback} wrapper that allows adding interceptors for certain operations. 
 */
final class InterceptableUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    private final UpdateCallback _updateCallback;
    private final InterceptorList<TableCreationBuilder> _tableCreationInterceptors;
    private final InterceptorList<TableDropBuilder> _tableDropInterceptors;
    private final InterceptorList<RowInsertionBuilder> _rowInsertionInterceptors;
    private final InterceptorList<RowUpdationBuilder> _rowUpdationInterceptors;
    private final InterceptorList<RowDeletionBuilder> _rowDeletionInterceptors;

    public InterceptableUpdateCallback(InterceptableDataContext dataContext, UpdateCallback updateCallback,
            InterceptorList<TableCreationBuilder> tableCreationInterceptors,
            InterceptorList<TableDropBuilder> tableDropInterceptors,
            InterceptorList<RowInsertionBuilder> rowInsertionInterceptors,
            InterceptorList<RowUpdationBuilder> rowUpdationInterceptors,
            InterceptorList<RowDeletionBuilder> rowDeletionInterceptors) {
        super(dataContext);
        _updateCallback = updateCallback;
        _tableCreationInterceptors = tableCreationInterceptors;
        _tableDropInterceptors = tableDropInterceptors;
        _rowInsertionInterceptors = rowInsertionInterceptors;
        _rowUpdationInterceptors = rowUpdationInterceptors;
        _rowDeletionInterceptors = rowDeletionInterceptors;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        TableCreationBuilder tabelCreationBuilder = _updateCallback.createTable(schema, name);
        if (_tableCreationInterceptors.isEmpty()) {
            return tabelCreationBuilder;
        }
        return new InterceptableTableCreationBuilder(tabelCreationBuilder, _tableCreationInterceptors);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        RowInsertionBuilder rowInsertionBuilder = _updateCallback.insertInto(table);
        if (_rowInsertionInterceptors.isEmpty()) {
            return rowInsertionBuilder;
        }
        return new InterceptableRowInsertionBuilder(rowInsertionBuilder, _rowInsertionInterceptors);
    }

    @Override
    public boolean isCreateTableSupported() {
        return _updateCallback.isCreateTableSupported();
    }

    @Override
    public boolean isDropTableSupported() {
        return _updateCallback.isDropTableSupported();
    }

    @Override
    public TableDropBuilder dropTable(Table table) {
        TableDropBuilder tableDropBuilder = _updateCallback.dropTable(table);
        if (_tableDropInterceptors.isEmpty()) {
            return tableDropBuilder;
        }
        return new InterceptableTableDropBuilder(tableDropBuilder, _tableDropInterceptors);
    }

    @Override
    public boolean isInsertSupported() {
        return _updateCallback.isInsertSupported();
    }

    @Override
    public boolean isUpdateSupported() {
        return _updateCallback.isUpdateSupported();
    }

    @Override
    public RowUpdationBuilder update(Table table) {
        RowUpdationBuilder rowUpdationBuilder = _updateCallback.update(table);
        if (_rowUpdationInterceptors.isEmpty()) {
            return rowUpdationBuilder;
        }
        return new InterceptableRowUpdationBuilder(rowUpdationBuilder, _rowUpdationInterceptors);
    }

    @Override
    public boolean isDeleteSupported() {
        return _updateCallback.isDeleteSupported();
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) {
        RowDeletionBuilder rowDeletionBuilder = _updateCallback.deleteFrom(table);
        if (_rowDeletionInterceptors.isEmpty()) {
            return rowDeletionBuilder;
        }
        return new InterceptableRowDeletionBuilder(rowDeletionBuilder, _rowDeletionInterceptors);
    }
}
