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
package org.eobjects.metamodel.create;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.TableType;

/**
 * Represents a single CREATE TABLE operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built create
 * table implementation. Some {@link DataContext}s may even optimize
 * specifically based on the knowledge that there will only be a single table
 * created.
 */
public final class CreateTable implements UpdateScript {

    private final MutableTable _table;

    public CreateTable(Schema schema, String tableName) {
        _table = new MutableTable(tableName, TableType.TABLE, schema);
    }

    public CreateTable(String schemaName, String tableName) {
        _table = new MutableTable(tableName, TableType.TABLE, new MutableSchema(schemaName));
    }

    /**
     * Adds a column to the current builder
     * 
     * @param name
     * @return
     */
    public ColumnBuilder<CreateTableColumnBuilder> withColumn(String name) {
        MutableColumn column = new MutableColumn(name);
        _table.addColumn(column);
        return new CreateTableColumnBuilder(this, column);
    }

    @Override
    public void run(UpdateCallback callback) {
        callback.createTable(_table.getSchema().getName(), _table.getName()).like(_table).execute();
    }
}
