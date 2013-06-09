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
package org.eobjects.metamodel.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eobjects.metamodel.AbstractUpdateCallback;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.create.AbstractTableCreationBuilder;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.AbstractTableDropBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.AbstractRowInsertionBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.SimpleTableDef;

/**
 * Update callback for the pojo module.
 */
final class PojoUpdateCallback extends AbstractUpdateCallback {

    private final PojoDataContext _dataContext;

    public PojoUpdateCallback(PojoDataContext dataContext) {
        super(dataContext);
        _dataContext = dataContext;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {

        return new AbstractTableCreationBuilder<PojoUpdateCallback>(this, schema, name) {

            @Override
            public Table execute() throws MetaModelException {
                MutableTable table = getTable();
                MutableSchema schema = (MutableSchema) getSchema();
                table.setSchema(schema);
                schema.addTable(table);
                _dataContext.addTableDataProvider(new MapTableDataProvider(new SimpleTableDef(table),
                        new ArrayList<Map<String, ?>>()));
                return table;
            }
        };
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new AbstractTableDropBuilder(table) {
            @Override
            public void execute() throws MetaModelException {
                MutableTable mutableTable = (MutableTable) getTable();
                MutableSchema schema = (MutableSchema) mutableTable.getSchema();
                schema.removeTable(mutableTable);
                mutableTable.setSchema(null);
            }
        };
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new AbstractRowInsertionBuilder<PojoUpdateCallback>(this, table) {
            @Override
            public void execute() throws MetaModelException {
                boolean[] explicitNulls = getExplicitNulls();
                Column[] columns = getColumns();
                Object[] values = getValues();
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 0; i < values.length; i++) {
                    if (values[i] != null || explicitNulls[i]) {
                        map.put(columns[i].getName(), values[i]);
                    }
                }
                _dataContext.insert(getTable().getName(), map);
            }
        };
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new AbstractRowDeletionBuilder(table) {

            @Override
            public void execute() throws MetaModelException {
                final DataSet dataSet = _dataContext.query().from(getTable()).select(getTable().getColumns()).execute();
                final PojoDataSet<?> pojoDataSet = (PojoDataSet<?>) dataSet;
                final List<FilterItem> whereItems = getWhereItems();
                while (pojoDataSet.next()) {
                    boolean delete = true;
                    final Row row = pojoDataSet.getRow();
                    for (FilterItem whereItem : whereItems) {
                        if (!whereItem.evaluate(row)) {
                            delete = false;
                            break;
                        }
                    }
                    if (delete) {
                        pojoDataSet.remove();
                    }
                }
            }
        };
    }

}
