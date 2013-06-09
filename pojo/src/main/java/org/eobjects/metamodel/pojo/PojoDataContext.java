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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.MaxRowsDataSet;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.SimpleTableDef;

/**
 * A {@link DataContext} used to serve MetaModel support for collections of Java
 * objects and key/value maps.
 */
public class PojoDataContext extends QueryPostprocessDataContext implements UpdateableDataContext, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, TableDataProvider<?>> _tables;
    private final String _schemaName;

    /**
     * Creates a new POJO data context that is empty but can be populated at
     * will.
     */
    public PojoDataContext() {
        this(new ArrayList<TableDataProvider<?>>());
    }

    /**
     * Creates a new POJO data context based on the provided
     * {@link TableDataProvider}s.
     * 
     * @param tables
     */
    public PojoDataContext(List<TableDataProvider<?>> tables) {
        this("Schema", tables);
    }

    /**
     * Creates a new POJO data context based on the provided
     * {@link TableDataProvider}s.
     * 
     * @param schemaName
     *            the name of the created schema
     * @param tableProviders
     */
    public PojoDataContext(String schemaName, @SuppressWarnings("rawtypes") TableDataProvider... tableProviders) {
        this(schemaName, Arrays.<TableDataProvider<?>> asList(tableProviders));
    }

    /**
     * Creates a new POJO data context based on the provided
     * {@link TableDataProvider}s.
     * 
     * @param schemaName
     *            the name of the created schema
     * @param tables
     */
    public PojoDataContext(String schemaName, List<TableDataProvider<?>> tables) {
        if (schemaName == null) {
            throw new IllegalArgumentException("Schema name cannot be null");
        }
        _schemaName = schemaName;
        _tables = new TreeMap<String, TableDataProvider<?>>();
        for (TableDataProvider<?> pojoTable : tables) {
            addTableDataProvider(pojoTable);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final TableDataProvider<?> pojoTable = _tables.get(table.getName());
        if (pojoTable == null) {
            throw new IllegalArgumentException("No such POJO table: " + table.getName());
        }

        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        DataSet dataSet = new PojoDataSet(pojoTable, selectItems);

        if (maxRows > 0) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }

        return dataSet;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(getMainSchemaName());

        for (TableDataProvider<?> pojoTable : _tables.values()) {
            final SimpleTableDef tableDef = pojoTable.getTableDef();
            final MutableTable table = tableDef.toTable();
            table.setSchema(schema);
            schema.addTable(table);
        }

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaName;
    }

    @Override
    public void executeUpdate(UpdateScript update) {
        PojoUpdateCallback updateCallback = new PojoUpdateCallback(this);
        synchronized (this) {
            update.run(updateCallback);
        }
    }

    protected void addTableDataProvider(TableDataProvider<?> tableDataProvider) {
        _tables.put(tableDataProvider.getName(), tableDataProvider);
    }

    public void insert(String tableName, Map<String, Object> recordData) {
        TableDataProvider<?> table = _tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("No table data provider for table: " + tableName);
        }
        table.insert(recordData);
    }

}
