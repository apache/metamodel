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
package org.apache.metamodel.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * A {@link DataContext} used to serve MetaModel support for collections of Java
 * objects and key/value maps.
 */
public class PojoDataContext extends QueryPostprocessDataContext implements UpdateableDataContext, Serializable {

    private static final long serialVersionUID = 1L;
    
    public static final String DEFAULT_SCHEMA_NAME = "Schema";

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
        this(DEFAULT_SCHEMA_NAME, tables);
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
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final TableDataProvider<?> pojoTable = _tables.get(table.getName());
        if (pojoTable == null) {
            throw new IllegalArgumentException("No such POJO table: " + table.getName());
        }

        final List<SelectItem> selectItems = columns.stream().map(SelectItem::new).collect(Collectors.toList());

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
    public UpdateSummary executeUpdate(UpdateScript update) {
        final PojoUpdateCallback updateCallback = new PojoUpdateCallback(this);
        synchronized (this) {
            update.run(updateCallback);
        }
        return updateCallback.getUpdateSummary();
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
