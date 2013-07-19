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
package org.eobjects.metamodel.access;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.data.CachingDataSetHeader;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.InMemoryDataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.schema.TableType;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexData.ColumnDescriptor;

/**
 * DataContext implementation for MS Access database files.
 * 
 * @author Kasper Sørensen
 */
public final class AccessDataContext extends QueryPostprocessDataContext {

    private final File _file;
    private Database _database;

    public AccessDataContext(File file) {
        _file = file;
    }

    public AccessDataContext(String filename) {
        this(new File(filename));
    }

    private Database getDatabase() {
        if (_database == null) {
            synchronized (this) {
                if (_database == null) {
                    try {
                        _database = Database.open(_file, true);
                    } catch (IOException e) {
                        throw new MetaModelException(e);
                    }
                }
            }
        }
        return _database;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        MutableSchema schema = new MutableSchema(_file.getName());
        Database db = getDatabase();
        for (com.healthmarketscience.jackcess.Table mdbTable : db) {
            final MutableTable table = new MutableTable(mdbTable.getName(), TableType.TABLE, schema);

            try {
                int i = 0;
                for (com.healthmarketscience.jackcess.Column mdbColumn : mdbTable.getColumns()) {
                    final ColumnType columnType = ColumnType.convertColumnType(mdbColumn.getSQLType());
                    final MutableColumn column = new MutableColumn(mdbColumn.getName(), columnType, table, i, null);
                    column.setColumnSize((int) mdbColumn.getLength());
                    column.setNativeType(mdbColumn.getType().name());

                    table.addColumn(column);
                    i++;
                }

                final Index primaryKeyIndex = mdbTable.getPrimaryKeyIndex();
                final List<ColumnDescriptor> columnDescriptors = primaryKeyIndex.getColumns();
                for (ColumnDescriptor columnDescriptor : columnDescriptors) {
                    final String name = columnDescriptor.getColumn().getName();
                    final MutableColumn column = (MutableColumn) table.getColumnByName(name);
                    column.setPrimaryKey(true);
                }

                schema.addTable(table);

            } catch (Exception e) {
                throw new MetaModelException(e);
            }
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _file.getName();
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        try {
            com.healthmarketscience.jackcess.Table mdbTable = getDatabase().getTable(table.getName());
            return mdbTable.getRowCount();
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    public DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        try {
            final com.healthmarketscience.jackcess.Table mdbTable = getDatabase().getTable(table.getName());
            final SelectItem[] selectItems = new SelectItem[columns.length];
            for (int i = 0; i < columns.length; i++) {
                selectItems[i] = new SelectItem(columns[i]);
            }

            final DataSetHeader header = new CachingDataSetHeader(selectItems);

            int rowNum = 0;
            final List<Row> data = new LinkedList<Row>();
            final Iterator<Map<String, Object>> it = mdbTable.iterator();
            while (it.hasNext() && (maxRows < 0 || rowNum < maxRows)) {
                rowNum++;
                final Map<String, Object> valueMap = it.next();
                final Object[] values = new Object[columns.length];
                for (int j = 0; j < columns.length; j++) {
                    values[j] = valueMap.get(columns[j].getName());
                }
                data.add(new DefaultRow(header, values));
            }

            return new InMemoryDataSet(header, data);
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        _database.close();
    }
}