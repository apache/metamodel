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

import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.SimpleTableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table implementation for HBase
 */
final class HBaseTable extends MutableTable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(HBaseTable.class);

    private final transient HBaseDataContext _dataContext;
    private final transient ColumnType _defaultRowKeyColumnType;

    public HBaseTable(HBaseDataContext dataContext, SimpleTableDef tableDef, MutableSchema schema,
            ColumnType defaultRowKeyColumnType) {
        super(tableDef.getName(), TableType.TABLE, schema);
        _dataContext = dataContext;
        _defaultRowKeyColumnType = defaultRowKeyColumnType;

        final String[] columnNames = tableDef.getColumnNames();
        if (columnNames == null || columnNames.length == 0) {
            logger.info("No user-defined columns specified for table {}. Columns will be auto-detected.");
        } else {

            final ColumnType[] types = tableDef.getColumnTypes();
            int columnNumber = 1;

            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                if (HBaseDataContext.FIELD_ID.equals(columnName)) {
                    final ColumnType type = types[i];
                    final MutableColumn idColumn = new MutableColumn(HBaseDataContext.FIELD_ID, type)
                            .setPrimaryKey(true).setColumnNumber(columnNumber).setTable(this);
                    addColumn(idColumn);
                    columnNumber++;
                }
            }

            if (columnNumber == 1) {
                // insert a default definition of the id column
                final MutableColumn idColumn = new MutableColumn(HBaseDataContext.FIELD_ID, defaultRowKeyColumnType)
                        .setPrimaryKey(true).setColumnNumber(columnNumber).setTable(this);
                addColumn(idColumn);
                columnNumber++;
            }

            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];

                if (!HBaseDataContext.FIELD_ID.equals(columnName)) {
                    final ColumnType type = types[i];
                    final MutableColumn column = new MutableColumn(columnName, type);
                    column.setTable(this);
                    column.setColumnNumber(columnNumber);
                    columnNumber++;
                    addColumn(column);
                }
            }
        }
    }

    @Override
    protected List<Column> getColumnsInternal() {
        final List<Column> columnsInternal = super.getColumnsInternal();
        if (columnsInternal.isEmpty() && _dataContext != null) {
            try {
                final org.apache.hadoop.hbase.client.Table table = _dataContext.getHTable(getName());
                int columnNumber = 1;

                final MutableColumn idColumn = new MutableColumn(HBaseDataContext.FIELD_ID, _defaultRowKeyColumnType)
                        .setPrimaryKey(true).setColumnNumber(columnNumber).setTable(this);
                addColumn(idColumn);
                columnNumber++;

                // What about timestamp?

                final HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
                for (int i = 0; i < columnFamilies.length; i++) {
                    final HColumnDescriptor columnDescriptor = columnFamilies[i];
                    final String columnFamilyName = columnDescriptor.getNameAsString();
                    // HBase column families are always unstructured maps.
                    final ColumnType type = ColumnType.MAP;
                    final MutableColumn column = new MutableColumn(columnFamilyName, type);
                    column.setTable(this);
                    column.setColumnNumber(columnNumber);
                    columnNumber++;
                    addColumn(column);
                }
            } catch (Exception e) {
                throw new MetaModelException("Could not resolve table ", e);
            }
        }
        return columnsInternal;
    }
}
