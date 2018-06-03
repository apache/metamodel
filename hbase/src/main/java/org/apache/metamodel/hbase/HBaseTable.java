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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
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

    /**
     * Creates an HBaseTable. If the tableDef variable doesn't include the ID-column (see {@link HBaseDataContext#FIELD_ID}).
     * Then it's first added.
     * @param dataContext
     * @param tableDef Table definition. The tableName, columnNames and columnTypes variables are used.
     * @param schema {@link MutableSchema} where the table belongs to.
     * @param defaultRowKeyColumnType This variable determines the {@link ColumnType},
     * used when the tableDef doesn't include the ID column (see {@link HBaseDataContext#FIELD_ID}).
     */
    public HBaseTable(final HBaseDataContext dataContext, final SimpleTableDef tableDef, final MutableSchema schema,
            final ColumnType defaultRowKeyColumnType) {
        super(tableDef.getName(), TableType.TABLE, schema);
        _dataContext = dataContext;
        _defaultRowKeyColumnType = defaultRowKeyColumnType;
        addColumns(tableDef);
    }

    /**
     * Add multiple columns to this table
     * @param tableDef
     */
    private void addColumns(final SimpleTableDef tableDef) {
        // Add the columns
        final String[] columnNames = tableDef.getColumnNames();
        if (columnNames == null || columnNames.length == 0) {
            logger.info("No user-defined columns specified for table {}. Columns will be auto-detected.");
        } else {
            final ColumnType[] columnTypes = tableDef.getColumnTypes();

            // Find the ID-Column
            int indexOfIDColumn = getIndexOfIdColumn(columnNames);
            boolean idColumnFound = indexOfIDColumn != -1;

            // ColumnNumbers start from 1
            if (idColumnFound) {
                addColumn(HBaseDataContext.FIELD_ID, columnTypes[indexOfIDColumn], indexOfIDColumn + 1);
            } else {
                addColumn(HBaseDataContext.FIELD_ID, _defaultRowKeyColumnType, 1);
            }

            // Add the other columns
            for (int i = 0; i < columnNames.length; i++) {
                if (!HBaseDataContext.FIELD_ID.equals(columnNames[i])) {
                    if (idColumnFound) {
                        addColumn(columnNames[i], columnTypes[i], i + 1);
                    } else {
                        addColumn(columnNames[i], columnTypes[i], i + 2);
                    }
                }
            }
        }
    }

    /**
     * Returns the index of the ID-column (see {@link HBaseDataContext#FIELD_ID}) in an array of columnNames. When no
     * ID-column is found, then -1 is returned.
     *
     * @param columnNames
     * @return {@link Integer}
     */
    private static int getIndexOfIdColumn(final String[] columnNames) {
        for (int i = 0; i < columnNames.length; i++) {
            if (HBaseDataContext.FIELD_ID.equals(columnNames[i])) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Add a column to this table
     *
     * @param columnName
     * @param columnType
     * @param columnNumber
     */
    private void addColumn(final String columnName, final ColumnType columnType, final int columnNumber) {
        addColumn(new HBaseColumn(columnName, null, this, columnNumber, columnType));
    }

    @Override
    protected synchronized List<Column> getColumnsInternal() {
        final List<Column> columnsInternal = super.getColumnsInternal();
        if (columnsInternal.isEmpty() && _dataContext != null) {
            try (final org.apache.hadoop.hbase.client.Table table = _dataContext.getHTable(getName())) {
                // Add the ID-Column (with columnNumber = 1)
                addColumn(HBaseDataContext.FIELD_ID, _defaultRowKeyColumnType, 1);

                // What about timestamp?

                // Add the other column (with columnNumbers starting from 2)
                final HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
                for (int i = 0; i < columnFamilies.length; i++) {
                    addColumn(columnFamilies[i].getNameAsString(), HBaseColumn.DEFAULT_COLUMN_TYPE_FOR_COLUMN_FAMILIES,
                            i + 2);
                }
            } catch (Exception e) {
                throw new MetaModelException("Could not resolve table ", e);
            }
        }
        return columnsInternal;
    }

    /**
     * Returns the column families for this HBase table.
     *
     * @return {@link Set}<{@link String}> of columnFamilies
     */
    Set<String> getColumnFamilies() {
        return getColumnsInternal()
                .stream()
                .map(column -> (HBaseColumn) column)
                .map(HBaseColumn::getColumnFamily)
                .distinct()
                .collect(Collectors.toSet());
    }
}
