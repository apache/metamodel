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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.SuperColumnType;
import org.apache.metamodel.schema.Table;

public final class HBaseColumn extends MutableColumn {
    public final static ColumnType DEFAULT_COLUMN_TYPE_FOR_ID_COLUMN = new ColumnTypeImpl("BYTE[]",
            SuperColumnType.LITERAL_TYPE);
    public final static ColumnType DEFAULT_COLUMN_TYPE_FOR_COLUMN_FAMILIES = ColumnType.LIST;

    private final String columnFamily;
    private final String qualifier;

    public HBaseColumn(final String columnFamily, final Table table) {
        this(columnFamily, null, table, -1);
    }

    public HBaseColumn(final String columnFamily, final String qualifier, final Table table) {
        this(columnFamily, qualifier, table, -1);
    }

    public HBaseColumn(final String columnFamily, final Table table, final int columnNumber) {
        this(columnFamily, null, table, columnNumber);
    }

    public HBaseColumn(final String columnFamily, final String qualifier, final Table table, final int columnNumber) {
        this(columnFamily, qualifier, table, columnNumber, null);
    }

    public HBaseColumn(final String columnFamily, final String qualifier, final Table table, final int columnNumber,
            final ColumnType columnType) {
        super(columnFamily, table);
        if (columnFamily == null) {
            throw new IllegalArgumentException("Column family isn't allowed to be null.");
        } else if (table == null || !(table instanceof HBaseTable)) {
            throw new IllegalArgumentException("Table is null or isn't a HBaseTable.");
        }

        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        setColumnNumber(columnNumber);
        setPrimaryKey(HBaseDataContext.FIELD_ID.equals(columnFamily));

        // Set the columnType
        if (columnType != null) {
            setType(columnType);
        } else {
            if (isPrimaryKey() || qualifier != null) {
                setType(DEFAULT_COLUMN_TYPE_FOR_ID_COLUMN);
            } else {
                setType(DEFAULT_COLUMN_TYPE_FOR_COLUMN_FAMILIES);
            }
        }
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getQualifier() {
        return qualifier;
    }

    @Override
    public String getName() {
        if (qualifier == null) {
            return columnFamily;
        }
        return columnFamily + ":" + qualifier;
    }

    @Override
    public Boolean isNullable() {
        return !isPrimaryKey();
    }

    @Override
    public String getRemarks() {
        return null;
    }

    @Override
    public Integer getColumnSize() {
        return null;
    }

    @Override
    public String getNativeType() {
        // TODO: maybe change if no qualifier is present (and not identifier column).
        return "byte[]";
    }

    @Override
    public boolean isIndexed() {
        return false;
    }

    @Override
    public String getQuote() {
        return null;
    }

    /**
     * Creates a set of columnFamilies out of a list of hbaseColumns
     * @param columns
     * @return {@link LinkedHashSet}
     */
    public static Set<String> getColumnFamilies(List<HBaseColumn> columns) {
        final LinkedHashSet<String> columnFamilies = new LinkedHashSet<>();
        for (HBaseColumn column : columns) {
            columnFamilies.add(column.getColumnFamily());
        }
        return columnFamilies;
    }

    /**
     * Returns the index of the ID-column (see {@link HBaseDataContext#FIELD_ID}) in an array of HBaseColumns.
     * When no ID-column is found, then null is returned.
     * @param columns
     * @return {@link Integer}
     */
    public static Integer findIndexOfIdColumn(List<HBaseColumn> columns) {
        int i = 0;
        Integer indexOfIDColumn = null;
        Iterator<HBaseColumn> iterator = columns.iterator();
        while (indexOfIDColumn == null && iterator.hasNext()) {
            indexOfIDColumn = findIndexOfIdColumn(iterator.next().getColumnFamily(), i);
            if (indexOfIDColumn == null) {
                i++;
            }
        }
        return indexOfIDColumn;
    }

    /**
     * Returns the index of the ID-column (see {@link HBaseDataContext#FIELD_ID}) in an array of columnNames.
     * When no ID-column is found, then null is returned.
     * @param columnNames
     * @return {@link Integer}
     */
    public static Integer findIndexOfIdColumn(String[] columnNames) {
        int i = 0;
        Integer indexOfIDColumn = null;
        while (indexOfIDColumn == null && i < columnNames.length) {
            indexOfIDColumn = findIndexOfIdColumn(columnNames[i], i);
            if (indexOfIDColumn == null) {
                i++;
            }
        }
        return indexOfIDColumn;
    }

    /**
     * Returns the index of the ID-column (see {@link HBaseDataContext#FIELD_ID})
     * When no ID-column is found, then null is returned.
     * @param columnNames
     * @return {@link Integer}
     */
    private static Integer findIndexOfIdColumn(String columnName, int index) {
        Integer indexOfIDColumn = null;
        if (columnName.equals(HBaseDataContext.FIELD_ID)) {
            indexOfIDColumn = new Integer(index);
        }
        return indexOfIDColumn;
    }

    /**
     * Converts a list of {@link Column}'s to a list of {@link HBaseColumn}'s
     * @param columns
     * @return {@link List}<{@link HBaseColumn}>
     */
    public static List<HBaseColumn> convertToHBaseColumnsList(List<Column> columns) {
        return columns.stream().map(column -> (HBaseColumn) column).collect(Collectors.toList());
    }

    /**
     * Converts a list of {@link HBaseColumn}'s to a list of {@link Column}'s
     * @param columns
     * @return {@link List}<{@link Column}>
     */
    public static List<Column> convertToColumnsList(List<HBaseColumn> columns) {
        return columns.stream().map(column -> (Column) column).collect(Collectors.toList());
    }

    /**
     * Converts a list of {@link HBaseColumn}'s to an array of {@link HBaseColumn}'s
     * @param columns
     * @return Array of {@link HBaseColumn}
     */
    public static HBaseColumn[] convertToHBaseColumnsArray(List<HBaseColumn> columns) {
        return columns.stream().map(column -> column).toArray(size -> new HBaseColumn[size]);
    }

    /**
     * Converts a array of {@link Column}'s to an array of {@link HBaseColumn}'s
     * @param columns
     * @return Array of {@link HBaseColumn}
     */
    public static HBaseColumn[] convertToHBaseColumnsArray(Column[] columns) {
        return Arrays.stream(columns).map(column -> (HBaseColumn) column).toArray(size -> new HBaseColumn[size]);
    }
}
