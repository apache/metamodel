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

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.metamodel.schema.AbstractColumn;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.SuperColumnType;
import org.apache.metamodel.schema.Table;

public final class HBaseColumn extends AbstractColumn {
    private final String columnFamily;
    private final String qualifier;
    private final Table table;
    private final boolean primaryKey;
    private final ColumnType columnType;
    private final int columnNumber;

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
        if (columnFamily == null) {
            throw new IllegalArgumentException("Column family isn't allowed to be null.");
        } else if (table == null) {
            throw new IllegalArgumentException("Table isn't allowed to be null.");
        }

        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        this.table = table;
        this.columnNumber = columnNumber;

        primaryKey = HBaseDataContext.FIELD_ID.equals(columnFamily);

        if (primaryKey || qualifier != null) {
            columnType = new ColumnTypeImpl("BYTE[]", SuperColumnType.LITERAL_TYPE);
        } else {
            columnType = ColumnType.LIST;
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
    public int getColumnNumber() {
        return columnNumber;
    }

    @Override
    public ColumnType getType() {
        return columnType;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public Boolean isNullable() {
        return !primaryKey;
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
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String getQuote() {
        return null;
    }

    /**
     * Creates a set of columnFamilies out of an array of hbaseColumns
     * @param hbaseColumns
     * @return {@link LinkedHashSet}
     */
    public static Set<String> getColumnFamilies(HBaseColumn[] hbaseColumns) {
        final LinkedHashSet<String> columnFamilies = new LinkedHashSet<String>();
        for (int i = 0; i < hbaseColumns.length; i++) {
            columnFamilies.add(hbaseColumns[i].getColumnFamily());
        }
        return columnFamilies;
    }
}
