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
package org.apache.metamodel.util;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;

/**
 * Represents a table definition to be used in scenarios where a
 * {@link DataContext} is unable to detect/discover the table structure and
 * needs some basic input around expected table structures.
 */
public class SimpleTableDef implements Serializable, HasName {

    private static final long serialVersionUID = 1L;

    private final String _name;
    private final String[] _columnNames;
    private final ColumnType[] _columnTypes;

    /**
     * Constructs a {@link SimpleTableDef} using a {@link Table} as a prototype.
     * 
     * @param table
     */
    public SimpleTableDef(Table table) {
        _name = table.getName();
        _columnNames = new String[table.getColumnCount()];
        _columnTypes = new ColumnType[table.getColumnCount()];
        for (int i = 0; i < table.getColumnCount(); i++) {
            Column column = table.getColumn(i);
            _columnNames[i] = column.getName();
            _columnTypes[i] = column.getType();
        }
    }

    /**
     * Constructs a {@link SimpleTableDef}.
     * 
     * @param name
     *            the name of the table
     * @param columnNames
     *            the names of the columns to include in the table
     */
    public SimpleTableDef(String name, String[] columnNames) {
        this(name, columnNames, null);
    }

    /**
     * Constructs a {@link SimpleTableDef}.
     * 
     * @param name
     *            the name of table
     * @param columnNames
     *            the names of the columns to include in the table
     * @param columnTypes
     *            the column types of the columns specified.
     */
    public SimpleTableDef(String name, String[] columnNames, ColumnType[] columnTypes) {
        if(name == null){
            throw new NullPointerException("Table name cannot be null");
        }
        _name = name;
        _columnNames = columnNames;
        if (columnTypes == null) {
            columnTypes = new ColumnType[columnNames.length];
            for (int i = 0; i < columnTypes.length; i++) {
                columnTypes[i] = ColumnType.VARCHAR;
            }
        } else {
            if (columnNames.length != columnTypes.length) {
                throw new IllegalArgumentException(
                        "Property names and column types cannot have different lengths (found " + columnNames.length
                                + " and " + columnTypes.length + ")");
            }
        }
        _columnTypes = columnTypes;
    }

    /**
     * Gets the name of the table
     * 
     * @return the name of the table
     */
    public String getName() {
        return _name;
    }

    /**
     * Gets the names of the columns in the table
     * 
     * @return the names of the columns in the table
     */
    public String[] getColumnNames() {
        return _columnNames;
    }

    /**
     * Gets the types of the columns in the table
     * 
     * @return the types of the columns in the table
     */
    public ColumnType[] getColumnTypes() {
        return _columnTypes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_name == null) ? 0 : _name.hashCode());
        result = prime * result + Arrays.hashCode(_columnTypes);
        result = prime * result + Arrays.hashCode(_columnNames);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleTableDef other = (SimpleTableDef) obj;
        if (_name == null) {
            if (other._name != null)
                return false;
        } else if (!_name.equals(other._name))
            return false;
        if (!Arrays.equals(_columnTypes, other._columnTypes))
            return false;
        if (!Arrays.equals(_columnNames, other._columnNames))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SimpleTableDef[name=" + _name + ",columnNames=" + Arrays.toString(_columnNames) + ",columnTypes="
                + Arrays.toString(_columnTypes) + "]";
    }

    /**
     * Creates a {@link MutableTable} based on this {@link SimpleTableDef}. Note
     * that the created table will not have any schema set.
     * 
     * @return a table representation of this table definition.
     */
    public MutableTable toTable() {
        String name = getName();
        String[] columnNames = getColumnNames();
        ColumnType[] columnTypes = getColumnTypes();

        MutableTable table = new MutableTable(name, TableType.TABLE);

        for (int i = 0; i < columnNames.length; i++) {
            table.addColumn(new MutableColumn(columnNames[i], columnTypes[i], table, i, true));
        }
        return table;
    }

    /**
     * Gets the index of a column name, or -1 if the column name does not exist
     * 
     * @param columnName
     * @return
     */
    public int indexOf(String columnName) {
        if (columnName == null) {
            throw new IllegalArgumentException("Column name cannot be null");
        }
        for (int i = 0; i < _columnNames.length; i++) {
            if (columnName.equals(_columnNames[i])) {
                return i;
            }
        }
        return -1;
    }
}
