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

package org.eobjects.metamodel.schema;

import java.io.Serializable;

/**
 * Represents a column and it's metadata description. Columns reside within a
 * Table and can be used as keys for relationships between tables.
 * 
 * @see MutableTable
 * @see Relationship
 */
public class MutableColumn extends AbstractColumn implements Serializable {

    private static final long serialVersionUID = -353183696233890927L;
    private int _columnNumber;
    private String _name;
    private ColumnType _type;
    private Table _table;
    private Boolean _nullable = null;
    private String _remarks;
    private boolean _indexed = false;
    private boolean _primaryKey = false;
    private Integer _columnSize = null;
    private String _nativeType = null;
    private String _quoteString = null;

    public MutableColumn() {
        super();
    }

    public MutableColumn(String name) {
        this();
        setName(name);
    }

    public MutableColumn(String name, ColumnType type) {
        this(name);
        setType(type);
    }

    public MutableColumn(String name, ColumnType type, Table table, int columnNumber, Boolean nullable) {
        this(name, type);
        setColumnNumber(columnNumber);
        setTable(table);
        setNullable(nullable);
    }

    public MutableColumn(String name, ColumnType type, Table table, int columnNumber, Integer columnSize,
            String nativeType, Boolean nullable, String remarks, boolean indexed, String quote) {
        this(name, type, table, columnNumber, nullable);
        setColumnSize(columnSize);
        setNativeType(nativeType);
        setRemarks(remarks);
        setIndexed(indexed);
        setQuote(quote);
    }

    @Override
    public int getColumnNumber() {
        return _columnNumber;
    }

    public MutableColumn setColumnNumber(int columnNumber) {
        _columnNumber = columnNumber;
        return this;
    }

    @Override
    public String getName() {
        return _name;
    }

    public MutableColumn setName(String name) {
        _name = name;
        return this;
    }

    @Override
    public ColumnType getType() {
        return _type;
    }

    public MutableColumn setType(ColumnType type) {
        _type = type;
        return this;
    }

    @Override
    public Table getTable() {
        return _table;
    }

    public MutableColumn setTable(Table table) {
        _table = table;
        return this;
    }

    @Override
    public Boolean isNullable() {
        return _nullable;
    }

    public MutableColumn setNullable(Boolean nullable) {
        _nullable = nullable;
        return this;
    }

    @Override
    public String getRemarks() {
        return _remarks;
    }

    public MutableColumn setRemarks(String remarks) {
        _remarks = remarks;
        return this;
    }

    @Override
    public Integer getColumnSize() {
        return _columnSize;
    }

    public MutableColumn setColumnSize(Integer columnSize) {
        _columnSize = columnSize;
        return this;
    }

    @Override
    public String getNativeType() {
        return _nativeType;
    }

    public MutableColumn setNativeType(String nativeType) {
        _nativeType = nativeType;
        return this;
    }

    @Override
    public boolean isIndexed() {
        return _indexed;
    }

    public MutableColumn setIndexed(boolean indexed) {
        _indexed = indexed;
        return this;
    }

    @Override
    public String getQuote() {
        return _quoteString;
    }

    public MutableColumn setQuote(String quoteString) {
        _quoteString = quoteString;
        return this;
    }

    @Override
    public boolean isPrimaryKey() {
        return _primaryKey;
    }

    public MutableColumn setPrimaryKey(boolean primaryKey) {
        _primaryKey = primaryKey;
        return this;
    }
}