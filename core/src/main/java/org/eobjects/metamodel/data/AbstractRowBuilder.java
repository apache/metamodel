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
package org.eobjects.metamodel.data;

import java.util.Arrays;

import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Abstract {@link RowBuilder} implementation.
 */
public abstract class AbstractRowBuilder<RB extends RowBuilder<?>> implements RowBuilder<RB> {

    private final Column[] _columns;
    private final Object[] _values;
    private final Style[] _styles;
    private final boolean[] _explicitNulls;

    public AbstractRowBuilder(Table table) {
        this(table.getColumns());
    }

    public AbstractRowBuilder(Column[] columns) {
        _columns = columns;
        _explicitNulls = new boolean[_columns.length];
        _values = new Object[_columns.length];
        _styles = new Style[_columns.length];
    }

    /**
     * Gets a boolean array indicating if any of the values have been explicitly
     * set to null (as opposed to just not set)
     * 
     * @return
     */
    protected boolean[] getExplicitNulls() {
        return _explicitNulls;
    }

    protected Object[] getValues() {
        return _values;
    }

    protected Column[] getColumns() {
        return _columns;
    }

    protected Style[] getStyles() {
        return _styles;
    }

    @Override
    public final Row toRow() {
        return new DefaultRow(new SimpleDataSetHeader(_columns), _values);
    }

    @Override
    public final RB value(Column column, Object value) {
        return value(column, value, null);
    }

    @Override
    public RB value(Column column, Object value, Style style) {
        if (column == null) {
            throw new IllegalArgumentException("Column cannot be null");
        }
        boolean written = false;
        for (int i = 0; i < _columns.length; i++) {
            if (_columns[i].equals(column)) {
                value(i, value, style);
                written = true;
                break;
            }
        }
        if (!written) {
            throw new IllegalArgumentException("No such column in table: " + column);
        }

        @SuppressWarnings("unchecked")
        RB result = (RB) this;
        return result;
    }

    @Override
    public RB value(int columnIndex, Object value) {
        return value(columnIndex, value, null);
    }

    @Override
    public final RB value(int columnIndex, Object value, Style style) {
        _values[columnIndex] = value;
        _styles[columnIndex] = style;
        _explicitNulls[columnIndex] = (value == null);

        @SuppressWarnings("unchecked")
        RB result = (RB) this;
        return result;
    }

    @Override
    public RB value(String columnName, Object value) {
        return value(columnName, value, null);
    }

    @Override
    public final RB value(String columnName, Object value, Style style) {
        if (columnName == null) {
            throw new IllegalArgumentException("Column name cannot be null");
        }
        for (int i = 0; i < _columns.length; i++) {
            Column column = _columns[i];
            if (column.getName().equalsIgnoreCase(columnName)) {
                return value(i, value, style);
            }
        }
        throw new IllegalArgumentException("No such column in table: " + columnName + ", available columns are: "
                + Arrays.toString(_columns));
    }

    @Override
    public boolean isSet(Column column) {
        for (int i = 0; i < _columns.length; i++) {
            if (_columns[i].equals(column)) {
                return _values[i] != null || _explicitNulls[i];
            }
        }
        return false;
    }
}
