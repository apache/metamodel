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
package org.eobjects.metamodel.insert;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.data.AbstractRowBuilder;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Abstract implementation of the {@link RowInsertionBuilder} interface,
 * provided as a convenience to {@link RowInsertable} implementations. Handles
 * all the building operations, but not the commit operation.
 * 
 * @author Kasper Sørensen
 */
public abstract class AbstractRowInsertionBuilder<U extends UpdateCallback> extends
        AbstractRowBuilder<RowInsertionBuilder> implements RowInsertionBuilder {

    private final U _updateCallback;
    private final Table _table;

    public AbstractRowInsertionBuilder(U updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
        _table = table;
    }

    @Override
    public Table getTable() {
        return _table;
    }

    protected U getUpdateCallback() {
        return _updateCallback;
    }

    @Override
    public RowInsertionBuilder like(Row row) {
        SelectItem[] selectItems = row.getSelectItems();
        for (int i = 0; i < selectItems.length; i++) {
            SelectItem selectItem = selectItems[i];
            Column column = selectItem.getColumn();
            if (column != null) {
                if (_table == column.getTable()) {
                    value(column, row.getValue(i));
                } else {
                    value(column.getName(), row.getValue(i));
                }
            }
        }
        return this;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(_table.getQualifiedLabel());
        sb.append("(");
        Column[] columns = getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(columns[i].getName());
        }
        sb.append(") VALUES (");
        Object[] values = getValues();
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            final String stringValue;
            if (value == null) {
                stringValue = "NULL";
            } else if (value instanceof String) {
                stringValue = "\"" + value + "\"";
            } else {
                stringValue = value.toString();
            }
            sb.append(stringValue);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
