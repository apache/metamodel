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
package org.eobjects.metamodel.update;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.data.AbstractRowBuilder;
import org.eobjects.metamodel.query.FilterClause;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.query.builder.AbstractFilterBuilder;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Abstract {@link RowUpdationBuilder} implementation.
 */
public abstract class AbstractRowUpdationBuilder extends AbstractRowBuilder<RowUpdationBuilder> implements
        RowUpdationBuilder {

    private final Table _table;
    private final List<FilterItem> _whereItems;

    public AbstractRowUpdationBuilder(Table table) {
        super(table);
        _table = table;
        _whereItems = new ArrayList<FilterItem>();
    }

    protected List<FilterItem> getWhereItems() {
        return _whereItems;
    }

    @Override
    public FilterBuilder<RowUpdationBuilder> where(Column column) {
        SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<RowUpdationBuilder>(selectItem) {
            @Override
            protected RowUpdationBuilder applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<RowUpdationBuilder> where(String columnName) {
        Column column = _table.getColumnByName(columnName);
        if (column == null) {
            throw new IllegalArgumentException("No such column: " + columnName);
        }
        return where(column);
    }

    @Override
    public RowUpdationBuilder where(FilterItem... filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public RowUpdationBuilder where(Iterable<FilterItem> filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public Table getTable() {
        return _table;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(_table.getQualifiedLabel());
        sb.append(" SET ");
        Column[] columns = getColumns();
        Object[] values = getValues();

        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(columns[i].getName());
            sb.append('=');
            sb.append(values[i] == null ? "NULL" : values[i].toString());
        }

        List<FilterItem> whereItems = getWhereItems();
        String whereClause = new FilterClause(null, " WHERE ").addItems(whereItems).toSql();
        sb.append(whereClause);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
