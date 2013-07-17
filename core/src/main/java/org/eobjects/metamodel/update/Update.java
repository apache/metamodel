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

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.AbstractRowBuilder;
import org.eobjects.metamodel.data.Style;
import org.eobjects.metamodel.data.WhereClauseBuilder;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.query.builder.AbstractFilterBuilder;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Represents a single UPDATE operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built update
 * implementation. Some {@link DataContext}s may even optimize specifically
 * based on the knowledge that there will only be a single update statement
 * executed.
 */
public final class Update extends AbstractRowBuilder<Update> implements UpdateScript, WhereClauseBuilder<Update> {

    private final Table _table;
    private final List<FilterItem> _whereItems;

    public Update(Table table) {
        super(table);
        _table = table;
        _whereItems = new ArrayList<FilterItem>();
    }

    @Override
    public Table getTable() {
        return _table;
    }

    @Override
    public void run(UpdateCallback callback) {
        RowUpdationBuilder updateBuilder = callback.update(_table).where(_whereItems);

        final Column[] columns = getColumns();
        final Object[] values = getValues();
        final Style[] styles = getStyles();
        final boolean[] explicitNulls = getExplicitNulls();

        for (int i = 0; i < columns.length; i++) {
            Object value = values[i];
            Column column = columns[i];
            Style style = styles[i];
            if (value == null) {
                if (explicitNulls[i]) {
                    updateBuilder = updateBuilder.value(column, value, style);
                }
            } else {
                updateBuilder = updateBuilder.value(column, value, style);
            }
        }

        updateBuilder.execute();
    }

    @Override
    public FilterBuilder<Update> where(Column column) {
        SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<Update>(selectItem) {
            @Override
            protected Update applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<Update> where(String columnName) {
        Column column = _table.getColumnByName(columnName);
        if (column == null) {
            throw new IllegalArgumentException("No such column: " + columnName);
        }
        return where(column);
    }

    @Override
    public Update where(FilterItem... filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public Update where(Iterable<FilterItem> filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }
}
