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
package org.eobjects.metamodel.delete;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.WhereClauseBuilder;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.query.builder.AbstractFilterBuilder;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Represents a single DELETE FROM operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built delete from
 * implementation. Some {@link DataContext}s may even optimize specifically
 * based on the knowledge that there will only be a single delete from statement
 * executed.
 */
public final class DeleteFrom implements UpdateScript, WhereClauseBuilder<DeleteFrom> {

    private final List<FilterItem> _whereItems;
    private final Table _table;

    public DeleteFrom(Table table) {
        _table = table;
        _whereItems = new ArrayList<FilterItem>();
    }

    @Override
    public void run(UpdateCallback callback) {
        callback.deleteFrom(_table).where(_whereItems).execute();
    }

    @Override
    public FilterBuilder<DeleteFrom> where(Column column) {
        SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<DeleteFrom>(selectItem) {
            @Override
            protected DeleteFrom applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<DeleteFrom> where(String columnName) {
        Column column = _table.getColumnByName(columnName);
        if (column == null) {
            throw new IllegalArgumentException("No such column: " + columnName);
        }
        return where(column);
    }

    @Override
    public DeleteFrom where(FilterItem... filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public DeleteFrom where(Iterable<FilterItem> filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }
}
