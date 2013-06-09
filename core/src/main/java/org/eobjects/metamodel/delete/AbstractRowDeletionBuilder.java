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

import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.FilterClause;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.query.builder.AbstractFilterBuilder;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Abstract {@link RowDeletionBuilder} implementation
 */
public abstract class AbstractRowDeletionBuilder implements RowDeletionBuilder {

    private final Table _table;
    private final List<FilterItem> _whereItems;

    public AbstractRowDeletionBuilder(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }
        _table = table;
        _whereItems = new ArrayList<FilterItem>();
    }

    protected List<FilterItem> getWhereItems() {
        return _whereItems;
    }

    @Override
    public FilterBuilder<RowDeletionBuilder> where(Column column) {
        SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<RowDeletionBuilder>(selectItem) {
            @Override
            protected RowDeletionBuilder applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<RowDeletionBuilder> where(String columnName) {
        Column column = _table.getColumnByName(columnName);
        if (column == null) {
            throw new IllegalArgumentException("No such column: " + columnName);
        }
        return where(column);
    }

    @Override
    public RowDeletionBuilder where(FilterItem... filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public RowDeletionBuilder where(Iterable<FilterItem> filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public Table getTable() {
        return _table;
    }

    /**
     * Determines if a row should be deleted or not (can be used by subclasses
     * as a convenient determinator).
     * 
     * @param row
     * @return true if the row should be deleted.
     */
    protected boolean deleteRow(Row row) {
        final List<FilterItem> whereItems = getWhereItems();
        for (FilterItem filterItem : whereItems) {
            if (!filterItem.evaluate(row)) {
                // since filter items are ANDed, if any item does not evaluate
                // to true, the row is not deleted
                return false;
            }
        }
        return true;
    }

    /**
     * Convenience method to tell subclasses if the delete operation represents
     * a full table truncate operation. Usually such operations can be optimized
     * by simply removing the table (and maybe restoring similar headers in a
     * new table).
     * 
     * @return
     */
    protected boolean isTruncateTableOperation() {
        final List<FilterItem> whereItems = getWhereItems();
        return whereItems.isEmpty();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        return "DELETE FROM " + _table.getQualifiedLabel() + new FilterClause(null, " WHERE ").addItems(_whereItems);
    }
}
