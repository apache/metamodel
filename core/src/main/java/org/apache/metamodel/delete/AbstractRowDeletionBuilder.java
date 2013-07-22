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
package org.apache.metamodel.delete;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FilterClause;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.builder.AbstractFilterBuilder;
import org.apache.metamodel.query.builder.FilterBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

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
