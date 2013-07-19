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
package org.apache.metamodel.update;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.data.AbstractRowBuilder;
import org.apache.metamodel.query.FilterClause;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.builder.AbstractFilterBuilder;
import org.apache.metamodel.query.builder.FilterBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

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
