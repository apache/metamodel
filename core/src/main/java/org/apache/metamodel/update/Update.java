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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.AbstractRowBuilder;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.data.WhereClauseBuilder;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.builder.AbstractFilterBuilder;
import org.apache.metamodel.query.builder.FilterBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

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

    public Update(Schema schema, String tableName) {
        this(schema.getTableByName(tableName));
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
