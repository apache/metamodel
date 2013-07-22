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
package org.apache.metamodel.intercept;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.builder.AbstractFilterBuilder;
import org.apache.metamodel.query.builder.FilterBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;

final class InterceptableRowUpdationBuilder implements RowUpdationBuilder {

    private final RowUpdationBuilder _rowUpdationBuilder;
    private final InterceptorList<RowUpdationBuilder> _rowUpdationInterceptors;

    public InterceptableRowUpdationBuilder(RowUpdationBuilder rowUpdationBuilder,
            InterceptorList<RowUpdationBuilder> rowUpdationInterceptors) {
        _rowUpdationBuilder = rowUpdationBuilder;
        _rowUpdationInterceptors = rowUpdationInterceptors;
    }

    @Override
    public RowUpdationBuilder value(int columnIndex, Object value) {
        _rowUpdationBuilder.value(columnIndex, value);
        return this;
    }

    @Override
    public RowUpdationBuilder value(int columnIndex, Object value, Style style) {
        _rowUpdationBuilder.value(columnIndex, value, style);
        return this;
    }

    @Override
    public RowUpdationBuilder value(Column column, Object value) {
        _rowUpdationBuilder.value(column, value);
        return this;
    }

    @Override
    public RowUpdationBuilder value(Column column, Object value, Style style) {
        _rowUpdationBuilder.value(column, value, style);
        return this;
    }

    @Override
    public RowUpdationBuilder value(String columnName, Object value) {
        _rowUpdationBuilder.value(columnName, value);
        return this;
    }

    @Override
    public RowUpdationBuilder value(String columnName, Object value, Style style) {
        _rowUpdationBuilder.value(columnName, value, style);
        return this;
    }

    @Override
    public Row toRow() {
        return _rowUpdationBuilder.toRow();
    }

    @Override
    public boolean isSet(Column column) {
        return _rowUpdationBuilder.isSet(column);
    }

    @Override
    public FilterBuilder<RowUpdationBuilder> where(Column column) {
        final SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<RowUpdationBuilder>(selectItem) {
            @Override
            protected RowUpdationBuilder applyFilter(FilterItem filter) {
                where(filter);
                return InterceptableRowUpdationBuilder.this;
            }
        };
    }

    @Override
    public FilterBuilder<RowUpdationBuilder> where(String columnName) {
        Column column = getTable().getColumnByName(columnName);
        return where(column);
    }

    @Override
    public RowUpdationBuilder where(FilterItem... filterItems) {
        _rowUpdationBuilder.where(filterItems);
        return this;
    }

    @Override
    public RowUpdationBuilder where(Iterable<FilterItem> filterItems) {
        _rowUpdationBuilder.where(filterItems);
        return this;
    }

    @Override
    public String toSql() {
        return _rowUpdationBuilder.toSql();
    }

    @Override
    public Table getTable() {
        return _rowUpdationBuilder.getTable();
    }

    @Override
    public void execute() throws MetaModelException {
        RowUpdationBuilder rowUpdationBuilder = _rowUpdationBuilder;
        rowUpdationBuilder = _rowUpdationInterceptors.interceptAll(rowUpdationBuilder);
        rowUpdationBuilder.execute();
    }

}
