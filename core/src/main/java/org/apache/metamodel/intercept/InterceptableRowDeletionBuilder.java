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
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.builder.AbstractFilterBuilder;
import org.apache.metamodel.query.builder.FilterBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

final class InterceptableRowDeletionBuilder implements RowDeletionBuilder {

    private final RowDeletionBuilder _rowDeletionBuilder;
    private final InterceptorList<RowDeletionBuilder> _rowDeletionInterceptors;

    public InterceptableRowDeletionBuilder(RowDeletionBuilder rowDeletionBuilder,
            InterceptorList<RowDeletionBuilder> rowDeletionInterceptors) {
        _rowDeletionBuilder = rowDeletionBuilder;
        _rowDeletionInterceptors = rowDeletionInterceptors;
    }

    @Override
    public FilterBuilder<RowDeletionBuilder> where(Column column) {
        final SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<RowDeletionBuilder>(selectItem) {
            @Override
            protected RowDeletionBuilder applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<RowDeletionBuilder> where(String columnName) {
        Column column = getTable().getColumnByName(columnName);
        return where(column);
    }

    @Override
    public RowDeletionBuilder where(FilterItem... filterItems) {
        _rowDeletionBuilder.where(filterItems);
        return this;
    }

    @Override
    public RowDeletionBuilder where(Iterable<FilterItem> filterItems) {
        _rowDeletionBuilder.where(filterItems);
        return this;
    }

    @Override
    public Table getTable() {
        return _rowDeletionBuilder.getTable();
    }

    @Override
    public String toSql() {
        return _rowDeletionBuilder.toSql();
    }

    @Override
    public void execute() throws MetaModelException {
        RowDeletionBuilder rowDeletionBuilder = _rowDeletionBuilder;
        rowDeletionBuilder = _rowDeletionInterceptors.interceptAll(rowDeletionBuilder);
        rowDeletionBuilder.execute();
    }

}
