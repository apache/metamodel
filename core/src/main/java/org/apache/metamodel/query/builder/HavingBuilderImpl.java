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
package org.apache.metamodel.query.builder;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

final class HavingBuilderImpl extends AbstractQueryFilterBuilder<SatisfiedHavingBuilder> implements
        HavingBuilder,
        SatisfiedHavingBuilder {

    private final Query _query;
    private final List<FilterItem> _orFilters;
    private FilterItem _parentOrFilter;

    public HavingBuilderImpl(SelectItem selectItem, Query query, GroupedQueryBuilder queryBuilder) {
        super(selectItem, queryBuilder);
        _query = query;
        _orFilters = new ArrayList<FilterItem>();
    }

    public HavingBuilderImpl(FunctionType function, Column column, Query query, GroupedQueryBuilder queryBuilder) {
        this(new SelectItem(function, column), query, queryBuilder);
    }

    public HavingBuilderImpl(FunctionType function, Column column, Query query, FilterItem parentOrFilter,
            List<FilterItem> orFilters, GroupedQueryBuilder queryBuilder) {
        this(function, column, query, queryBuilder);
    }

    @Override
    protected SatisfiedHavingBuilder applyFilter(FilterItem filter) {
        if (_parentOrFilter == null) {
            _query.having(filter);
        } else {
            if (_parentOrFilter.getChildItemCount() == 1) {
                _query.getHavingClause().removeItem(_orFilters.get(0));
                _query.getHavingClause().addItem(_parentOrFilter);
            }
        }
        _orFilters.add(filter);
        return this;
    }

    @Override
    public HavingBuilder or(FunctionType function, Column column) {
        if (function == null) {
            throw new IllegalArgumentException("function cannot be null");
        }
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        if (_parentOrFilter == null) {
            _parentOrFilter = new FilterItem(_orFilters);
        }
        return new HavingBuilderImpl(function, column, _query, _parentOrFilter, _orFilters, getQueryBuilder());
    }

    @Override
    public HavingBuilder and(FunctionType function, Column column) {
        return getQueryBuilder().having(function, column);
    }
}