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
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.QueryParameter;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

final class WhereBuilderImpl extends AbstractQueryFilterBuilder<SatisfiedWhereBuilder<GroupedQueryBuilder>> implements
        WhereBuilder<GroupedQueryBuilder>, SatisfiedWhereBuilder<GroupedQueryBuilder> {

    private final Query _query;
    private final List<FilterItem> _orFilters;
    private FilterItem _parentOrFilter;

    public WhereBuilderImpl(Column column, Query query, GroupedQueryBuilder queryBuilder) {
        this(new SelectItem(column), query, queryBuilder);
    }
    
    public WhereBuilderImpl(SelectItem selectItem, Query query, GroupedQueryBuilder queryBuilder) {
        super(selectItem, queryBuilder);
        _query = query;
        _orFilters = new ArrayList<FilterItem>();
    }

    public WhereBuilderImpl(Column column, Query query, FilterItem parentOrFilter, List<FilterItem> orFilters,
            GroupedQueryBuilder queryBuilder) {
        super(new SelectItem(column), queryBuilder);
        _query = query;
        _parentOrFilter = parentOrFilter;
        _orFilters = orFilters;
    }

    @Override
    protected SatisfiedWhereBuilder<GroupedQueryBuilder> applyFilter(FilterItem filter) {
        if (_parentOrFilter == null) {
            _query.where(filter);
        } else {
            if (_parentOrFilter.getChildItemCount() == 1) {
                _query.getWhereClause().removeItem(_orFilters.get(0));
                _query.getWhereClause().addItem(_parentOrFilter);
            }
        }
        _orFilters.add(filter);
        return this;
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> or(String columnName) {
        Column column = findColumn(columnName);
        return or(column);
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> or(Column column) {
        if (_parentOrFilter == null) {
            _parentOrFilter = new FilterItem(_orFilters);
        }
        return new WhereBuilderImpl(column, _query, _parentOrFilter, _orFilters, getQueryBuilder());
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> and(String columnName) {
        Column column = findColumn(columnName);
        return and(column);
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> and(Column column) {
        return getQueryBuilder().where(column);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> eq(QueryParameter queryParameter) {
        return isEquals(queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> isEquals(QueryParameter queryParameter) {
        if (queryParameter == null) {
            throw new IllegalArgumentException("query parameter cannot be null");
        }
        return _filterBuilder.applyFilter(OperatorType.EQUALS_TO, queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> differentFrom(QueryParameter queryParameter) {
        return ne(queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> ne(QueryParameter queryParameter) {
        if (queryParameter == null) {
            throw new IllegalArgumentException("query parameter cannot be null");
        }
        return _filterBuilder.applyFilter(OperatorType.DIFFERENT_FROM, queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> greaterThan(QueryParameter queryParameter) {
        return gt(queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> gt(QueryParameter queryParameter) {
        if (queryParameter == null) {
            throw new IllegalArgumentException("query parameter cannot be null");
        }
        return _filterBuilder.applyFilter(OperatorType.GREATER_THAN, queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> lessThan(QueryParameter queryParameter) {
        return lt(queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> lt(QueryParameter queryParameter) {
        if (queryParameter == null) {
            throw new IllegalArgumentException("query parameter cannot be null");
        }
        return _filterBuilder.applyFilter(OperatorType.LESS_THAN, queryParameter);
    }

    @Override
    public SatisfiedWhereBuilder<GroupedQueryBuilder> like(QueryParameter queryParameter) {
        if (queryParameter == null) {
            throw new IllegalArgumentException("query parameter cannot be null");
        }
        return _filterBuilder.applyFilter(OperatorType.LIKE, queryParameter);
    }

}