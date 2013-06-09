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
package org.eobjects.metamodel.query.builder;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.QueryParameter;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

final class WhereBuilderImpl extends AbstractQueryFilterBuilder<SatisfiedWhereBuilder<GroupedQueryBuilder>> implements
        WhereBuilder<GroupedQueryBuilder>, SatisfiedWhereBuilder<GroupedQueryBuilder> {

    private final Query _query;
    private final List<FilterItem> _orFilters;
    private FilterItem _parentOrFilter;

    public WhereBuilderImpl(Column column, Query query, GroupedQueryBuilder queryBuilder) {
        super(new SelectItem(column), queryBuilder);
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