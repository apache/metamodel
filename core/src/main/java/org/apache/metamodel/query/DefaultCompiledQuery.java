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
package org.apache.metamodel.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a default implementation of the {@link CompiledQuery} interface.
 * This implementation does not actually do anything to prepare the query, but
 * allows creating a clone of the originating query with the parameters replaced
 * by values.
 */
public class DefaultCompiledQuery implements CompiledQuery {

    private final Query _query;
    private final List<QueryParameter> _parameters;

    public DefaultCompiledQuery(Query query) {
        _query = query;
        _parameters = createParameterList();
    }

    /**
     * Clones the query while replacing query parameters with corresponding
     * values.
     * 
     * @param values
     * @return
     */
    public Query cloneWithParameterValues(Object[] values) {
        final AtomicInteger parameterIndex = new AtomicInteger(0);
        final Query clonedQuery = _query.clone();
        replaceParametersInQuery(values, parameterIndex, _query, clonedQuery);
        return clonedQuery;
    }

    private void replaceParametersInQuery(Object[] values, AtomicInteger parameterIndex, Query originalQuery,
            Query newQuery) {
        replaceParametersInFromClause(values, parameterIndex, originalQuery, newQuery);
        replaceParametersInWhereClause(values, parameterIndex, originalQuery, newQuery);
    }

    private void replaceParametersInWhereClause(Object[] values, final AtomicInteger parameterIndex,
            Query originalQuery, Query newQuery) {
        // creates a clone of the original query, but rebuilds a completely new
        // where clause based on parameter values

        final List<FilterItem> items = originalQuery.getWhereClause().getItems();
        int i = 0;
        for (FilterItem filterItem : items) {
            final FilterItem newFilter = copyFilterItem(filterItem, values, parameterIndex);
            if (filterItem != newFilter) {
                newQuery.getWhereClause().removeItem(i);
                newQuery.getWhereClause().addItem(i, newFilter);
            }
            i++;
        }
    }

    private void replaceParametersInFromClause(Object[] values, AtomicInteger parameterIndex, Query originalQuery,
            Query newQuery) {
        final List<FromItem> fromItems = originalQuery.getFromClause().getItems();
        int i = 0;
        for (FromItem fromItem : fromItems) {
            final Query subQuery = fromItem.getSubQuery();
            if (subQuery != null) {
                final Query newSubQuery = newQuery.getFromClause().getItem(i).getSubQuery();
                replaceParametersInQuery(values, parameterIndex, subQuery, newSubQuery);

                newQuery.getFromClause().removeItem(i);
                newQuery.getFromClause().addItem(i, new FromItem(newSubQuery).setAlias(fromItem.getAlias()));
            }
            i++;
        }
    }

    private FilterItem copyFilterItem(FilterItem item, Object[] values, AtomicInteger parameterIndex) {
        if (item.isCompoundFilter()) {
            final FilterItem[] childItems = item.getChildItems();
            final FilterItem[] newChildItems = new FilterItem[childItems.length];
            for (int i = 0; i < childItems.length; i++) {
                final FilterItem childItem = childItems[i];
                final FilterItem newChildItem = copyFilterItem(childItem, values, parameterIndex);
                newChildItems[i] = newChildItem;
            }
            final FilterItem newFilter = new FilterItem(item.getLogicalOperator(), newChildItems);
            return newFilter;
        } else {
            if (item.getOperand() instanceof QueryParameter) {
                final Object newOperand = values[parameterIndex.getAndIncrement()];
                final FilterItem newFilter = new FilterItem(item.getSelectItem(), item.getOperator(), newOperand);
                return newFilter;
            } else {
                return item;
            }
        }
    }

    private List<QueryParameter> createParameterList() {
        final List<QueryParameter> parameters = new ArrayList<QueryParameter>();

        buildParameterListInFromClause(parameters, _query);
        buildParameterListInWhereClause(parameters, _query);
        return parameters;
    }

    private void buildParameterListInWhereClause(List<QueryParameter> parameters, Query query) {
        List<FilterItem> items = query.getWhereClause().getItems();
        for (FilterItem item : items) {
            buildParameterFromFilterItem(parameters, item);
        }
    }

    private void buildParameterListInFromClause(List<QueryParameter> parameters, Query query) {
        List<FromItem> fromItems = query.getFromClause().getItems();
        for (FromItem fromItem : fromItems) {
            Query subQuery = fromItem.getSubQuery();
            if (subQuery != null) {
                buildParameterListInFromClause(parameters, subQuery);
                buildParameterListInWhereClause(parameters, subQuery);
            }
        }
    }

    @Override
    public List<QueryParameter> getParameters() {
        return _parameters;
    }

    @Override
    public String toSql() {
        return _query.toSql();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + toSql() + "]";
    }

    @Override
    public void close() {
        // do nothing
    }

    private void buildParameterFromFilterItem(List<QueryParameter> parameters, FilterItem item) {
        if (item.isCompoundFilter()) {
            FilterItem[] childItems = item.getChildItems();
            for (FilterItem childItem : childItems) {
                buildParameterFromFilterItem(parameters, childItem);
            }
        } else {
            if (item.getOperand() instanceof QueryParameter) {
                parameters.add((QueryParameter) item.getOperand());
            }
        }
    }

}
