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

import java.util.List;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.BaseObject;

abstract class GroupedQueryBuilderCallback extends BaseObject implements GroupedQueryBuilder {

    private GroupedQueryBuilder queryBuilder;

    public GroupedQueryBuilderCallback(GroupedQueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    protected GroupedQueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> firstRow(int firstRow) {
        return getQueryBuilder().firstRow(firstRow);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> limit(int maxRows) {
        return getQueryBuilder().limit(maxRows);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> offset(int offset) {
        return getQueryBuilder().offset(offset);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> maxRows(int maxRows) {
        return getQueryBuilder().maxRows(maxRows);
    }

    @Override
    public SatisfiedSelectBuilder<GroupedQueryBuilder> select(Column... columns) {
        return getQueryBuilder().select(columns);
    }

    @Override
    public final Column findColumn(String columnName) throws IllegalArgumentException {
        return getQueryBuilder().findColumn(columnName);
    }

    @Override
    public ColumnSelectBuilder<GroupedQueryBuilder> select(Column column) {
        return getQueryBuilder().select(column);
    }

    @Override
    public SatisfiedQueryBuilder<?> select(FunctionType function, String columnName) {
        return getQueryBuilder().select(function, columnName);
    }

    @Override
    public FunctionSelectBuilder<GroupedQueryBuilder> select(FunctionType functionType, Column column) {
        return getQueryBuilder().select(functionType, column);
    }

    @Override
    public ColumnSelectBuilder<GroupedQueryBuilder> select(String columnName) {
        return getQueryBuilder().select(columnName);
    }

    @Override
    public CountSelectBuilder<GroupedQueryBuilder> selectCount() {
        return getQueryBuilder().selectCount();
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> where(Column column) {
        return getQueryBuilder().where(column);
    }
    
    @Override
    public WhereBuilder<GroupedQueryBuilder> where(ScalarFunction function, Column column) {
        return getQueryBuilder().where(function, column);
    }
    
    @Override
    public WhereBuilder<GroupedQueryBuilder> where(ScalarFunction function, String columnName) {
        return getQueryBuilder().where(function, columnName);
    }

    @Override
    public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(Column column) {
        return getQueryBuilder().orderBy(column);
    }

    @Override
    public GroupedQueryBuilder groupBy(String columnName) {
        return getQueryBuilder().groupBy(columnName);
    }

    @Override
    public GroupedQueryBuilder groupBy(Column column) {
        return getQueryBuilder().groupBy(column);
    }

    @Override
    public Query toQuery() {
        return getQueryBuilder().toQuery();
    }

    @Override
    public CompiledQuery compile() {
        return getQueryBuilder().compile();
    }

    @Override
    public HavingBuilder having(FunctionType functionType, Column column) {
        return getQueryBuilder().having(functionType, column);
    }

    @Override
    public HavingBuilder having(String columnExpression) {
        return getQueryBuilder().having(columnExpression);
    }
    
    @Override
    public HavingBuilder having(SelectItem selectItem) {
        return getQueryBuilder().having(selectItem);
    }

    @Override
    public GroupedQueryBuilder groupBy(String... columnNames) {
        return getQueryBuilder().groupBy(columnNames);
    }

    @Override
    public GroupedQueryBuilder groupBy(Column... columns) {
        getQueryBuilder().groupBy(columns);
        return this;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(queryBuilder);
    }

    @Override
    public DataSet execute() {
        return queryBuilder.execute();
    }

    @Override
    public WhereBuilder<GroupedQueryBuilder> where(String columnName) {
        return getQueryBuilder().where(columnName);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> where(FilterItem... filters) {
        return getQueryBuilder().where(filters);
    }

    @Override
    public SatisfiedQueryBuilder<GroupedQueryBuilder> where(Iterable<FilterItem> filters) {
        return getQueryBuilder().where(filters);
    }

    @Override
    public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(String columnName) {
        return getQueryBuilder().orderBy(columnName);
    }

    @Override
    public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(FunctionType function, Column column) {
        return getQueryBuilder().orderBy(function, column);
    }
}