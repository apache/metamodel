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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.BaseObject;

abstract class SatisfiedFromBuilderCallback extends BaseObject implements SatisfiedFromBuilder {

    private Query query;
    private DataContext dataContext;

    public SatisfiedFromBuilderCallback(Query query, DataContext dataContext) {
        this.query = query;
        this.dataContext = dataContext;
    }

    protected Query getQuery() {
        return query;
    }

    protected DataContext getDataContext() {
        return dataContext;
    }

    @Override
    public TableFromBuilder and(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("table cannot be null");
        }

        return new TableFromBuilderImpl(table, query, dataContext);
    }

    @Override
    public ColumnSelectBuilder<?> select(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }

        GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new ColumnSelectBuilderImpl(column, query, queryBuilder);
    }

    @Override
    public SatisfiedSelectBuilder<?> selectAll() {
        getQuery().selectAll();
        GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new SatisfiedSelectBuilderImpl(queryBuilder);
    }

    @Override
    public FunctionSelectBuilder<?> select(FunctionType function, String columnName) {
        GroupedQueryBuilderImpl queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        Column column = queryBuilder.findColumn(columnName);
        return select(function, column);
    }

    @Override
    public FunctionSelectBuilder<?> select(FunctionType function, String columnName, Object[] functionParameters) {
        GroupedQueryBuilderImpl queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        Column column = queryBuilder.findColumn(columnName);
        return select(function, column, functionParameters);
    }

    @Override
    public FunctionSelectBuilder<?> select(FunctionType function, Column column) {
        return select(function, column, new Object[0]);
    }

    @Override
    public FunctionSelectBuilder<?> select(FunctionType function, Column column, Object[] functionParameters) {
        if (function == null) {
            throw new IllegalArgumentException("functionType cannot be null");
        }
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }

        final GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new FunctionSelectBuilderImpl(function, column, functionParameters, query, queryBuilder);
    }

    @Override
    public CountSelectBuilder<?> selectCount() {
        GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new CountSelectBuilderImpl(query, queryBuilder);
    }

    @Override
    public TableFromBuilder and(String schemaName, String tableName) {
        if (schemaName == null) {
            throw new IllegalArgumentException("schemaName cannot be null");
        }
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }

        Schema schema = dataContext.getSchemaByName(schemaName);
        if (schema == null) {
            schema = dataContext.getDefaultSchema();
        }
        return and(schema, tableName);
    }

    private TableFromBuilder and(Schema schema, String tableName) {
        Table table = schema.getTableByName(tableName);
        return and(table);
    }

    @Override
    public TableFromBuilder and(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }
        return and(dataContext.getDefaultSchema(), tableName);
    }

    @Override
    public SatisfiedSelectBuilder<?> select(Column... columns) {
        if (columns == null) {
            throw new IllegalArgumentException("columns cannot be null");
        }
        query.select(columns);
        GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new SatisfiedSelectBuilderImpl(queryBuilder);
    }

    @Override
    public SatisfiedSelectBuilder<?> select(List<Column> columns) {
        return select(columns.toArray(new Column[columns.size()]));
    }

    @Override
    public SatisfiedSelectBuilder<?> select(String... columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null");
        }
        for (String columnName : columnNames) {
            select(columnName);
        }
        GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new SatisfiedSelectBuilderImpl(queryBuilder);
    }
    
    @Override
    public SatisfiedSelectBuilder<?> select(String selectExpression, boolean allowExpressionBasedSelectItem) {
        if (selectExpression == null) {
            throw new IllegalArgumentException("selectExpression cannot be null");
        }

        query.select(selectExpression, allowExpressionBasedSelectItem);

        final GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new SatisfiedSelectBuilderImpl(queryBuilder);
    }

    @Override
    public SatisfiedSelectBuilder<?> select(String selectExpression) {
        return select(selectExpression, false);
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(query);
    }

}