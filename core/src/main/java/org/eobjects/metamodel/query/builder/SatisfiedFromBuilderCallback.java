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

import java.util.List;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.BaseObject;

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
    public FunctionSelectBuilder<?> select(FunctionType functionType, Column column) {
        if (functionType == null) {
            throw new IllegalArgumentException("functionType cannot be null");
        }
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }

        GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        return new FunctionSelectBuilderImpl(functionType, column, query, queryBuilder);
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
    public ColumnSelectBuilder<?> select(String columnName) {
        if (columnName == null) {
            throw new IllegalArgumentException("columnName cannot be null");
        }

        GroupedQueryBuilderImpl queryBuilder = new GroupedQueryBuilderImpl(dataContext, query);
        Column column = queryBuilder.findColumn(columnName);
        return select(column);
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(query);
    }

}