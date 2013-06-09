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

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.CompiledQuery;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.util.BaseObject;

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