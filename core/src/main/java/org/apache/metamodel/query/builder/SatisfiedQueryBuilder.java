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

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.InvokableQuery;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.schema.Column;

/**
 * Represents a built query that is satisfied and ready for querying or further
 * building.
 * 
 * @param <B>
 */
public interface SatisfiedQueryBuilder<B extends SatisfiedQueryBuilder<?>> extends InvokableQuery {

    public ColumnSelectBuilder<B> select(Column column);

    public SatisfiedSelectBuilder<B> select(Column... columns);

    /**
     * Sets the offset (number of rows to skip) of the query that is being
     * built.
     * 
     * Note that this number is a 0-based variant of invoking
     * {@link #firstRow(int)}.
     * 
     * @param offset
     *            the number of rows to skip
     * @return
     */
    public SatisfiedQueryBuilder<B> offset(int offset);

    /**
     * Sets the first row of the query that is being built.
     * 
     * Note that this is a 1-based variant of invoking {@link #limit(int)}.
     * 
     * @param firstRow
     * @return
     */
    public SatisfiedQueryBuilder<B> firstRow(int firstRow);

    /**
     * Sets the limit (aka. max rows) of the query that is being built.
     * 
     * @param limit
     * @return
     */
    public SatisfiedQueryBuilder<B> limit(int limit);

    /**
     * Sets the max rows (aka. limit) of the query that is being built.
     * 
     * @param maxRows
     * @return
     */
    public SatisfiedQueryBuilder<B> maxRows(int maxRows);

    public FunctionSelectBuilder<B> select(FunctionType function, Column column);

    public SatisfiedQueryBuilder<?> select(FunctionType function, String columnName);

    public CountSelectBuilder<B> selectCount();

    public ColumnSelectBuilder<B> select(String columnName);

    public WhereBuilder<B> where(Column column);

    public WhereBuilder<B> where(String columnName);
    
    public WhereBuilder<B> where(ScalarFunction function, Column column);
    
    public WhereBuilder<B> where(ScalarFunction function, String columnName);

    public SatisfiedQueryBuilder<B> where(FilterItem... filters);

    public SatisfiedQueryBuilder<B> where(Iterable<FilterItem> filters);

    public SatisfiedOrderByBuilder<B> orderBy(String columnName);

    public SatisfiedOrderByBuilder<B> orderBy(Column column);

    public GroupedQueryBuilder groupBy(String columnName);
    
    public GroupedQueryBuilder groupBy(String ... columnNames);

    public GroupedQueryBuilder groupBy(Column column);

    public GroupedQueryBuilder groupBy(Column... columns);

    /**
     * Gets the built query as a {@link Query} object. Typically the returned
     * query will be a clone of the built query to prevent conflicting
     * mutations.
     * 
     * @return a {@link Query} object representing the built query.
     */
    public Query toQuery();

    /**
     * Finds a column by name within the already defined FROM items
     * 
     * @param columnName
     * @return
     * @throws IllegalArgumentException
     */
    public Column findColumn(String columnName) throws IllegalArgumentException;
}