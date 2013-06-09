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

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.CompiledQuery;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;

/**
 * Represents a built query that is satisfied and ready for querying or further
 * building.
 * 
 * @author Kasper Sørensen
 * 
 * @param <B>
 */
public interface SatisfiedQueryBuilder<B extends SatisfiedQueryBuilder<?>> {

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
     * @param maxRows
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

    public FunctionSelectBuilder<B> select(FunctionType functionType, Column column);

    public CountSelectBuilder<B> selectCount();

    public ColumnSelectBuilder<B> select(String columnName);

    public WhereBuilder<B> where(Column column);

    public WhereBuilder<B> where(String columnName);

    public SatisfiedQueryBuilder<B> where(FilterItem... filters);

    public SatisfiedQueryBuilder<B> where(Iterable<FilterItem> filters);

    public SatisfiedOrderByBuilder<B> orderBy(String columnName);

    public SatisfiedOrderByBuilder<B> orderBy(Column column);

    public GroupedQueryBuilder groupBy(String columnName);

    public GroupedQueryBuilder groupBy(Column column);

    public B groupBy(Column... columns);

    /**
     * Gets the built query as a {@link Query} object. Typically the returned
     * query will be a clone of the built query to prevent conflicting
     * mutations.
     * 
     * @return a {@link Query} object representing the built query.
     */
    public Query toQuery();

    public CompiledQuery compile();

    /**
     * Executes the built query. This call is similar to calling
     * {@link #toQuery()} and then {@link DataContext#executeQuery(Query)}.
     * 
     * @return the {@link DataSet} that is returned by executing the query.
     */
    public DataSet execute();

    /**
     * Finds a column by name within the already defined FROM items
     * 
     * @param columnName
     * @return
     * @throws IllegalArgumentException
     */
    public Column findColumn(String columnName) throws IllegalArgumentException;
}