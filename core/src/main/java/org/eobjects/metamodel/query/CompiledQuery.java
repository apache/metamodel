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
package org.eobjects.metamodel.query;

import java.io.Closeable;
import java.util.List;

import org.eobjects.metamodel.DataContext;

/**
 * A {@link CompiledQuery} is a {@link Query} which has been compiled, typically
 * by the data source itself, to provide optimized execution speed. Compiled
 * queries are produced using the {@link DataContext#compileQuery(Query)} method.
 * 
 * Typically the compilation itself takes a bit of time, but firing the compiled
 * query is faster than regular queries. This means that for repeated executions
 * of the same query, it is usually faster to use compiled queries.
 * 
 * To make {@link CompiledQuery} useful for more than just one specific query,
 * variations of the query can be fired, as long as the variations can be
 * expressed as a {@link QueryParameter} for instance in the WHERE clause of the
 * query.
 * 
 * @see DataContext#compileQuery(Query)
 * @see QueryParameter
 */
public interface CompiledQuery extends Closeable {

    /**
     * Gets the {@link QueryParameter}s associated with the compiled query.
     * Values for these parameters are expected when the query is executed.
     * 
     * @return a list of query parameters
     */
    public List<QueryParameter> getParameters();

    /**
     * A representation of the query as SQL.
     * 
     * @return a SQL string.
     */
    public String toSql();

    /**
     * Closes any resources related to the compiled query.
     */
    @Override
    public void close();
}
