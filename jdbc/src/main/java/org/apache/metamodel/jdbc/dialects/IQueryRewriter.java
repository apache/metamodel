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
package org.apache.metamodel.jdbc.dialects;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.AggregateFunction;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;

/**
 * A query rewriter can be used for rewriting (part of) a query's string
 * representation. This is usefull for databases that deviate from the SQL 99
 * compliant syntax which is delievered by the query and it's query item's
 * toString() methods.
 * 
 * @see AbstractQueryRewriter
 * @see JdbcDataContext
 */
public interface IQueryRewriter {

    public String rewriteFromItem(FromItem item);

    public String rewriteQuery(Query query);

    public String rewriteFilterItem(FilterItem whereItem);

    /**
     * Method which handles the action of setting a parameterized value on a
     * statement. Traditionally this is done using the
     * {@link PreparedStatement#setObject(int, Object)} method but for some
     * types we use more specific setter methods.
     * 
     * @param st
     * @param valueIndex
     * @param column
     * @param value
     * @throws SQLException
     */
    public void setStatementParameter(final PreparedStatement st, final int valueIndex, final Column column,
            final Object value) throws SQLException;

    /**
     * Retrieves a value from a JDBC {@link ResultSet} when the anticipated value is mapped to a particular column.
     * 
     * @param resultSet
     * @param columnIndex
     * @param column
     * @throws SQLException
     * @return
     */
    public Object getResultSetValue(ResultSet resultSet, int columnIndex, Column column) throws SQLException;

    /**
     * Gets whether this query rewriter is able to write the "Max rows" query
     * property to the query string.
     * 
     * @return whether this query rewriter is able to write the "Max rows" query
     *         property to the query string.
     */
    public boolean isMaxRowsSupported();

    /**
     * Gets whether this query rewriter is able to write the "First row" query
     * property to the query string.
     * 
     * @return whether this query rewriter is able to write the "First row"
     *         query property to the query string.
     */
    public boolean isFirstRowSupported();

    /**
     * Determines whether a specific scalar function is supported by the
     * database or not.
     * 
     * If the function is not supported then MetaModel will handle the function
     * on the client side.
     * 
     * @param function
     * @return
     */
    public boolean isScalarFunctionSupported(ScalarFunction function);

    /**
     * Determines whether a specific aggregate function is supported by the
     * database or not.
     * 
     * If the function is not supported then MetaModel will handle the function
     * on the client side.
     * 
     * @param function
     * @return
     */
    public boolean isAggregateFunctionSupported(AggregateFunction function);

    /**
     * Escapes the quotes within a String literal of a query item.
     * 
     * @return String item with quotes escaped.
     */
    public String escapeQuotes(String item);

    /**
     * Rewrites the name of a column type, as it is written in CREATE TABLE
     * statements. Some databases dont support all column types, or have
     * different names for them. The implementation of this method will do that
     * conversion.
     * 
     * @param columnType
     *            the (non-null) {@link ColumnType} to rewrite
     * @param columnSize
     *            the (possibly null) column size that may or may not have been
     *            specified
     * @return
     */
    public String rewriteColumnType(ColumnType columnType, Integer columnSize);

    /**
     * Gets the column type for a specific JDBC type (as defined in
     * {@link Types}), native type name and column size.
     * 
     * @param jdbcType
     * @param nativeType
     * @param columnSize
     * @return
     */
    public ColumnType getColumnType(int jdbcType, String nativeType, Integer columnSize);

    /**
     * Determines if the JDBC data source supports transactions or not. Usually
     * this is the case since JDBC is designed for ACID compliant databases, but
     * in some cases the JDBC interface is used also to facilitate connectivity
     * to non-transactional data source such as Apache Hive and others.
     * 
     * @return
     */
    public boolean isTransactional();
}