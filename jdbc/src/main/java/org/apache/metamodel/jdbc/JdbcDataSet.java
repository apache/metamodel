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
package org.apache.metamodel.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataSet implementation that wraps a JDBC resultset.
 */
final class JdbcDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSet.class);

    private final JdbcCompiledQuery _compiledQuery;
    private final JdbcCompiledQueryLease _lease;
    private final Statement _statement;
    private final ResultSet _resultSet;
    private final JdbcDataContext _jdbcDataContext;
    private final Connection _connection;
    private Row _row;
    private boolean _closed;

    /**
     * Constructor used for regular query execution.
     * 
     * @param query
     * @param jdbcDataContext
     * @param connection
     * @param statement
     * @param resultSet
     */
    public JdbcDataSet(Query query, JdbcDataContext jdbcDataContext, Connection connection, Statement statement,
            ResultSet resultSet) {
        super(query.getSelectClause().getItems());
        if (query == null || statement == null || resultSet == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }
        _jdbcDataContext = jdbcDataContext;
        _connection = connection;
        _statement = statement;
        _resultSet = resultSet;
        _closed = false;
        _compiledQuery = null;
        _lease = null;
    }

    /**
     * Constructor used for compiled query execution
     * 
     * @param query
     * @param jdbcDataContext
     * @param resultSet
     */
    public JdbcDataSet(JdbcCompiledQuery compiledQuery, JdbcCompiledQueryLease lease, ResultSet resultSet) {
        super(compiledQuery.getSelectItems());
        if (compiledQuery == null || lease == null || resultSet == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }

        _compiledQuery = compiledQuery;
        _lease = lease;

        _jdbcDataContext = null;
        _connection = null;
        _statement = null;
        _resultSet = resultSet;
        _closed = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row getRow() {
        return _row;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean next() throws MetaModelException {
        try {
            boolean result = _resultSet.next();
            if (result) {
                Object[] values = new Object[getHeader().size()];
                for (int i = 0; i < values.length; i++) {

                    values[i] = getValue(_resultSet, i);

                    try {
                        // some drivers return boxed primitive types in stead of
                        // nulls (such as false in stead of null for a Boolean
                        // column)
                        if (_resultSet.wasNull()) {
                            values[i] = null;
                        }
                    } catch (Exception e) {
                        logger.debug("Could not invoke wasNull() method on resultset, error message: {}", e
                                .getMessage());
                    }
                }
                _row = new DefaultRow(getHeader(), values);
            } else {
                _row = null;
            }
            return result;
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "get next record in resultset");
        }
    }

    private Object getValue(ResultSet resultSet, int i) throws SQLException {
        final SelectItem selectItem = getHeader().getSelectItem(i);
        final int columnIndex = i + 1;
        if (selectItem.getAggregateFunction() == null) {
            final Column column = selectItem.getColumn();
            if (column != null) {
                final IQueryRewriter queryRewriter;
                if (_jdbcDataContext == null) {
                    queryRewriter = new DefaultQueryRewriter(null);
                } else {
                    queryRewriter = _jdbcDataContext.getQueryRewriter();
                }
                return queryRewriter.getResultSetValue(resultSet, columnIndex, column);
            }
        }
        return resultSet.getObject(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (_closed) {
            return;
        }

        FileHelper.safeClose(_resultSet);

        if (_jdbcDataContext != null) {
            FileHelper.safeClose(_statement);
            _jdbcDataContext.close(_connection);
        }
        if (_compiledQuery != null) {
            _compiledQuery.returnLease(_lease);
        }
        _closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!_closed) {
            logger.warn("finalize() invoked, but DataSet is not closed. Invoking close() on {}", this);
            close();
        }
    }
}