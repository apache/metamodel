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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.DefaultCompiledQuery;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link CompiledQuery} for JDBC which uses a
 * {@link PreparedStatement} behind the scenes.
 */
final class JdbcCompiledQuery extends DefaultCompiledQuery implements CompiledQuery {

    private static final Logger logger = LoggerFactory.getLogger(JdbcCompiledQuery.class);

    private final String _sql;
    private final Query _query;
    private final GenericObjectPool<JdbcCompiledQueryLease> _pool;
    private boolean _closed;

    public JdbcCompiledQuery(JdbcDataContext dc, Query query) {
        super(query);
        _query = query;
        _sql = dc.getQueryRewriter().rewriteQuery(query);

        final Config config = new Config();
        config.maxActive = getSystemPropertyValue(JdbcDataContext.SYSTEM_PROPERTY_COMPILED_QUERY_POOL_MAX_SIZE, -1);
        config.minEvictableIdleTimeMillis = getSystemPropertyValue(
                JdbcDataContext.SYSTEM_PROPERTY_COMPILED_QUERY_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS, 500);
        config.timeBetweenEvictionRunsMillis = getSystemPropertyValue(
                JdbcDataContext.SYSTEM_PROPERTY_COMPILED_QUERY_POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS, 1000);

        _pool = new GenericObjectPool<JdbcCompiledQueryLease>(new JdbcCompiledQueryLeaseFactory(dc, _sql), config);
        _closed = false;

        logger.debug("Created compiled JDBC query: {}", _sql);
    }

    private int getSystemPropertyValue(String property, int defaultValue) {
        String str = System.getProperty(property);
        if (str == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            logger.debug("Failed to parse system property '{}': '{}'", property, str);
            return defaultValue;
        }
    }

    public JdbcCompiledQueryLease borrowLease() {
        if (logger.isDebugEnabled()) {
            logger.debug("Borrowing lease. Leases (before): Active={}, Idle={}", getActiveLeases(), getIdleLeases());
        }
        try {
            return _pool.borrowObject();
        } catch (Exception e) {
            throw handleError(e, "borrow lease");
        }
    }

    public void returnLease(JdbcCompiledQueryLease lease) {
        if (logger.isDebugEnabled()) {
            logger.debug("Returning lease. Leases (before): Active={}, Idle={}", getActiveLeases(), getIdleLeases());
        }
        try {
            _pool.returnObject(lease);
        } catch (Exception e) {
            throw handleError(e, "return lease");
        }
    }

    private RuntimeException handleError(Exception e, String message) {
        if (logger.isWarnEnabled()) {
            logger.warn("Unexpected error occurred in compiled JDBC query: " + message, e);
        }

        if (e instanceof SQLException) {
            return JdbcUtils.wrapException((SQLException) e, message);
        } else if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new MetaModelException(message, e);
        }
    }

    protected int getActiveLeases() {
        return _pool.getNumActive();
    }

    protected int getIdleLeases() {
        return _pool.getNumIdle();
    }

    protected Query getQuery() {
        return _query;
    }

    @Override
    public String toSql() {
        return _sql;
    }

    @Override
    public void close() {
        logger.debug("Closing compiled JDBC query: {}", _sql);
        try {
            _pool.close();
        } catch (Exception e) {
            throw handleError(e, "close pool of leases");
        } finally {
            _closed = true;
        }
    }

    protected List<SelectItem> getSelectItems() {
        return _query.getSelectClause().getItems();
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