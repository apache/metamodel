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
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.builder.InitFromBuilder;
import org.apache.metamodel.query.builder.InitFromBuilderImpl;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * A delegate {@link DataContext} which wraps the {@link JdbcDataContext} for schema traversal, but uses a fixed
 * connection to execute any queries
 */
final class JdbcUpdateCallbackDataContext implements DataContext {

    private final JdbcDataContext delegate;
    private final Connection connection;

    public JdbcUpdateCallbackDataContext(JdbcDataContext delegate, Connection connection) {
        this.delegate = delegate;
        this.connection = connection;
    }

    @Override
    public DataSet executeQuery(Query query) throws MetaModelException {
        // use the overloaded method that defines the query connection
        return delegate.executeQuery(connection, query, false);
    }

    @Override
    public InitFromBuilder query() {
        // produce an InitFromBuilder that uses _this_ datacontext
        return new InitFromBuilderImpl(this);
    }

    @Override
    public DataContext refreshSchemas() {
        delegate.refreshSchemas();
        return this;
    }

    @Override
    public DataSet executeQuery(String queryString) throws MetaModelException {
        final Query query = parseQuery(queryString);
        return executeQuery(query);
    }

    @Override
    public DataSet executeQuery(CompiledQuery compiledQuery, Object... values) {
        // it would be better to reuse the connection here, but JDBC compiled queries (prepared statements) are tied to
        // the connections with which they where made, so the connection cannot be replaced.
        return delegate.executeQuery(compiledQuery, values);
    }

    @Override
    public List<Schema> getSchemas() throws MetaModelException {
        return delegate.getSchemas();
    }

    @Override
    public List<String> getSchemaNames() throws MetaModelException {
        return delegate.getSchemaNames();
    }

    @Override
    public Schema getDefaultSchema() throws MetaModelException {
        return delegate.getDefaultSchema();
    }

    @Override
    public Schema getSchemaByName(String name) throws MetaModelException {
        return delegate.getSchemaByName(name);
    }

    @Override
    public Query parseQuery(String queryString) throws MetaModelException {
        return delegate.parseQuery(queryString);
    }

    @Override
    public CompiledQuery compileQuery(Query query) throws MetaModelException {
        return delegate.compileQuery(query);
    }

    @Override
    public Column getColumnByQualifiedLabel(String columnName) {
        return delegate.getColumnByQualifiedLabel(columnName);
    }

    @Override
    public Table getTableByQualifiedLabel(String tableName) {
        return delegate.getTableByQualifiedLabel(tableName);
    }

}
