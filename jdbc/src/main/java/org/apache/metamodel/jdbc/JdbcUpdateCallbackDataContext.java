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

import org.apache.metamodel.AbstractDataContext;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Schema;

/**
 * A delegate {@link DataContext} which wraps the {@link JdbcDataContext} for schema traversal, but uses a fixed
 * connection to execute any queries
 */
final class JdbcUpdateCallbackDataContext extends AbstractDataContext {

    private final JdbcDataContext delegate;
    private final Connection connection;

    public JdbcUpdateCallbackDataContext(JdbcDataContext delegate, Connection connection) {
        this.delegate = delegate;
        this.connection = connection;
    }

    @Override
    public DataSet executeQuery(Query query) throws MetaModelException {
        return delegate.executeQuery(connection, query); 
    }

    @Override
    protected List<String> getSchemaNamesInternal() {
        return delegate.getSchemaNames();
    }

    @Override
    protected String getDefaultSchemaName() {
        return delegate.getDefaultSchemaName();
    }

    @Override
    protected Schema getSchemaByNameInternal(String name) {
        return delegate.getSchemaByNameInternal(name);
    }
}
