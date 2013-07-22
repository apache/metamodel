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
import java.sql.PreparedStatement;

/**
 * Represents a "lease" of a JdbcCompiledQuery. A lease holds the
 * {@link Connection} and {@link PreparedStatement} object associated with
 * executing a compiled query. Since these are not thread-safe, but expensive to
 * create, they are pooled to allow proper isolation when executing compiled
 * queries.
 */
final class JdbcCompiledQueryLease {

    private final Connection _connection;
    private final PreparedStatement _statement;

    public JdbcCompiledQueryLease(Connection connection, PreparedStatement statement) {
        _connection = connection;
        _statement = statement;
    }
    
    public Connection getConnection() {
        return _connection;
    }
    
    public PreparedStatement getStatement() {
        return _statement;
    }
}
