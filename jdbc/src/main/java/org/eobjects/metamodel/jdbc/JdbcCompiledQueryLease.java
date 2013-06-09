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
package org.eobjects.metamodel.jdbc;

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
