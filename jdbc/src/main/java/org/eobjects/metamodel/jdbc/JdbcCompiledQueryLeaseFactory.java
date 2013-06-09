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
import java.sql.SQLException;

import org.apache.commons.pool.PoolableObjectFactory;

/**
 * Factory for the object pool of {@link JdbcCompiledQueryLease}s.
 */
final class JdbcCompiledQueryLeaseFactory implements PoolableObjectFactory<JdbcCompiledQueryLease> {

	private final JdbcDataContext _dataContext;
    private final Connection _connection;
    private final String _sql;

    public JdbcCompiledQueryLeaseFactory(JdbcDataContext dataContext, Connection connection, String sql) {
    	_dataContext = dataContext;
        _connection = connection;
        _sql = sql;
    }
    

    @Override
    public JdbcCompiledQueryLease makeObject() throws Exception {
        try {
            final PreparedStatement statement = _connection.prepareStatement(_sql);
            final JdbcCompiledQueryLease lease = new JdbcCompiledQueryLease(_connection, statement);
            return lease;
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "preparing statement");
        }
    }

    @Override
    public void destroyObject(JdbcCompiledQueryLease lease) throws Exception {
        _dataContext.close(null, null, lease.getStatement());
    }

    @Override
    public boolean validateObject(JdbcCompiledQueryLease lease) {
        return true;
    }

    @Override
    public void activateObject(JdbcCompiledQueryLease obj) throws Exception {
    }

    @Override
    public void passivateObject(JdbcCompiledQueryLease obj) throws Exception {
    }
}
