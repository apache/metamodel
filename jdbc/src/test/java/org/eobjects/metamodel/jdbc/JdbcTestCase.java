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
import java.sql.DriverManager;

import junit.framework.TestCase;

public abstract class JdbcTestCase extends TestCase {

	private Connection _connection;

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		if (_connection != null) {
			if (!_connection.isClosed()) {
				_connection.close();
			}
			_connection = null;
		}
	}

	public Connection getTestDbConnection() throws Exception {
		if (_connection == null || _connection.isClosed()) {
			Class.forName("org.hsqldb.jdbcDriver");
			_connection = DriverManager
					.getConnection("jdbc:hsqldb:res:metamodel");
			_connection.setReadOnly(true);
		}
		return _connection;
	}

}
