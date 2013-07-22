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
