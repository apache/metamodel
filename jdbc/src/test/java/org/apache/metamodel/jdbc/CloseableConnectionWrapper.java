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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.Ignore;

/**
 * A wrapper around a connection, used to assert whether or not a it has been
 * closed or not, without actually closing it.
 */
@Ignore
public class CloseableConnectionWrapper implements Connection {

	private boolean _closed = false;
	private final Connection _con;

	public CloseableConnectionWrapper(Connection con) {
		_con = con;
	}

	public void close() throws SQLException {
		_closed = true;
	}

	public boolean isClosed() throws SQLException {
		return _closed;
	}

	// remaining methods simply delegate to _con

	public void clearWarnings() throws SQLException {
		_con.clearWarnings();
	}

	public void commit() throws SQLException {
		_con.commit();
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return _con.createArrayOf(typeName, elements);
	}

	public Blob createBlob() throws SQLException {
		return _con.createBlob();
	}

	public Clob createClob() throws SQLException {
		return _con.createClob();
	}

	public NClob createNClob() throws SQLException {
		return _con.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		return _con.createSQLXML();
	}

	public Statement createStatement() throws SQLException {
		return _con.createStatement();
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return _con.createStatement(resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return _con.createStatement(resultSetType, resultSetConcurrency);
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return _con.createStruct(typeName, attributes);
	}

	public boolean getAutoCommit() throws SQLException {
		return _con.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		return _con.getCatalog();
	}

	public Properties getClientInfo() throws SQLException {
		return _con.getClientInfo();
	}

	public String getClientInfo(String name) throws SQLException {
		return _con.getClientInfo(name);
	}

	public int getHoldability() throws SQLException {
		return _con.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return _con.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		return _con.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return _con.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		return _con.getWarnings();
	}

	public boolean isReadOnly() throws SQLException {
		return _con.isReadOnly();
	}

	public boolean isValid(int timeout) throws SQLException {
		return _con.isValid(timeout);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return _con.isWrapperFor(iface);
	}

	public String nativeSQL(String sql) throws SQLException {
		return _con.nativeSQL(sql);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return _con.prepareCall(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return _con.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return _con.prepareCall(sql);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return _con.prepareStatement(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return _con.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return _con.prepareStatement(sql, autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return _con.prepareStatement(sql, columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return _con.prepareStatement(sql, columnNames);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return _con.prepareStatement(sql);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		_con.releaseSavepoint(savepoint);
	}

	public void rollback() throws SQLException {
		_con.rollback();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		_con.rollback(savepoint);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		_con.setAutoCommit(autoCommit);
	}

	public void setCatalog(String catalog) throws SQLException {
		_con.setCatalog(catalog);
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		_con.setClientInfo(properties);
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		_con.setClientInfo(name, value);
	}

	public void setHoldability(int holdability) throws SQLException {
		_con.setHoldability(holdability);
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		_con.setReadOnly(readOnly);
	}

	public Savepoint setSavepoint() throws SQLException {
		return _con.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return _con.setSavepoint(name);
	}

	public void setTransactionIsolation(int level) throws SQLException {
		_con.setTransactionIsolation(level);
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		_con.setTypeMap(map);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return _con.unwrap(iface);
	}

	public void setSchema(String schema) throws SQLException {
		throw new UnsupportedOperationException("Jdbc 4.1 methods are not wrapped");
	}

	public String getSchema() throws SQLException {
		throw new UnsupportedOperationException("Jdbc 4.1 methods are not wrapped");
	}

	public void abort(Executor executor) throws SQLException {
		throw new UnsupportedOperationException("Jdbc 4.1 methods are not wrapped");
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		throw new UnsupportedOperationException("Jdbc 4.1 methods are not wrapped");
	}

	public int getNetworkTimeout() throws SQLException {
		throw new UnsupportedOperationException("Jdbc 4.1 methods are not wrapped");
	}

}
