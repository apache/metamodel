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

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Table;

/**
 * Column implementation for JDBC data contexts.
 */
final class JdbcColumn extends MutableColumn {

	private static final long serialVersionUID = 389872697452157919L;

	public JdbcColumn(String columnName, ColumnType columnType, JdbcTable table, int columnNumber, Boolean nullable) {
		super(columnName, columnType, table, columnNumber, nullable);
	}

	@Override
	public boolean isIndexed() {
		Table table = getTable();
		if (table instanceof JdbcTable) {
			((JdbcTable) table).loadIndexes();
		}
		return super.isIndexed();
	}

	@Override
	public boolean isPrimaryKey() {
		Table table = getTable();
		if (table instanceof JdbcTable) {
			((JdbcTable) table).loadPrimaryKeys();
		}
		return super.isPrimaryKey();
	}
}