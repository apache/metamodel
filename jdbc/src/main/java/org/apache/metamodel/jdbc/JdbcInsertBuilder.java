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
import java.util.Arrays;

import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RowInsertionBuilder} that issues an SQL INSERT statement
 */
final class JdbcInsertBuilder extends AbstractRowInsertionBuilder<JdbcUpdateCallback> {

	private static final Logger logger = LoggerFactory.getLogger(JdbcInsertBuilder.class);

	private final boolean _inlineValues;
	private final IQueryRewriter _queryRewriter;

	public JdbcInsertBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter) {
		this(updateCallback, table, false, queryRewriter);
	}

	public JdbcInsertBuilder(JdbcUpdateCallback updateCallback, Table table, boolean isInlineValues,
			IQueryRewriter queryRewriter) {
		super(updateCallback, table);
		if (!(table instanceof JdbcTable)) {
			throw new IllegalArgumentException("Not a valid JDBC table: " + table);
		}

		_inlineValues = isInlineValues;
		_queryRewriter = queryRewriter;
	}

	@Override
	public void execute() {
		final String sql = createSqlStatement();
		if (logger.isDebugEnabled()) {
			logger.debug("Inserting: {}", Arrays.toString(getValues()));
			logger.debug("Insert statement created: {}", sql);
		}
		final JdbcUpdateCallback updateCallback = getUpdateCallback();
		final boolean reuseStatement = !_inlineValues;
		final PreparedStatement st = updateCallback.getPreparedStatement(sql, reuseStatement);
		try {
			if (reuseStatement) {
				Column[] columns = getColumns();
				Object[] values = getValues();
				boolean[] explicitNulls = getExplicitNulls();
				int valueCounter = 1;
				for (int i = 0; i < columns.length; i++) {
					boolean explicitNull = explicitNulls[i];
					if (values[i] != null || explicitNull) {
					    _queryRewriter.setStatementParameter(st, valueCounter, columns[i], values[i]);
						valueCounter++;
					}
				}
			}
			updateCallback.executePreparedStatement(st, reuseStatement);
		} catch (SQLException e) {
			throw JdbcUtils.wrapException(e, "execute insert statement: " + sql);
		} finally {
			if (_inlineValues) {
				FileHelper.safeClose(st);
			}
		}
	}
	
	protected String createSqlStatement() {
	    return createSqlStatement(_inlineValues);
	}

	private String createSqlStatement(boolean inlineValues) {
		final Object[] values = getValues();
		final Table table = getTable();
		final StringBuilder sb = new StringBuilder();

		final String tableLabel = _queryRewriter.rewriteFromItem(new FromItem(table));

		sb.append("INSERT INTO ");
		sb.append(tableLabel);
		sb.append(" (");
		Column[] columns = getColumns();
		boolean[] explicitNulls = getExplicitNulls();
		boolean firstValue = true;
		for (int i = 0; i < columns.length; i++) {
			if (values[i] != null || explicitNulls[i]) {
				if (firstValue) {
					firstValue = false;
				} else {
					sb.append(',');
				}
				String columnName = columns[i].getName();
				columnName = getUpdateCallback().quoteIfNescesary(columnName);
				sb.append(columnName);
			}
		}

		sb.append(") VALUES (");
		firstValue = true;
		for (int i = 0; i < columns.length; i++) {
			if (values[i] != null || explicitNulls[i]) {
				if (firstValue) {
					firstValue = false;
				} else {
					sb.append(',');
				}
				if (inlineValues) {
					sb.append(JdbcUtils.getValueAsSql(columns[i], values[i], _queryRewriter));
				} else {
					sb.append('?');
				}
			}
		}
		sb.append(")");
		String sql = sb.toString();
		return sql;
	}

	@Override
	public String toSql() {
	    return createSqlStatement(true);
	}
}
