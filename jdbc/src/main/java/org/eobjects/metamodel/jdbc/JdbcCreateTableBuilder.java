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
import java.sql.SQLException;
import java.sql.Statement;

import org.eobjects.metamodel.create.AbstractTableCreationBuilder;
import org.eobjects.metamodel.jdbc.dialects.IQueryRewriter;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CreateTableBuilder} implementation that issues a SQL CREATE TABLE
 * statement.
 * 
 * @author Kasper Sørensen
 */
final class JdbcCreateTableBuilder extends AbstractTableCreationBuilder<JdbcUpdateCallback> {

	private static final Logger logger = LoggerFactory.getLogger(JdbcCreateTableBuilder.class);

	public JdbcCreateTableBuilder(JdbcUpdateCallback updateCallback, Schema schema, String name) {
		super(updateCallback, schema, name);
		if (!(schema instanceof JdbcSchema)) {
			throw new IllegalArgumentException("Not a valid JDBC schema: " + schema);
		}
	}

	@Override
	public Table execute() {
		final String sql = createSqlStatement();
		logger.info("Create table statement created: {}", sql);

		Connection connection = getUpdateCallback().getConnection();
		Statement st = null;
		try {
			st = connection.createStatement();
			int rowsAffected = st.executeUpdate(sql);
			logger.debug("Create table statement executed, {} rows affected", rowsAffected);
		} catch (SQLException e) {
			throw JdbcUtils.wrapException(e, "execute create table statement: " + sql);
		} finally {
			FileHelper.safeClose(st);
		}

		JdbcSchema schema = (JdbcSchema) getSchema();
		schema.refreshTables();
		return schema.getTableByName(getTable().getName());
	}

	protected String createSqlStatement() {
		return createSqlStatement(getTable());
	}

	private String createSqlStatement(Table table) {
		final IQueryRewriter queryRewriter = getUpdateCallback().getDataContext().getQueryRewriter();
		final StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		final Schema schema = getSchema();
		if (schema != null && schema.getName() != null) {
			sb.append(schema.getQualifiedLabel());
			sb.append(".");
		}
		sb.append(getUpdateCallback().quoteIfNescesary(table.getName()));
		sb.append(" (");
		final Column[] columns = table.getColumns();
		for (int i = 0; i < columns.length; i++) {
			final Column column = columns[i];
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(getUpdateCallback().quoteIfNescesary(column.getName()));
			sb.append(' ');
			final String nativeType = column.getNativeType();
			if (nativeType == null) {
				ColumnType columnType = column.getType();
				if (columnType == null) {
					columnType = ColumnType.VARCHAR;
				}

				final String columnTypeString = queryRewriter.rewriteColumnType(columnType);

				sb.append(columnTypeString);
			} else {
				sb.append(nativeType);
			}
			final Integer columnSize = column.getColumnSize();
			if (columnSize != null) {
				sb.append('(');
				sb.append(columnSize.intValue());
				sb.append(')');
			}
			if (column.isNullable() != null && !column.isNullable().booleanValue()) {
				sb.append(" NOT NULL");
			}
			if (column.isPrimaryKey()) {
				sb.append(" PRIMARY KEY");
			}
		}
		sb.append(")");
		return sb.toString();
	}

}
