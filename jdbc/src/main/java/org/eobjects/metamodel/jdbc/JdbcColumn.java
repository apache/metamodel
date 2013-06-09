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

import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.Table;

/**
 * Column implementation for JDBC data contexts.
 * 
 * @author Kasper Sørensen
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