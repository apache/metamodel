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
package org.eobjects.metamodel.schema;

import java.util.List;

import org.eobjects.metamodel.util.BaseObject;

public abstract class AbstractRelationship extends BaseObject implements
		Relationship {
    
    private static final long serialVersionUID = 1L;

	protected static Table checkSameTable(Column[] columns) {
		if (columns == null || columns.length == 0) {
			throw new IllegalArgumentException(
					"At least one key-column must exist on both "
							+ "primary and foreign side for "
							+ "a relation to exist.");
		}
		Table table = null;
		for (int i = 0; i < columns.length; i++) {
			Column column = columns[i];
			if (i == 0) {
				table = column.getTable();
			} else {
				if (table != column.getTable()) {
					throw new IllegalArgumentException(
							"Key-columns did not have same table");
				}
			}
		}
		return table;
	}

	@Override
	public Table getForeignTable() {
		return getForeignColumns()[0].getTable();
	}

	@Override
	public Table getPrimaryTable() {
		return getPrimaryColumns()[0].getTable();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Relationship[");
		sb.append("primaryTable=" + getPrimaryTable().getName());
		Column[] columns = getPrimaryColumns();
		sb.append(",primaryColumns=[");
		for (int i = 0; i < columns.length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns[i].getName());
		}
		sb.append("]");
		sb.append(",foreignTable=" + getForeignTable().getName());
		columns = getForeignColumns();
		sb.append(",foreignColumns=[");
		for (int i = 0; i < columns.length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns[i].getName());
		}
		sb.append("]");
		sb.append("]");
		return sb.toString();
	}

	public int compareTo(Relationship that) {
		return toString().compareTo(that.toString());
	}

	@Override
	protected final void decorateIdentity(List<Object> identifiers) {
		identifiers.add(getPrimaryColumns());
		identifiers.add(getForeignColumns());
	}

	@Override
	protected final boolean classEquals(BaseObject obj) {
		return obj instanceof Relationship;
	}

	@Override
	public boolean containsColumnPair(Column pkColumn, Column fkColumn) {
		if (pkColumn != null && fkColumn != null) {
			Column[] primaryColumns = getPrimaryColumns();
			Column[] foreignColumns = getForeignColumns();
			for (int i = 0; i < primaryColumns.length; i++) {
				if (pkColumn.equals(primaryColumns[i])
						&& fkColumn.equals(foreignColumns[i])) {
					return true;
				}
			}
		}
		return false;
	}
}