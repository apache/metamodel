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
package org.apache.metamodel.schema;

import java.util.List;

import org.apache.metamodel.util.BaseObject;

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