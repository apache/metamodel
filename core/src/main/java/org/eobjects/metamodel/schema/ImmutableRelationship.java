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

import java.io.Serializable;

public final class ImmutableRelationship extends AbstractRelationship implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Column[] primaryColumns;
	private final Column[] foreignColumns;

	public static void create(Relationship origRelationship,
			ImmutableSchema schema) {
		ImmutableTable primaryTable = getSimilarTable(
				origRelationship.getPrimaryTable(), schema);
		assert primaryTable != null;
		Column[] primaryColumns = getSimilarColumns(
				origRelationship.getPrimaryColumns(), primaryTable);
		checkSameTable(primaryColumns);

		ImmutableTable foreignTable = getSimilarTable(
				origRelationship.getForeignTable(), schema);
		assert foreignTable != null;
		Column[] foreignColumns = getSimilarColumns(
				origRelationship.getForeignColumns(), foreignTable);
		checkSameTable(foreignColumns);

		ImmutableRelationship relationship = new ImmutableRelationship(
				primaryColumns, foreignColumns);
		primaryTable.addRelationship(relationship);
		foreignTable.addRelationship(relationship);
	}

	private static Column[] getSimilarColumns(Column[] columns, Table table) {
		Column[] result = new Column[columns.length];
		for (int i = 0; i < columns.length; i++) {
			String name = columns[i].getName();
			result[i] = table.getColumnByName(name);
		}
		return result;
	}

	private static ImmutableTable getSimilarTable(Table table,
			ImmutableSchema schema) {
		String name = table.getName();
		return (ImmutableTable) schema.getTableByName(name);
	}

	private ImmutableRelationship(Column[] primaryColumns,
			Column[] foreignColumns) {
		this.primaryColumns = primaryColumns;
		this.foreignColumns = foreignColumns;
	}

	@Override
	public Column[] getPrimaryColumns() {
		return primaryColumns;
	}

	@Override
	public Column[] getForeignColumns() {
		return foreignColumns;
	}
}
