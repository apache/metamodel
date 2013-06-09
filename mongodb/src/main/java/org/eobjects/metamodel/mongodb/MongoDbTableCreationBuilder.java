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
package org.eobjects.metamodel.mongodb;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.create.AbstractTableCreationBuilder;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.ImmutableColumn;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

final class MongoDbTableCreationBuilder extends
		AbstractTableCreationBuilder<MongoDbUpdateCallback> implements
		TableCreationBuilder {

	public MongoDbTableCreationBuilder(MongoDbUpdateCallback updateCallback,
			Schema schema, String name) {
		super(updateCallback, schema, name);
	}

	@Override
	public Table execute() throws MetaModelException {
		final MongoDbDataContext dataContext = getUpdateCallback()
				.getDataContext();
		final Schema schema = dataContext.getDefaultSchema();
		final MutableTable table = getTable();
		if (table.getColumnByName("_id") == null) {
			// all mongo db collections have an _id field as the first field.
			ImmutableColumn idColumn = new ImmutableColumn("_id",
					ColumnType.ROWID, table, table.getColumnCount(), null,
					null, null, null, true, null, true);
			table.addColumn(idColumn);
		}
		table.setSchema(schema);
		getUpdateCallback().createCollection(table.getName());
		dataContext.addTable(table);
		return table;
	}
}
