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
package org.eobjects.metamodel.couchdb;

import java.util.HashMap;
import java.util.Map;

import org.ektorp.CouchDbConnector;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.AbstractRowUpdationBuilder;

final class CouchDbRowUpdationBuilder extends AbstractRowUpdationBuilder {

	private final CouchDbUpdateCallback _updateCallback;

	public CouchDbRowUpdationBuilder(CouchDbUpdateCallback updateCallback, Table table) {
		super(table);
		_updateCallback = updateCallback;
	}

	@Override
	public void execute() throws MetaModelException {
		final Table table = getTable();
		final CouchDbConnector connector = _updateCallback.getConnector(table.getName());

		// create a map which will act as a prototype for updated objects
		final Map<String, Object> prototype = new HashMap<String, Object>();
		final Column[] columns = getColumns();
		final Object[] values = getValues();
		for (int i = 0; i < columns.length; i++) {
			final Column column = columns[i];
			if (isSet(column)) {
				final String columnName = column.getName();
				final Object value = values[i];
				prototype.put(columnName, value);
			}
		}

		final CouchDbDataContext dc = _updateCallback.getDataContext();
		final DataSet dataSet = dc.query().from(table).select(table.getColumns()).where(getWhereItems()).execute();
		try {
			while (dataSet.next()) {
				final Map<String, Object> map = new HashMap<String, Object>(prototype);
				final Row row = dataSet.getRow();
				for (Column column : table.getColumns()) {
					if (!map.containsKey(column.getName())) {
						map.put(column.getName(), row.getValue(column));
					}
				}

				// copy the prototype and set the not-updated values
				connector.update(map);
			}
		} finally {
			dataSet.close();
		}
	}

}
