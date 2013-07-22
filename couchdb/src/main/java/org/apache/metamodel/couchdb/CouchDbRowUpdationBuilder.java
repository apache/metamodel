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
package org.apache.metamodel.couchdb;

import java.util.HashMap;
import java.util.Map;

import org.ektorp.CouchDbConnector;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.AbstractRowUpdationBuilder;

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
