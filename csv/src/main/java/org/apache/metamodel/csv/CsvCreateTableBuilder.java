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
package org.eobjects.metamodel.csv;

import org.eobjects.metamodel.create.AbstractTableCreationBuilder;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

final class CsvCreateTableBuilder extends
		AbstractTableCreationBuilder<CsvUpdateCallback> {

	public CsvCreateTableBuilder(CsvUpdateCallback updateCallback,
			Schema schema, String name) {
		super(updateCallback, schema, name);
		if (!(schema instanceof CsvSchema)) {
			throw new IllegalArgumentException("Not a valid CSV schema: "
					+ schema);
		}
	}

	@Override
	public Table execute() {
		CsvUpdateCallback csvUpdateCallback = getUpdateCallback();

		MutableTable table = getTable();
		String[] columnNames = table.getColumnNames();
		csvUpdateCallback.writeRow(columnNames, false);

		CsvSchema schema = (CsvSchema) table.getSchema();
		CsvTable csvTable = new CsvTable(schema, table.getColumnNames());
		schema.setTable(csvTable);
		return csvTable;
	}
}
