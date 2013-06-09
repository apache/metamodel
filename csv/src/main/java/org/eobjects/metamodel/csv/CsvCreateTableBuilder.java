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
