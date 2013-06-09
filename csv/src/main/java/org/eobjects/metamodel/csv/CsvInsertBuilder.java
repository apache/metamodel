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

import org.eobjects.metamodel.insert.AbstractRowInsertionBuilder;
import org.eobjects.metamodel.schema.Table;

final class CsvInsertBuilder extends AbstractRowInsertionBuilder<CsvUpdateCallback> {

	public CsvInsertBuilder(CsvUpdateCallback updateCallback, Table table) {
		super(updateCallback, table);
	}

	@Override
	public void execute() {
		Object[] values = getValues();
		String[] stringValues = new String[values.length];
		for (int i = 0; i < stringValues.length; i++) {
			stringValues[i] = values[i] == null ? "" : values[i].toString();
		}
		getUpdateCallback().writeRow(stringValues, true);
	}

}
