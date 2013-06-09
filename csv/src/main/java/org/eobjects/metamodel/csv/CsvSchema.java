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

import org.eobjects.metamodel.schema.AbstractSchema;
import org.eobjects.metamodel.schema.Table;

final class CsvSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private final String _name;
	private final transient CsvDataContext _dataContext;
	private CsvTable _table;

	public CsvSchema(String name, CsvDataContext dataContext) {
		super();
		_name = name;
		_dataContext = dataContext;
	}

	protected void setTable(CsvTable table) {
		_table = table;
	}

	@Override
	public String getName() {
		return _name;
	}

	protected CsvDataContext getDataContext() {
		return _dataContext;
	}

	@Override
	public String getQuote() {
		return null;
	}

	@Override
	public Table[] getTables() {
		if (_table == null) {
			return new Table[0];
		}
		return new Table[] { _table };
	}
}
