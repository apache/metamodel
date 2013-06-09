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
package org.eobjects.metamodel;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.Func;

final class CompositeQueryDelegate extends QueryPostprocessDelegate {

	private final Func<Table, DataContext> _dataContextRetrievalFunction;

	public CompositeQueryDelegate(
			Func<Table, DataContext> dataContextRetrievalFunction) {
		_dataContextRetrievalFunction = dataContextRetrievalFunction;
	}

	@Override
	protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
			int maxRows) {
		// find the appropriate datacontext to execute a simple
		// table materialization query
		DataContext dc = _dataContextRetrievalFunction.eval(table);
		Query q = new Query().select(columns).from(table);
		if (maxRows >= 0) {
			q.setMaxRows(maxRows);
		}
		return dc.executeQuery(q);
	}

}
