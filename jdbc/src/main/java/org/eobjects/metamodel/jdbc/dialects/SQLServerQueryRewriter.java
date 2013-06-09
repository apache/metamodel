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
package org.eobjects.metamodel.jdbc.dialects;

import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectClause;

public class SQLServerQueryRewriter extends DefaultQueryRewriter {

	public SQLServerQueryRewriter(JdbcDataContext dataContext) {
		super(dataContext);
	}

	@Override
	public boolean isMaxRowsSupported() {
		return true;
	}

	/**
	 * SQL server expects the fully qualified column name, including schema, in
	 * select items.
	 */
	@Override
	public boolean isSchemaIncludedInColumnPaths() {
		return true;
	}

	@Override
	protected String rewriteSelectClause(Query query, SelectClause selectClause) {
		String result = super.rewriteSelectClause(query, selectClause);

		Integer maxRows = query.getMaxRows();
		if (maxRows != null) {
			result = "SELECT TOP " + maxRows + " " + result.substring(7);
		}

		return result;
	}
}