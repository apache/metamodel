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

import org.eobjects.metamodel.schema.Schema;

/**
 * A simple subclass of {@link QueryPostprocessDataContext} which provides less
 * implementation fuzz when custom querying features (like composite
 * datacontexts or type conversion) is needed.
 * 
 * @author Kasper Sørensen
 * @author Ankit Kumar
 */
public abstract class QueryPostprocessDelegate extends
		QueryPostprocessDataContext {

	@Override
	protected String getMainSchemaName() throws MetaModelException {
		throw new UnsupportedOperationException(
				"QueryPostprocessDelegate cannot perform schema exploration");
	}

	@Override
	protected Schema getMainSchema() throws MetaModelException {
		throw new UnsupportedOperationException(
				"QueryPostprocessDelegate cannot perform schema exploration");
	}
}
