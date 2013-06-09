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

package org.eobjects.metamodel.query;

import org.eobjects.metamodel.MetaModelTestCase;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class FromClauseTest extends MetaModelTestCase {

	public void testGetItemByReference() throws Exception {
		Schema exampleSchema = getExampleSchema();
		Table table = exampleSchema.getTableByName(TABLE_CONTRIBUTOR);

		Query query = new Query();
		query.from(table, "foobar");

		assertNull(query.getFromClause().getItemByReference("foob"));
		assertNull(query.getFromClause().getItemByReference(TABLE_CONTRIBUTOR));
		assertEquals("MetaModelSchema.contributor foobar", query
				.getFromClause().getItemByReference("foobar").toString());

		query = new Query();
		query.from(table);
		assertNull(query.getFromClause().getItemByReference("foob"));
		assertEquals("MetaModelSchema.contributor", query.getFromClause()
				.getItemByReference(TABLE_CONTRIBUTOR).toString());
		assertNull(query.getFromClause().getItemByReference("foobar"));
	}
}