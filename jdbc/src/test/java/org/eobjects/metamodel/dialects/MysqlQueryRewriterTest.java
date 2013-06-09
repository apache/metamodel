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
package org.eobjects.metamodel.dialects;

import junit.framework.TestCase;

import org.eobjects.metamodel.jdbc.dialects.MysqlQueryRewriter;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;

public class MysqlQueryRewriterTest extends TestCase {

	public void testRewriteLimit() throws Exception {
		Query q = new Query().from(new MutableTable("foo"))
				.select(new MutableColumn("bar")).setMaxRows(25).setFirstRow(6);
		String queryString = new MysqlQueryRewriter(null).rewriteQuery(q);
		assertEquals("SELECT bar FROM foo LIMIT 25 OFFSET 5", queryString);
	}

	public void testRewriteFilterOperandQuote() throws Exception {
		MutableColumn col = new MutableColumn("bar");
		Query q = new Query().from(new MutableTable("foo")).select(col)
				.where(col, OperatorType.EQUALS_TO, "M'jellow strain'ger");
		String queryString = new MysqlQueryRewriter(null).rewriteQuery(q);
		assertEquals("SELECT bar FROM foo WHERE bar = 'M\\'jellow strain\\'ger'",
				queryString);
	}
}