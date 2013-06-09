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

import org.eobjects.metamodel.jdbc.dialects.IQueryRewriter;
import org.eobjects.metamodel.jdbc.dialects.SQLServerQueryRewriter;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;

public class SQLServerQueryRewriterTest extends TestCase {

	private MutableTable table;
	private MutableColumn column;
	private IQueryRewriter qr = new SQLServerQueryRewriter(null);

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		table = new MutableTable("foo");
		table.setSchema(new MutableSchema("MY_SCHEMA"));
		table.setQuote("\"");
		column = new MutableColumn("bar");
		column.setQuote("\"");
		column.setTable(table);
	}

	public void testRewriteFromItem() throws Exception {
		assertEquals("foo",
				qr.rewriteFromItem(new FromItem(new MutableTable("foo"))));
	}

	public void testAliasing() throws Exception {
		Query q = new Query().from(table).select(column);

		assertEquals("SELECT MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\"",
				qr.rewriteQuery(q));
	}

	public void testSelectMaxRowsRewriting() throws Exception {
		Query q = new Query().from(table).select(column).setMaxRows(20);

		assertEquals(
				"SELECT TOP 20 MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\"",
				qr.rewriteQuery(q));
	}
}