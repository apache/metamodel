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

import java.sql.Types;

import junit.framework.TestCase;

import org.eobjects.metamodel.jdbc.dialects.PostgresqlQueryRewriter;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;

public class PostgresqlQueryRewriterTest extends TestCase {

	public void testRewriteLimit() throws Exception {
		MutableTable table = new MutableTable("foo");
		table.setSchema(new MutableSchema("MY_SCHEMA"));
		table.setQuote("\"");
		MutableColumn column = new MutableColumn("bar");
		column.setQuote("\"");
		column.setTable(table);
		Query q = new Query().from(table).select(column).setMaxRows(25).setFirstRow(5);
		String queryString = new PostgresqlQueryRewriter(null).rewriteQuery(q);
		assertEquals("SELECT \"foo\".\"bar\" FROM \"MY_SCHEMA\".\"foo\" LIMIT 25 OFFSET 4", queryString);
	}

	public void testRewriteFromItem() throws Exception {
		PostgresqlQueryRewriter rewriter = new PostgresqlQueryRewriter(null);

		assertEquals("\"public\".foo",
				rewriter.rewriteFromItem(new FromItem(new MutableTable("foo").setSchema(new MutableSchema("public")))));
	}

	public void testGetColumnType() throws Exception {
		PostgresqlQueryRewriter rewriter = new PostgresqlQueryRewriter(null);
		assertEquals(ColumnType.BOOLEAN, rewriter.getColumnType(Types.BIT, "bool", -1));
	}
}