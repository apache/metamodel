/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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