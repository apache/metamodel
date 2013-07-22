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
package org.apache.metamodel.dialects;

import java.sql.Types;

import junit.framework.TestCase;

import org.apache.metamodel.jdbc.dialects.PostgresqlQueryRewriter;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;

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