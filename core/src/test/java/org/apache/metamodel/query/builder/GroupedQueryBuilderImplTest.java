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
package org.apache.metamodel.query.builder;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.apache.metamodel.AbstractDataContext;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class GroupedQueryBuilderImplTest extends TestCase {

	public void testFindColumnWithAlias() throws Exception {
		DataContext dataContext = EasyMock.createMock(DataContext.class);

		MutableTable table1 = new MutableTable("foo");
		table1.addColumn(new MutableColumn("col1").setTable(table1));
		table1.addColumn(new MutableColumn("col2").setTable(table1));
		table1.addColumn(new MutableColumn("col3").setTable(table1));

		MutableTable table2 = new MutableTable("bar");
		table2.addColumn(new MutableColumn("col1").setTable(table2));
		table2.addColumn(new MutableColumn("col2").setTable(table2));
		table2.addColumn(new MutableColumn("col3").setTable(table2));

		Query query = new Query().from(table1, "f").from(table2, "b");
		GroupedQueryBuilderImpl gqbi = new GroupedQueryBuilderImpl(dataContext,
				query);

		Column col = gqbi.findColumn("b.col2");
		assertEquals("bar.col2", col.getQualifiedLabel());

		col = gqbi.findColumn("f.col2");
		assertEquals("foo.col2", col.getQualifiedLabel());

		try {
			col = gqbi.findColumn("f.col4");
			fail("Exception expected");
		} catch (IllegalArgumentException e) {
			assertEquals("Could not find column: f.col4", e.getMessage());
		}
	}

	// test-case to recreate the problems reported at
	// http://eobjects.org/trac/discussion/7/134
	public void testLeftJoinQueries() throws Exception {
		DataContext dc = new AbstractDataContext() {

			@Override
			public DataSet executeQuery(Query query) throws MetaModelException {
				throw new UnsupportedOperationException();
			}

			@Override
			protected String[] getSchemaNamesInternal() {
				throw new UnsupportedOperationException();
			}

			@Override
			protected String getDefaultSchemaName() {
				throw new UnsupportedOperationException();
			}

			@Override
			protected Schema getSchemaByNameInternal(String name) {
				throw new UnsupportedOperationException();
			}
		};
		Table tableAB = new MutableTable("tableAB");
		Table tableC = new MutableTable("tableC");

		Column colA = new MutableColumn("colA", null, tableAB, 0, true);
		Column colB = new MutableColumn("colB", null, tableAB, 1, true);
		Column colC = new MutableColumn("colC", null, tableC, 0, true);

		Query q = dc.query().from(tableAB).leftJoin(tableC).on(colB, colC)
				.select(colA).as("a").select(colB).as("b").select(colC).as("c")
				.toQuery();

		assertEquals(
				"SELECT tableAB.colA AS a, tableAB.colB AS b, tableC.colC AS c FROM tableAB LEFT JOIN tableC ON tableAB.colB = tableC.colC",
				q.toSql());
	}
}