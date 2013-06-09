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
package org.eobjects.metamodel.query.builder;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.eobjects.metamodel.AbstractDataContext;
import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

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