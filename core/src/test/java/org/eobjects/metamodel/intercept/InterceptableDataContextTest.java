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
package org.eobjects.metamodel.intercept;

import java.util.Arrays;

import junit.framework.TestCase;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MockUpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.MaxRowsDataSet;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class InterceptableDataContextTest extends TestCase {

	private final MockUpdateableDataContext delegateDataContext = new MockUpdateableDataContext();
	private final Table table = delegateDataContext.getDefaultSchema()
			.getTables()[0];

	public void testInterceptSchema() throws Exception {
		// without an interceptor
		{
			DataContext dc = new InterceptableDataContext(delegateDataContext);

			Schema schema = dc.getDefaultSchema();
			Schema[] schemas = dc.getSchemas();

			assertEquals("schema", schema.getName());
			assertEquals(MutableSchema.class, schema.getClass());
			assertEquals("[information_schema, schema]",
					Arrays.toString(dc.getSchemaNames()));
			assertEquals(2, schemas.length);
			assertEquals("information_schema", schemas[0].getName());
			assertEquals("schema", schemas[1].getName());
		}

		// with an interceptor
		{
			DataContext dc = new InterceptableDataContext(delegateDataContext)
					.addSchemaInterceptor(new SchemaInterceptor() {
						@Override
						public Schema intercept(Schema input) {
							return new MutableSchema(input.getName() + " foo!");
						}
					});

			Schema schema = dc.getDefaultSchema();
			Schema[] schemas = dc.getSchemas();

			assertEquals("schema foo!", schema.getName());
			assertEquals(MutableSchema.class, schema.getClass());
			assertEquals("[information_schema foo!, schema foo!]",
					Arrays.toString(dc.getSchemaNames()));
			assertEquals(2, schemas.length);
			assertEquals("information_schema foo!", schemas[0].getName());
			assertEquals("schema foo!", schemas[1].getName());
		}
	}

	public void testInterceptDataSet() throws Exception {
		DataContext dc = new InterceptableDataContext(delegateDataContext)
				.addDataSetInterceptor(new DataSetInterceptor() {
					@Override
					public DataSet intercept(DataSet dataSet) {
						return new MaxRowsDataSet(dataSet, 1);
					}
				});

		DataSet ds = dc.query().from(table).select("foo").execute();
		assertEquals(MaxRowsDataSet.class, ds.getClass());
		assertEquals(1, ds.toObjectArrays().size());
	}

	public void testInterceptQuery() throws Exception {

		DataContext dc = new InterceptableDataContext(delegateDataContext)
				.addQueryInterceptor(new QueryInterceptor() {
					@Override
					public Query intercept(Query input) {
						return input.select(table.getColumnByName("foo"));
					}
				}).addQueryInterceptor(new QueryInterceptor() {
					@Override
					public Query intercept(Query input) {
						return input.select(table.getColumnByName("bar"));

					}
				});

		DataSet ds = dc.executeQuery(new Query().from(table));
		assertEquals("[table.foo, table.bar]", Arrays.toString(ds.getSelectItems()));
	}
}
