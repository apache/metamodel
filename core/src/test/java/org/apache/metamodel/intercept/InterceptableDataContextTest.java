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
package org.apache.metamodel.intercept;

import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MockUpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

import junit.framework.TestCase;

public class InterceptableDataContextTest extends TestCase {

	private final MockUpdateableDataContext delegateDataContext = new MockUpdateableDataContext();
	private final Table table = delegateDataContext.getDefaultSchema()
			.getTables().get(0);

	public void testInterceptSchema() throws Exception {
		// without an interceptor
		{
			DataContext dc = new InterceptableDataContext(delegateDataContext);

			Schema schema = dc.getDefaultSchema();
			List<Schema> schemas = dc.getSchemas();

			assertEquals("schema", schema.getName());
			assertEquals(MutableSchema.class, schema.getClass());
			assertEquals("[information_schema, schema]",
					Arrays.toString(dc.getSchemaNames().toArray()));
			assertEquals(2, schemas.size());
			assertEquals("information_schema", schemas.get(0).getName());
			assertEquals("schema", schemas.get(1).getName());
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
			List<Schema> schemas = dc.getSchemas();

			assertEquals("schema foo!", schema.getName());
			assertEquals(MutableSchema.class, schema.getClass());
			assertEquals("[information_schema foo!, schema foo!]",
					Arrays.toString(dc.getSchemaNames().toArray()));
			assertEquals(2, schemas.size());
			assertEquals("information_schema foo!", schemas.get(0).getName());
			assertEquals("schema foo!", schemas.get(1).getName());
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
		assertEquals("[table.foo, table.bar]", Arrays.toString(ds.getSelectItems().toArray()));
	}
}
