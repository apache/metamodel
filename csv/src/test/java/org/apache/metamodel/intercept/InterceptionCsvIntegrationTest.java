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

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Table;

public class InterceptionCsvIntegrationTest extends TestCase {

	public void testScenario() throws Exception {
		final UpdateableDataContext source = new CsvDataContext(new File(
				"target/test_interception_scenario.txt"));
		final InterceptableDataContext dc = Interceptors.intercept(source);

		dc.addTableCreationInterceptor(new TableCreationInterceptor() {
			@Override
			public TableCreationBuilder intercept(TableCreationBuilder input) {
				return input.withColumn("foobar");
			}
		});

		dc.addRowInsertionInterceptor(new RowInsertionInterceptor() {
			@Override
			public RowInsertionBuilder intercept(RowInsertionBuilder input) {
				return input.value("foobar", "elite!");
			}
		});

		dc.executeUpdate(new UpdateScript() {
			@Override
			public void run(UpdateCallback callback) {
				Table table = callback
						.createTable(dc.getDefaultSchema(), "table")
						.withColumn("col1").withColumn("col2").execute();

				callback.insertInto(table).value("col1", "hello")
						.value("col2", "world").execute();
				callback.insertInto(table).value("col1", "123")
						.value("col2", "567").execute();
			}
		});

		assertEquals("[table]",
				Arrays.toString(dc.getDefaultSchema().getTableNames().toArray()));
		Table table = dc.getDefaultSchema().getTables().get(0);
		assertEquals("[col1, col2, foobar]",
				Arrays.toString(table.getColumnNames().toArray()));

		DataSet ds = dc.query().from(table).select(table.getColumns())
				.execute();
		assertTrue(ds.next());
		assertEquals("Row[values=[hello, world, elite!]]", ds.getRow()
				.toString());
		assertTrue(ds.next());
		assertEquals("Row[values=[123, 567, elite!]]", ds.getRow().toString());
		assertFalse(ds.next());
	}
}
