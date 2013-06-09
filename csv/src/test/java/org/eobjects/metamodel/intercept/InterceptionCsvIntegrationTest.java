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

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.csv.CsvDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Table;

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

		assertEquals("[test_interception_scenario]",
				Arrays.toString(dc.getDefaultSchema().getTableNames()));
		Table table = dc.getDefaultSchema().getTables()[0];
		assertEquals("[col1, col2, foobar]",
				Arrays.toString(table.getColumnNames()));

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
