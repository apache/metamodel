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
package org.eobjects.metamodel.csv;

import java.io.File;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.csv.CsvDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Table;

import junit.framework.TestCase;

public class CsvBigFileMemoryTest extends TestCase {

	private final int hugeFileRows = 3000;
	private final int hugeFileCols = 2000;

	private File getHugeFile() {
		final File file = new File("target/huge_csv.csv");
		if (!file.exists()) {

			final ExampleDataGenerator exampleDataGenerator = new ExampleDataGenerator(
					hugeFileRows, hugeFileCols);
			exampleDataGenerator.createFile(file);
		}
		return file;
	}

	/**
	 * Runs a performance test based on the data created by the
	 * ExampleDataCreator utility.
	 * 
	 * @see ExampleDataGenerator
	 * @throws Exception
	 */
	public void testHugeFile() throws Exception {
		final File file = getHugeFile();

		final long timeAtStart = System.currentTimeMillis();
		System.out.println("time at start: " + timeAtStart);

		final DataContext dc = new CsvDataContext(file, new CsvConfiguration());
		final Table t = dc.getDefaultSchema().getTables()[0];

		final long timeAfterDataContext = System.currentTimeMillis();
		System.out.println("time after DataContext: " + timeAfterDataContext);

		final Query q = new Query().select(t.getColumns()).from(t);
		DataSet ds = dc.executeQuery(q);

		long timeAfterQuery = System.currentTimeMillis();
		System.out.println("time after query: " + timeAfterQuery);

		while (ds.next()) {
			assertEquals(hugeFileCols, ds.getRow().getValues().length);
		}
		ds.close();

		long timeAfterDataSet = System.currentTimeMillis();
		System.out.println("time after dataSet: " + timeAfterDataSet);

		if (!file.delete()) {
			file.deleteOnExit();
		}
	}

	public void testApproximatedCountHugeFile() throws Exception {
		DataContext dc = new CsvDataContext(getHugeFile());

		Table table = dc.getDefaultSchema().getTables()[0];
		Query q = dc.query().from(table).selectCount().toQuery();
		SelectItem selectItem = q.getSelectClause().getItem(0);
		selectItem.setFunctionApproximationAllowed(true);

		DataSet ds = dc.executeQuery(q);
		assertTrue(ds.next());
		Object[] values = ds.getRow().getValues();
		assertEquals(1, values.length);
		assertEquals(3332, ((Long) ds.getRow().getValue(selectItem)).intValue());
		assertEquals(3332, ((Long) values[0]).intValue());
		assertFalse(ds.next());
	}
}
