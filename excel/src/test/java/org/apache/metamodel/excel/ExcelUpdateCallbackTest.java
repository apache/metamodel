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
package org.apache.metamodel.excel;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;

public class ExcelUpdateCallbackTest extends TestCase {

	public void testStreamingAPI() throws Exception {
		File file = new File("target/streaming-api-test.xlsx");
		if (file.exists()) {
			file.delete();
		}

		assertFalse(file.exists());

		// write using streaming writer
		{
			ExcelDataContext dc = new ExcelDataContext(file);
			ExcelUpdateCallback callback = new ExcelUpdateCallback(dc);

			SXSSFSheet sheet = (SXSSFSheet) callback.createSheet("foobar");

			Field windowSizeField = SXSSFSheet.class
					.getDeclaredField("_randomAccessWindowSize");
			windowSizeField.setAccessible(true);
			int windowSize = windowSizeField.getInt(sheet);
			assertEquals(1000, windowSize);

			Field rowsField = SXSSFSheet.class.getDeclaredField("_rows");
			rowsField.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<Integer, SXSSFRow> rows = (Map<Integer, SXSSFRow>) rowsField
					.get(sheet);
			assertEquals(0, rows.size());

			// create 5x the amound of rows as the streaming sheet will hold in
			// memory
			for (int i = 0; i < windowSize * 5; i++) {
				Row row = sheet.createRow(i);
				Cell cell = row.createCell(0);
				cell.setCellValue("value" + i);

				assertTrue(rows.size() <= 1000);
			}

			assertEquals(1000, rows.size());

			ExcelUtils.writeAndCloseWorkbook(dc, sheet.getWorkbook());
		}

		assertTrue("Usually the file size will be circa 42000, but it was: "
				+ file.length(), file.length() > 40000 && file.length() < 45000);

		// read to check results
		{
			ExcelDataContext dc = new ExcelDataContext(file);
			assertEquals("[foobar]",
					Arrays.toString(dc.getDefaultSchema().getTableNames().toArray()));

			Table table = dc.getDefaultSchema().getTableByName("foobar");

			assertEquals("[value0]", Arrays.toString(table.getColumnNames().toArray()));

			DataSet ds = dc.query().from(table).select("value0").execute();
			int recordNo = 1;
			while (ds.next()) {
				assertEquals("value" + recordNo, ds.getRow().getValue(0));
				recordNo++;
			}

			assertEquals(5000, recordNo);
		}
	}
}
