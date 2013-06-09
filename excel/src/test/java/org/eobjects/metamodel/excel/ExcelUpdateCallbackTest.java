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
package org.eobjects.metamodel.excel;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.schema.Table;

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

			ExcelUtils.writeWorkbook(dc, sheet.getWorkbook());
		}

		assertTrue("Usually the file size will be circa 42000, but it was: "
				+ file.length(), file.length() > 40000 && file.length() < 45000);

		// read to check results
		{
			ExcelDataContext dc = new ExcelDataContext(file);
			assertEquals("[foobar]",
					Arrays.toString(dc.getDefaultSchema().getTableNames()));

			Table table = dc.getDefaultSchema().getTableByName("foobar");

			assertEquals("[value0]", Arrays.toString(table.getColumnNames()));

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
