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

import java.io.FileInputStream;

import junit.framework.TestCase;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.easymock.EasyMock;
import org.apache.metamodel.excel.ZeroBasedRowIterator;

public class ZeroBasedRowIteratorTest extends TestCase {

	public void testHasNext() throws Exception {
		Workbook workbook = WorkbookFactory.create(new FileInputStream(
				"src/test/resources/xls_single_cell_sheet.xls"));
		Sheet sheet = workbook.getSheetAt(0);

		// POI's row numbers are 0-based also - the last cell in the sheet is
		// actually A6.
		assertEquals(5, sheet.getLastRowNum());

		ZeroBasedRowIterator it = new ZeroBasedRowIterator(sheet);

		assertTrue(it.hasNext());
		assertNull(it.next());

		assertTrue(it.hasNext());
		assertNull(it.next());

		assertTrue(it.hasNext());
		assertNull(it.next());

		assertTrue(it.hasNext());
		assertNull(it.next());

		assertTrue(it.hasNext());
		assertNull(it.next());

		assertTrue(it.hasNext());
		assertNotNull(it.next());

		assertFalse(it.hasNext());
	}
	
	public void testUnsupportedRemove() throws Exception {
		ZeroBasedRowIterator it = new ZeroBasedRowIterator(EasyMock.createMock(Sheet.class));
		
		try {
			it.remove();
			fail("Exception expected");
		} catch (UnsupportedOperationException e) {
			assertEquals("remove() is not supported", e.getMessage());
		}
	}
}
