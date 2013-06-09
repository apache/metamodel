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

import java.io.FileInputStream;

import junit.framework.TestCase;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.easymock.EasyMock;
import org.eobjects.metamodel.excel.ZeroBasedRowIterator;

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
