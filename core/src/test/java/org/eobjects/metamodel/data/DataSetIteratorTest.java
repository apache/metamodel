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
package org.eobjects.metamodel.data;

import org.easymock.EasyMock;

import junit.framework.TestCase;

public class DataSetIteratorTest extends TestCase {

	public void testHasNextAndNextAndClose() throws Exception {
		DataSet ds = EasyMock.createMock(DataSet.class);
		Row row = EasyMock.createMock(Row.class);

		EasyMock.expect(ds.next()).andReturn(true);
		EasyMock.expect(ds.getRow()).andReturn(row);
		EasyMock.expect(ds.next()).andReturn(true);
		EasyMock.expect(ds.getRow()).andReturn(row);
		EasyMock.expect(ds.next()).andReturn(false);
		ds.close();

		EasyMock.replay(ds, row);

		DataSetIterator it = new DataSetIterator(ds);

		// multiple hasNext calls does not iterate before next is called
		assertTrue(it.hasNext());
		assertTrue(it.hasNext());
		assertTrue(it.hasNext());

		assertSame(row, it.next());

		assertTrue(it.hasNext());
		assertTrue(it.hasNext());

		assertSame(row, it.next());
		assertFalse(it.hasNext());
		assertFalse(it.hasNext());
		assertFalse(it.hasNext());

		assertNull(it.next());

		EasyMock.verify(ds, row);
	}

	public void testRemove() throws Exception {
		DataSet ds = EasyMock.createMock(DataSet.class);
		DataSetIterator it = new DataSetIterator(ds);

		try {
			it.remove();
			fail("Exception expected");
		} catch (UnsupportedOperationException e) {
			assertEquals("DataSet is read-only, remove() is not supported.",
					e.getMessage());
		}
	}
}
