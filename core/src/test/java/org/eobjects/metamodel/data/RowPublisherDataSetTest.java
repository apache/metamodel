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

import junit.framework.TestCase;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.util.Action;

public class RowPublisherDataSetTest extends TestCase {

	public void testMaxSize() throws Exception {
		SelectItem[] selectItems = new SelectItem[2];
		selectItems[0] = new SelectItem(new MutableColumn("foos"));
		selectItems[1] = new SelectItem(new MutableColumn("bars"));
		DataSet ds = new RowPublisherDataSet(selectItems, 5,
				new Action<RowPublisher>() {
					@Override
					public void run(RowPublisher publisher) throws Exception {

						// we want to exceed the buffer size
						int iterations = RowPublisherImpl.BUFFER_SIZE * 2;

						for (int i = 0; i < iterations; i++) {
							publisher.publish(new Object[] { "foo" + i,
									"bar" + i });
						}
					}
				});

		assertTrue(ds.next());
		assertEquals("Row[values=[foo0, bar0]]", ds.getRow().toString());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertEquals("Row[values=[foo4, bar4]]", ds.getRow().toString());
		assertFalse(ds.next());
	}

	public void testExceptionInAction() throws Exception {
		SelectItem[] selectItems = new SelectItem[2];
		selectItems[0] = new SelectItem(new MutableColumn("foos"));
		selectItems[1] = new SelectItem(new MutableColumn("bars"));
		DataSet ds = new RowPublisherDataSet(selectItems, 5,
				new Action<RowPublisher>() {
					@Override
					public void run(RowPublisher publisher) throws Exception {
						publisher.publish(new Object[] { "foo0", "bar0" });
						publisher.publish(new Object[] { "foo1", "bar1" });
						throw new IllegalStateException("foobar!");
					}
				});

		assertTrue(ds.next());
		assertEquals("Row[values=[foo0, bar0]]", ds.getRow().toString());
		assertTrue(ds.next());
		assertEquals("Row[values=[foo1, bar1]]", ds.getRow().toString());

		try {
			ds.next();
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals("foobar!", e.getMessage());
			assertEquals(IllegalStateException.class, e.getClass());
		}

	}
}
