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

package org.eobjects.metamodel.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import junit.framework.TestCase;

public class ObjectComparatorTest extends TestCase {

	public void testString() throws Exception {
		Comparator<Object> c = ObjectComparator.getComparator();
		assertTrue(c.compare("aaaa", "bbbb") < 0);

		assertTrue(c.compare("w", "y") < 0);
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = ObjectComparator.getComparable("aaaa");
		assertEquals(-1, comparable.compareTo("bbbb"));
	}

	public void testNull() throws Exception {
		Comparator<Object> comparator = ObjectComparator.getComparator();
		assertEquals(0, comparator.compare(null, null));
		assertEquals(1, comparator.compare("h", null));
		assertEquals(-1, comparator.compare(null, "h"));

		TreeSet<Object> set = new TreeSet<Object>(comparator);
		set.add("Hello");
		set.add(null);
		set.add(null);
		set.add(DateUtils.get(2010, Month.SEPTEMBER, 27));
		set.add(DateUtils.get(2010, Month.SEPTEMBER, 28));
		set.add(DateUtils.get(2010, Month.SEPTEMBER, 26));

		assertEquals(5, set.size());
		Iterator<Object> it = set.iterator();
		assertEquals(null, it.next());
		assertEquals("Hello", it.next());
		assertEquals(DateUtils.get(2010, Month.SEPTEMBER, 26), it.next());
		assertEquals(DateUtils.get(2010, Month.SEPTEMBER, 27), it.next());
		assertEquals(DateUtils.get(2010, Month.SEPTEMBER, 28), it.next());
	}
}