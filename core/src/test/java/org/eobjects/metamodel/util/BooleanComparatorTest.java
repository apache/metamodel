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

import junit.framework.TestCase;

public class BooleanComparatorTest extends TestCase {

	public void testCompare() throws Exception {
		Comparator<Object> c = BooleanComparator.getComparator();
		assertEquals(1, c.compare(true, false));
		assertEquals(-1, c.compare(false, true));
		assertEquals(0, c.compare(true, true));
		assertEquals(0, c.compare(false, false));

		assertEquals(1, c.compare("true", "false"));
		assertEquals(1, c.compare("1", "false"));
		assertEquals(1, c.compare("true", "0"));
		assertEquals(1, c.compare("true", "false"));

		assertEquals(1, c.compare(1, 0));

		assertEquals(1, c.compare(1, "false"));
		assertEquals(1, c.compare("yes", false));
		assertEquals(1, c.compare("y", false));
		assertEquals(1, c.compare("TRUE", false));
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = BooleanComparator.getComparable(true);
		assertEquals(1, comparable.compareTo(false));
		assertEquals(1, comparable.compareTo(0));
		assertEquals(1, comparable.compareTo("false"));
	}
}