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
import org.eobjects.metamodel.util.ToStringComparator;

public class ToStringComparatorTest extends TestCase {

	private Comparator<Object> comparator = ToStringComparator.getComparator();

	public void testNotNull() throws Exception {
		assertEquals(4, comparator.compare("foo", "bar"));
		assertEquals(-4, comparator.compare("bar", "foo"));
	}

	public void testNull() throws Exception {
		int result = comparator.compare(null, null);
		assertEquals(-1, result);

		result = comparator.compare(1, null);
		assertEquals(1, result);

		result = comparator.compare(null, 1);
		assertEquals(-1, result);
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = ToStringComparator
				.getComparable("aaaa");
		assertEquals(-1, comparable.compareTo("bbbb"));
	}
}