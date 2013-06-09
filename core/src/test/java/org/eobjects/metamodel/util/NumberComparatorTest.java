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

public class NumberComparatorTest extends TestCase {

	public void testDoubleAndIntegerComparison() throws Exception {
		Comparator<Object> comparator = NumberComparator.getComparator();
		assertEquals(0, comparator.compare(1, 1.0));
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = NumberComparator.getComparable("125");
		assertEquals(0, comparable.compareTo(125));
		assertEquals(-1, comparable.compareTo(126));
	}
}