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

import java.util.List;

import junit.framework.TestCase;

public class BaseObjectTest extends TestCase {

	class MyClass extends BaseObject {
		private int[] ints;

		@Override
		protected void decorateIdentity(List<Object> identifiers) {
			identifiers.add(ints);
		}
	}

	public void testHashCodeForPrimitiveArray() throws Exception {
		MyClass o1 = new MyClass();
		o1.ints = new int[] { 1, 2, 3 };
		MyClass o2 = new MyClass();
		o2.ints = new int[] { 4, 5, 6 };
		MyClass o3 = new MyClass();
		o3.ints = new int[] { 1, 2, 3 };

		assertTrue(o1.hashCode() == o1.hashCode());
		assertTrue(o1.hashCode() == o3.hashCode());
		assertFalse(o1.hashCode() == o2.hashCode());
		assertFalse(o3.hashCode() == o2.hashCode());
	}
}
