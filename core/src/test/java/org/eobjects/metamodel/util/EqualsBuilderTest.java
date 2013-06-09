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

import junit.framework.TestCase;

public class EqualsBuilderTest extends TestCase {

	public void testEquals() throws Exception {
		assertTrue(EqualsBuilder.equals(null, null));
		assertTrue(EqualsBuilder.equals("hello", "hello"));
		assertFalse(EqualsBuilder.equals("hello", null));
		assertFalse(EqualsBuilder.equals(null, "hello"));
		assertFalse(EqualsBuilder.equals("world", "hello"));

		MyCloneable o1 = new MyCloneable();
		assertTrue(EqualsBuilder.equals(o1, o1));
		MyCloneable o2 = o1.clone();
		assertFalse(EqualsBuilder.equals(o1, o2));
	}
	
	static final class MyCloneable implements Cloneable {
		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public MyCloneable clone() {
			try {
				return (MyCloneable) super.clone();
			} catch (CloneNotSupportedException e) {
				throw new UnsupportedOperationException();
			}
		}
	};
}
