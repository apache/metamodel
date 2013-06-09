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

/**
 * Uses the toString method for comparison of objects
 */
public final class ToStringComparator implements Comparator<Object> {

	private static Comparator<Object> _instance = new ToStringComparator();

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	private ToStringComparator() {
	}

	public static Comparable<Object> getComparable(final Object o) {
		final String s = o.toString();
		return new Comparable<Object>() {
			
			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o2) {
				return _instance.compare(s, o2);
			}

			@Override
			public String toString() {
				return "ToStringComparable[string=" + s + "]";
			}
		};
	}

	public int compare(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return -1;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		return o1.toString().compareTo(o2.toString());
	}
}