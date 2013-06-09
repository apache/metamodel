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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General purpose comparator to use for objects of various kinds. Prevents
 * NullPointerExceptions and tries to use comparable interface if available and
 * appropriate on incoming objects.
 */
public final class ObjectComparator implements Comparator<Object> {

	private static final Logger logger = LoggerFactory
			.getLogger(ObjectComparator.class);

	private static final Comparator<Object> _instance = new ObjectComparator();

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	private ObjectComparator() {
	}

	public static Comparable<Object> getComparable(final Object o) {
		return new Comparable<Object>() {

			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o2) {
				return _instance.compare(o, o2);
			}

			@Override
			public String toString() {
				return "ObjectComparable[object=" + o + "]";
			}
		};
	}

	@SuppressWarnings("unchecked")
	public int compare(final Object o1, final Object o2) {
		logger.debug("compare({},{})", o1, o2);
		if (o1 == null && o2 == null) {
			return 0;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		if (o1 instanceof Number && o1 instanceof Number) {
			return NumberComparator.getComparator().compare(o1, o2);
		}
		if (TimeComparator.isTimeBased(o1) && TimeComparator.isTimeBased(o2)) {
			return TimeComparator.getComparator().compare(o1, o2);
		}
		if (BooleanComparator.isBoolean(o1) && BooleanComparator.isBoolean(o2)) {
			return BooleanComparator.getComparator().compare(o1, o2);
		}
		if (o1 instanceof Comparable && o2 instanceof Comparable) {
			@SuppressWarnings("rawtypes")
			Comparable c1 = (Comparable) o1;
			@SuppressWarnings("rawtypes")
			Comparable c2 = (Comparable) o2;
			// We can only count on using the comparable interface if o1 and o2
			// are within of the same class or if one is a subclass of the other
			if (c1.getClass().isAssignableFrom(c2.getClass())) {
				return c1.compareTo(o2);
			}
			if (o2.getClass().isAssignableFrom(c1.getClass())) {
				return -1 * c2.compareTo(o1);
			}
		}
		logger.info("Using ToStringComparator because no apparent better comparison method could be found");
		return ToStringComparator.getComparator().compare(o1, o2);
	}
}