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
 * Comparator that can compare numbers of various kinds (short, integer, float,
 * double etc)
 */
public final class NumberComparator implements Comparator<Object> {

	private static final Logger logger = LoggerFactory
			.getLogger(NumberComparator.class);

	private static final Comparator<Object> _instance = new NumberComparator();

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	private NumberComparator() {
	}

	public static Comparable<Object> getComparable(Object o) {
		final Number n = toNumber(o);
		return new Comparable<Object>() {

			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o) {
				return _instance.compare(n, o);
			}

			@Override
			public String toString() {
				return "NumberComparable[number=" + n + "]";
			}

		};
	}

	public int compare(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return 0;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		Number n1 = toNumber(o1);
		Number n2 = toNumber(o2);
		return Double.valueOf(n1.doubleValue()).compareTo(n2.doubleValue());
	}

	public static Number toNumber(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return (Number) value;
		} else if (value instanceof Boolean) {
			if (Boolean.TRUE.equals(value)) {
				return 1;
			} else {
				return 0;
			}
		} else {
			String stringValue = value.toString();
			try {
				return Integer.parseInt(stringValue);
			} catch (NumberFormatException e1) {
				try {
					return Double.parseDouble(stringValue);
				} catch (NumberFormatException e2) {
					logger.warn(
							"Could not convert '{}' to number, returning null",
							value);
					return null;
				}
			}
		}
	}
}