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
 * Comparator of booleans
 * 
 * @author Kasper Sørensen
 */
public final class BooleanComparator implements Comparator<Object> {

	private static final Logger logger = LoggerFactory
			.getLogger(BooleanComparator.class);

	private static BooleanComparator _instance = new BooleanComparator();

	private BooleanComparator() {
	}

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	public static Comparable<Object> getComparable(Object object) {
		final Boolean b = toBoolean(object);
		return new Comparable<Object>() {

			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o) {
				return _instance.compare(b, o);
			}

			@Override
			public String toString() {
				return "BooleanComparable[boolean=" + b + "]";
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
		Boolean b1 = toBoolean(o1);
		Boolean b2 = toBoolean(o2);
		return b1.compareTo(b2);
	}

	public static Boolean toBoolean(Object o) {
		if (o == null) {
			return null;
		}

		if (o instanceof Boolean) {
			return (Boolean) o;
		}
		if (o instanceof String) {
			try {
				return parseBoolean((String) o);
			} catch (IllegalArgumentException e) {
				logger.warn(
						"Could not convert String '{}' to boolean, returning false", o);
				return false;
			}
		}
		if (o instanceof Number) {
			int i = ((Number) o).intValue();
			return i >= 1;
		}
		
		logger.warn(
				"Could not convert '{}' to boolean, returning false",
				o);
		return false;
	}

	/**
	 * Parses a string and returns a boolean representation of it. To parse the
	 * string the following values will be accepted, irrespective of case.
	 * <ul>
	 * <li>true</li>
	 * <li>false</li>
	 * <li>1</li>
	 * <li>0</li>
	 * <li>yes</li>
	 * <li>no</li>
	 * <li>y</li>
	 * <li>n</li>
	 * </ul>
	 * 
	 * @param string
	 *            the string to parse
	 * @return a boolean
	 * @throws IllegalArgumentException
	 *             if the string provided is null or cannot be parsed as a
	 *             boolean
	 */
	public static boolean parseBoolean(String string)
			throws IllegalArgumentException {
		if (string == null) {
			throw new IllegalArgumentException("string cannot be null");
		}
		string = string.trim();
		if ("true".equalsIgnoreCase(string) || "1".equals(string)
				|| "y".equalsIgnoreCase(string)
				|| "yes".equalsIgnoreCase(string)) {
			return true;
		} else if ("false".equalsIgnoreCase(string) || "0".equals(string)
				|| "n".equalsIgnoreCase(string)
				|| "no".equalsIgnoreCase(string)) {
			return false;
		} else {
			throw new IllegalArgumentException(
					"Could not get boolean value of string: " + string);
		}
	}

	public static boolean isBoolean(Object o) {
		if (o instanceof Boolean) {
			return true;
		}
		if (o instanceof String) {
			if ("true".equalsIgnoreCase((String) o)
					|| "false".equalsIgnoreCase((String) o)) {
				return true;
			}
		}
		return false;
	}

}
