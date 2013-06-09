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

import java.text.DateFormat;
import java.util.Comparator;
import java.util.Date;

import junit.framework.TestCase;

public class TimeComparatorTest extends TestCase {

	public void testCompare() throws Exception {
		Comparator<Object> c = TimeComparator.getComparator();
		Date d1 = new Date();
		Thread.sleep(100);
		Date d2 = new Date();
		assertEquals(0, c.compare(d1, d1));
		assertEquals(-1, c.compare(d1, d2));
		assertEquals(1, c.compare(d2, d1));

		assertEquals(1, c.compare(d2, "2005-10-08"));
		assertEquals(1, c.compare("2006-11-09", "2005-10-08"));
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = TimeComparator
				.getComparable(new Date());
		Thread.sleep(100);
		assertEquals(-1, comparable.compareTo(new Date()));
	}

	public void testToDate() throws Exception {
		DateFormat dateFormat = DateUtils
				.createDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		assertEquals("2008-11-04 00:00:00.000",
				dateFormat.format(TimeComparator.toDate("08-11-04")));

		assertEquals("2010-09-21 14:06:00.000",
				dateFormat.format(TimeComparator.toDate("2010-09-21 14:06")));

		assertEquals("2010-09-21 14:06:13.000",
				dateFormat.format(TimeComparator.toDate("2010-09-21 14:06:13")));

		assertEquals("2010-09-21 14:06:13.009",
				dateFormat.format(TimeComparator
						.toDate("2010-09-21 14:06:13.009")));

		assertEquals("2000-12-31 02:30:05.100",
				dateFormat.format(TimeComparator
						.toDate("2000-12-31 02:30:05.100")));
	}

	public void testToDateOfDateToString() throws Exception {
		Date date = new Date();
		String dateString = date.toString();
		Date convertedDate = TimeComparator.toDate(dateString);
		
		String convertedToString = convertedDate.toString();
		assertEquals(dateString, convertedToString);
	}
}