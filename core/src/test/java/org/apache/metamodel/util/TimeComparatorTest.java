/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.util;

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