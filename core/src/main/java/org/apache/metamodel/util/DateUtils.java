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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Various utility methods pertaining to date handling
 */
public final class DateUtils {

	public static final long MILLISECONDS_PER_SECOND = 1000;
	public static final long MILLISECONDS_PER_MINUTE = MILLISECONDS_PER_SECOND * 60;
	public static final long MILLISECONDS_PER_HOUR = MILLISECONDS_PER_MINUTE * 60;
	public static final long MILLISECONDS_PER_DAY = MILLISECONDS_PER_HOUR * 24;

	private DateUtils() {
		// prevent instantiation
	}

	public static Date get(int year, Month month, int dayOfMonth) {
		Calendar c = createCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month.getCalendarConstant());
		c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
		c.set(Calendar.HOUR, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		c.set(Calendar.AM_PM, 0);
		return c.getTime();
	}

	public static Date get(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		c.set(Calendar.AM_PM, 0);
		return c.getTime();
	}

	public static Date get(Date originalDate, int daysDiff) {
		long millis = originalDate.getTime();
		long diff = daysDiff * MILLISECONDS_PER_DAY;
		millis = millis + diff;
		return new Date(millis);
	}

	public static int getYear(Date date) {
		Calendar cal = createCalendar();
		cal.setTime(date);
		return cal.get(Calendar.YEAR);
	}

	public static Month getMonth(Date date) {
		Calendar cal = createCalendar();
		cal.setTime(date);
		int monthConstant = cal.get(Calendar.MONTH);
		return Month.getByCalendarConstant(monthConstant);
	}

	public static Weekday getWeekday(Date date) {
		Calendar cal = createCalendar();
		cal.setTime(date);
		int weekdayConstant = cal.get(Calendar.DAY_OF_WEEK);
		return Weekday.getByCalendarConstant(weekdayConstant);
	}

	public static int getDayOfMonth(Date date) {
		Calendar cal = createCalendar();
		cal.setTime(date);
		return cal.get(Calendar.DAY_OF_MONTH);
	}

	public static Calendar createCalendar() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(0l);
		return c;
	}

	public static DateFormat createDateFormat() {
		return createDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public static DateFormat createDateFormat(String datePattern) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
		return dateFormat;
	}
}
