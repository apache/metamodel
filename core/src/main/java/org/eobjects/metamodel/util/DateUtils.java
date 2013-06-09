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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Various utility methods pertaining to date handling
 * 
 * @author Kasper Sørensen
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
		return c.getTime();
	}

	public static Date get(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

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
