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

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.metamodel.schema.ColumnType;

import junit.framework.TestCase;

public class FormatHelperTest extends TestCase {

	public void testNumberFormat() throws Exception {
		NumberFormat format = FormatHelper.getUiNumberFormat();
		assertEquals("987643.213456343", format.format(987643.213456343));
		assertEquals("0.218456343", format.format(0.218456343));
		assertEquals("20.1", format.format(20.1));
	}

	public void testFormatSqlValue() throws Exception {
		assertEquals("'foo'", FormatHelper.formatSqlValue(null, "foo"));
		assertEquals("1", FormatHelper.formatSqlValue(null, 1));
		assertEquals("NULL", FormatHelper.formatSqlValue(null, null));
		assertEquals(
				"TIMESTAMP '2011-07-24 00:00:00'",
				FormatHelper.formatSqlValue(ColumnType.TIMESTAMP,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"DATE '2011-07-24'",
				FormatHelper.formatSqlValue(ColumnType.DATE,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"TIME '00:00:00'",
				FormatHelper.formatSqlValue(ColumnType.TIME,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"('foo' , 1 , 'bar' , 0.1234)",
				FormatHelper.formatSqlValue(null,
						Arrays.asList("foo", 1, "bar", 0.1234)));
		assertEquals(
				"('foo' , 1 , 'bar' , 0.1234)",
				FormatHelper.formatSqlValue(null, new Object[] { "foo", 1,
						"bar", 0.1234 }));
	}

    public void testParseTimeSqlValue() throws Exception {
        final Calendar c = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault());

        c.setTimeInMillis(0);
        c.set(Calendar.YEAR, 2011);
        c.set(Calendar.MONTH, Month.JULY.getCalendarConstant());
        c.set(Calendar.DAY_OF_MONTH, 24);
        c.set(Calendar.HOUR_OF_DAY, 17);
        c.set(Calendar.MINUTE, 34);
        c.set(Calendar.SECOND, 56);
        final Date timestampFullSeconds = c.getTime();
        c.set(Calendar.MILLISECOND, 413);
        final Date timestampFullMillis = c.getTime();

        c.setTimeInMillis(0);
        c.set(Calendar.YEAR, 2011);
        c.set(Calendar.MONTH, Month.JULY.getCalendarConstant());
        c.set(Calendar.DAY_OF_MONTH, 24);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        final Date dateOnly = c.getTime();

        c.setTimeInMillis(0);
        c.set(Calendar.YEAR, 1970);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, 17);
        c.set(Calendar.MINUTE, 34);
        c.set(Calendar.SECOND, 56);
        final Date timeOnlySeconds = c.getTime();
        c.set(Calendar.MILLISECOND, 413);
        final Date timeOnlyMillis = c.getTime();

        // Test parsing of formatted date/time values
        final String dateStr = FormatHelper.formatSqlValue(ColumnType.DATE, timestampFullSeconds);
        final Date parsedOnlyDate = FormatHelper.parseSqlTime(ColumnType.DATE, dateStr);
        assertEquals(dateOnly, parsedOnlyDate);

        final String timeStr = FormatHelper.formatSqlValue(ColumnType.TIME, timestampFullSeconds);
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, timeStr));

        final String timestampStr = FormatHelper.formatSqlValue(ColumnType.TIMESTAMP, timestampFullSeconds);
        assertEquals(timestampFullSeconds, FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, timestampStr));

        // Now tests some specific cases
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "DATE '2011-07-24'"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "DATE'2011-07-24'"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "DATE \"2011-07-24\""));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "DATE\"2011-07-24\""));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "DATE (2011-07-24)"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "DATE(2011-07-24)"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "2011-07-24"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "'2011-07-24'"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "\"2011-07-24\""));

        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME '17:34:56'"));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME'17:34:56'"));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME \"17:34:56\""));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME\"17:34:56\""));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME (17:34:56)"));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME(17:34:56)"));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "'17:34:56'"));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "\"17:34:56\""));
        assertEquals(timeOnlySeconds, FormatHelper.parseSqlTime(ColumnType.TIME, "17:34:56"));
        assertEquals(timeOnlyMillis, FormatHelper.parseSqlTime(ColumnType.TIME, "TIME '17:34:56.413'"));

        assertEquals(timestampFullSeconds,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP '2011-07-24 17:34:56'"));
        assertEquals(timestampFullSeconds,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP'2011-07-24 17:34:56'"));
        assertEquals(timestampFullSeconds,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP \"2011-07-24 17:34:56\""));
        assertEquals(timestampFullSeconds,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP\"2011-07-24 17:34:56\""));
        assertEquals(timestampFullSeconds,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP (2011-07-24 17:34:56)"));
        assertEquals(timestampFullSeconds,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP(2011-07-24 17:34:56)"));
        assertEquals(timestampFullSeconds, FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "'2011-07-24 17:34:56'"));
        assertEquals(timestampFullSeconds, FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "\"2011-07-24 17:34:56\""));
        assertEquals(timestampFullSeconds, FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "2011-07-24 17:34:56"));
        assertEquals(timestampFullMillis,
                FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "TIMESTAMP '2011-07-24 17:34:56.413'"));
        assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.TIMESTAMP, "2011-07-24"));

        try {
            FormatHelper.parseSqlTime(ColumnType.DATE, "XXX '2011-07-24'");
            fail("should fail");
        } catch (IllegalArgumentException e) {
            // OK
        }

        try {
            assertEquals(dateOnly, FormatHelper.parseSqlTime(ColumnType.DATE, "TIME '2011-07-24'"));
            fail("should fail");
        } catch (IllegalArgumentException e) {
            // OK
        }

        try {
            assertEquals(timestampFullSeconds,
                    FormatHelper.parseSqlTime(ColumnType.TIME, "TIMESTAMP '2011-07-24 17:34:56'"));
            fail("should fail");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }
}