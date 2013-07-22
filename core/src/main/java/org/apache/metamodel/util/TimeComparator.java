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
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares dates of various formats. Since this class has unchecked generic
 * conversion it can compare java.util.Date, java.sql.Date, java.sql.Time,
 * java.util.Calendar, Date-formatted strings etc.
 */
public final class TimeComparator implements Comparator<Object> {

    private static final Logger logger = LoggerFactory.getLogger(TimeComparator.class);

    private static final String[] prototypePatterns = { "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm", "HH:mm:ss.SSS", "yyyy-MM-dd", "dd-MM-yyyy", "yy-MM-dd", "MM-dd-yyyy", "HH:mm:ss",
            "HH:mm" };

    private static final Comparator<Object> _instance = new TimeComparator();

    public static Comparator<Object> getComparator() {
        return _instance;
    }

    private TimeComparator() {
    }

    public static Comparable<Object> getComparable(final Object o) {
        final Date dt1 = toDate(o);
        return new Comparable<Object>() {

            @Override
            public boolean equals(Object obj) {
                return _instance.equals(obj);
            }

            public int compareTo(Object o) {
                return _instance.compare(dt1, o);
            }

            @Override
            public String toString() {
                return "TimeComparable[time=" + dt1 + "]";
            }
        };
    }

    public int compare(final Object o1, final Object o2) {
        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        try {
            Date dt1 = toDate(o1);
            Date dt2 = toDate(o2);
            return dt1.compareTo(dt2);
        } catch (Exception e) {
            logger.error("Could not compare {} and {}", o1, o2);
            throw new RuntimeException(e);
        }
    }

    public static Date toDate(final Object value) {
        Date result = null;
        if (value == null) {
            result = null;
        } else if (value instanceof Date) {
            result = (Date) value;
        } else if (value instanceof Calendar) {
            result = ((Calendar) value).getTime();
        } else if (value instanceof String) {
            result = convertFromString((String) value);
        } else if (value instanceof Number) {
            result = convertFromNumber((Number) value);
        }

        if (result == null) {
            logger.warn("Could not convert '{}' to date, returning null", value);
        }

        return result;
    }

    public static boolean isTimeBased(Object o) {
        if (o instanceof Date || o instanceof Calendar) {
            return true;
        }
        return false;
    }

    private static Date convertFromString(final String value) {
        try {
            long longValue = Long.parseLong(value);
            return convertFromNumber(longValue);
        } catch (NumberFormatException e) {
            // do nothing, proceed to dateFormat parsing
        }

        // try with Date.toString() date format first
        try {
            DateFormatSymbols dateFormatSymbols = DateFormatSymbols.getInstance(Locale.US);
            SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", dateFormatSymbols);
            return dateFormat.parse(value);
        } catch (ParseException e) {
            // do noting
        }

        for (String prototypePattern : prototypePatterns) {
            if (prototypePattern.length() == value.length()) {
                DateFormat dateFormat;
                try {
                    dateFormat = DateUtils.createDateFormat(prototypePattern);
                    return dateFormat.parse(value);
                } catch (Exception e) {
                    // proceed to next formatter
                }

                if (prototypePattern.indexOf('-') != -1) {
                    // try with '.' in stead of '-'
                    try {
                        dateFormat = DateUtils.createDateFormat(prototypePattern.replaceAll("\\-", "\\."));
                        return dateFormat.parse(value);
                    } catch (Exception e) {
                        // proceed to next formatter
                    }

                    // try with '/' in stead of '-'
                    try {
                        dateFormat = DateUtils.createDateFormat(prototypePattern.replaceAll("\\-", "\\/"));
                        return dateFormat.parse(value);
                    } catch (Exception e) {
                        // proceed to next formatter
                    }
                }
            }

        }

        return null;
    }

    private static Date convertFromNumber(Number value) {
        Number numberValue = (Number) value;
        long longValue = numberValue.longValue();

        String stringValue = Long.toString(longValue);
        // test if the number is actually a format of the type yyyyMMdd
        if (stringValue.length() == 8 && (stringValue.startsWith("1") || stringValue.startsWith("2"))) {
            try {
                return DateUtils.createDateFormat("yyyyMMdd").parse(stringValue);
            } catch (Exception e) {
                // do nothing, proceed to next method of conversion
            }
        }

        // test if the number is actually a format of the type yyMMdd
        if (stringValue.length() == 6) {
            try {
                return DateUtils.createDateFormat("yyMMdd").parse(stringValue);
            } catch (Exception e) {
                // do nothing, proceed to next method of conversion
            }
        }

        if (longValue > 5000000) {
            // this number is most probably amount of milliseconds since
            // 1970
            return new Date(longValue);
        } else {
            // this number is most probably the amount of days since
            // 1970
            return new Date(longValue * 1000 * 60 * 60 * 24);
        }
    }
}