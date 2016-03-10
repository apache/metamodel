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
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.metamodel.query.QueryParameter;
import org.apache.metamodel.schema.ColumnType;

/**
 * Helper class for formatting
 */
public final class FormatHelper {

    /**
     * Creates a uniform number format which is similar to that of eg. Java
     * doubles. The format will not include thousand separators and it will use
     * a dot as a decimal separator.
     * 
     * @return
     */
    public static NumberFormat getUiNumberFormat() {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setDecimalSeparator('.');
        DecimalFormat format = new DecimalFormat("###.##", symbols);
        format.setGroupingUsed(false);
        format.setMaximumFractionDigits(Integer.MAX_VALUE);
        return format;
    }

    public static NumberFormat getSqlNumberFormat() {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setDecimalSeparator('.');
        DecimalFormat format = new DecimalFormat("###.##", symbols);
        format.setGroupingUsed(false);
        format.setMaximumFractionDigits(100);
        return format;
    }

    public static String formatSqlBoolean(ColumnType columnType, boolean b) {
        if (columnType == ColumnType.BIT) {
            if (b) {
                return "1";
            } else {
                return "0";
            }
        } else {
            if (b) {
                return "TRUE";
            } else {
                return "FALSE";
            }
        }
    }

    /**
     * Formats a date according to a specific column type (DATE, TIME or
     * TIMESTAMP)
     * 
     * @param columnType
     *            the column type
     * @param date
     *            the date value
     * @return
     */
    public static String formatSqlTime(ColumnType columnType, Date date) {
        return formatSqlTime(columnType, date, true);
    }

    /**
     * Formats a date according to a specific column type (DATE, TIME or
     * TIMESTAMP)
     * 
     * @param columnType
     *            the column type
     * @param date
     *            the date value
     * @param typeCastDeclaration
     *            whether or not to include a type cast declaration
     * @param beforeDateLiteral
     *            before date literal
     * @param afterDateLiteral
     *            after date literal
     * @return
     */
    public static String formatSqlTime(ColumnType columnType, Date date, boolean typeCastDeclaration,
            String beforeDateLiteral, String afterDateLiteral) {
        if (columnType == null) {
            throw new IllegalArgumentException("Column type cannot be null");
        }
        final DateFormat format;
        final String typePrefix;
        if (columnType.isTimeBased()) {
            if (columnType == ColumnType.DATE) {
                format = DateUtils.createDateFormat("yyyy-MM-dd");
                typePrefix = "DATE";
            } else if (columnType == ColumnType.TIME) {
                format = DateUtils.createDateFormat("HH:mm:ss");
                typePrefix = "TIME";
            } else {
                format = DateUtils.createDateFormat("yyyy-MM-dd HH:mm:ss");
                typePrefix = "TIMESTAMP";
            }

        } else {
            throw new IllegalArgumentException("Cannot format time value of type: " + columnType);
        }

        if (typeCastDeclaration) {
            return typePrefix + " " + beforeDateLiteral + format.format(date) + afterDateLiteral;
        } else {
            return format.format(date);
        }
    }

    /**
     * Formats a date according to a specific column type (DATE, TIME or
     * TIMESTAMP). For backward compatibility.
     * 
     * @param columnType
     * @param date
     * @param typeCastDeclaration
     * @return
     */
    public static String formatSqlTime(ColumnType columnType, Date date, boolean typeCastDeclaration) {
        return formatSqlTime(columnType, date, typeCastDeclaration, "\'", "\'");
    }

    /**
     * Parses a SQL string representation of a time based value
     * 
     * @param columnType
     * @param value
     * @return
     */
    public static Date parseSqlTime(ColumnType columnType, String value) {
        final String[] formats;
        if (columnType.isTimeBased()) {
            if (columnType == ColumnType.DATE) {
                formats = new String[] { "yyyy-MM-dd" };
            } else if (columnType == ColumnType.TIME) {
                formats = new String[] { "HH:mm:ss", "HH:mm" };
            } else {
                formats = new String[] { "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };
            }
        } else {
            throw new IllegalArgumentException("Cannot parse time value of type: " + columnType);
        }

        for (String format : formats) {
            try {
                DateFormat dateFormat = DateUtils.createDateFormat(format);
                return dateFormat.parse(value);
            } catch (ParseException e) {
                // proceed to next format
            }
        }

        throw new IllegalArgumentException("String value '" + value + "' not parseable as a " + columnType);
    }

    public static String formatSqlValue(ColumnType columnType, Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof QueryParameter) {
            return value.toString();
        }
        if (value.getClass().isArray()) {
            value = CollectionUtils.toList(value);
        }
        if (value instanceof Iterable) {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            for (Object item : (Iterable<?>) value) {
                if (sb.length() > 1) {
                    sb.append(" , ");
                }
                sb.append(formatSqlValue(columnType, item));
            }
            sb.append(')');
            return sb.toString();
        } else if (isNumber(columnType, value)) {
            NumberFormat numberFormat = getSqlNumberFormat();
            Number n = NumberComparator.toNumber(value);
            if (n == null) {
                throw new IllegalStateException("Could not convert " + value + " to number");
            }
            String numberString = numberFormat.format(n);
            return numberString;
        } else if (isBoolean(columnType, value)) {
            Boolean b = BooleanComparator.toBoolean(value);
            if (b == null) {
                throw new IllegalStateException("Could not convert " + value + " to boolean");
            }
            String booleanString = formatSqlBoolean(columnType, b);
            return booleanString;
        } else if (isTimeBased(columnType, value)) {
            Date date = TimeComparator.toDate(value);
            if (date == null) {
                throw new IllegalStateException("Could not convert " + value + " to date");
            }
            String timeString = formatSqlTime(columnType, date);
            return timeString;
        } else if (isLiteral(columnType, value)) {
            return '\'' + value.toString() + '\'';
        } else {
            if (columnType == null) {
                throw new IllegalStateException("Value type not supported: " + value);
            }
            throw new IllegalStateException("Column type not supported: " + columnType);
        }
    }

    private static boolean isTimeBased(ColumnType columnType, Object operand) {
        if (columnType == null) {
            return TimeComparator.isTimeBased(operand);
        }
        return columnType.isTimeBased();
    }

    private static boolean isBoolean(ColumnType columnType, Object operand) {
        if (columnType == null) {
            return operand instanceof Boolean;
        }
        return columnType.isBoolean();
    }

    private static boolean isNumber(ColumnType columnType, Object operand) {
        if (columnType == null) {
            return operand instanceof Number;
        }
        return columnType.isNumber();
    }

    private static boolean isLiteral(ColumnType columnType, Object operand) {
        if (columnType == null) {
            return operand instanceof String;
        }
        return columnType.isLiteral();
    }
}