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
package org.apache.metamodel.convert;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

import org.apache.metamodel.util.TimeComparator;

/**
 * A {@link TypeConverter} that converts String values (on the physical layer)
 * to interpreted {@link Date}s.
 */
public class StringToDateConverter implements TypeConverter<String, Date> {

    private final Function<Date, String> _serializeFunc;
    private final Function<String, Date> _deserializeFunc;

    /**
     * Constructs a new {@link StringToDateConverter} which will use the
     * {@link TimeComparator#toDate(Object)} method for parsing dates and the
     * {@link DateFormat#MEDIUM} date time format for physical representation.
     */
    public StringToDateConverter() {
        _deserializeFunc = stringValue -> {
            return TimeComparator.toDate(stringValue);
        };
        _serializeFunc = date -> {
            return DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(date);
        };
    }

    /**
     * Constructs a new {@link StringToDateConverter} using a given date
     * pattern.
     * 
     * @param datePattern
     *            a String date pattern, corresponding to the syntax of a
     *            {@link SimpleDateFormat}.
     */
    public StringToDateConverter(String datePattern) {
        this(new SimpleDateFormat(datePattern));
    }

    /**
     * Constructs a new {@link StringToDateConverter} using a given
     * {@link DateFormat}.
     * 
     * @param dateFormat
     *            the {@link DateFormat} to use for parsing and formatting
     *            dates.
     */
    public StringToDateConverter(final DateFormat dateFormat) {
        if (dateFormat == null) {
            throw new IllegalArgumentException("DateFormat cannot be null");
        }
        _deserializeFunc = string -> {
            try {
                return dateFormat.parse(string);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Could not parse date string: " + string);
            }
        };
        _serializeFunc = date -> {
            return dateFormat.format(date);
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toPhysicalValue(Date virtualValue) {
        if (virtualValue == null) {
            return null;
        }
        return _serializeFunc.apply(virtualValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toVirtualValue(String physicalValue) {
        if (physicalValue == null || physicalValue.length() == 0) {
            return null;
        }
        return _deserializeFunc.apply(physicalValue);
    }

}
