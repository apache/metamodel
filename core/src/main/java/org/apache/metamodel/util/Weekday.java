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

import java.util.Calendar;

/**
 * Provides a handy and type-safe enum around the weekdays otherwise defined as
 * int constants in java.util.Calendar.
 */
public enum Weekday implements HasName {

    MONDAY(Calendar.MONDAY),

    TUESDAY(Calendar.TUESDAY),

    WEDNESDAY(Calendar.WEDNESDAY),

    THURSDAY(Calendar.THURSDAY),

    FRIDAY(Calendar.FRIDAY),

    SATURDAY(Calendar.SATURDAY),

    SUNDAY(Calendar.SUNDAY);

    private int calendarConstant;

    private Weekday(int calendarConstant) {
        this.calendarConstant = calendarConstant;
    }

    public int getCalendarConstant() {
        return calendarConstant;
    }

    public static Weekday getByCalendarConstant(int calendarConstant) {
        for (Weekday weekday : values()) {
            if (weekday.getCalendarConstant() == calendarConstant) {
                return weekday;
            }
        }
        return null;
    }

    public Weekday next() {
        if (this == SUNDAY) {
            return MONDAY;
        }
        return values()[ordinal() + 1];
    }

    public Weekday previous() {
        if (this == MONDAY) {
            return SUNDAY;
        }
        return values()[ordinal() - 1];
    }

    @Override
    public String getName() {
        final String capitalized = toString();
        return capitalized.charAt(0) + capitalized.substring(1).toLowerCase();
    }
}
