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
 * Provides a handy and type-safe enum around the months otherwise defined as
 * int constants in java.util.Calendar.
 */
public enum Month implements HasName {

    JANUARY(Calendar.JANUARY),

    FEBRUARY(Calendar.FEBRUARY),

    MARCH(Calendar.MARCH),

    APRIL(Calendar.APRIL),

    MAY(Calendar.MAY),

    JUNE(Calendar.JUNE),

    JULY(Calendar.JULY),

    AUGUST(Calendar.AUGUST),

    SEPTEMBER(Calendar.SEPTEMBER),

    OCTOBER(Calendar.OCTOBER),

    NOVEMBER(Calendar.NOVEMBER),

    DECEMBER(Calendar.DECEMBER);

    private int calendarConstant;

    private Month(int calendarConstant) {
        this.calendarConstant = calendarConstant;
    }

    public int getCalendarConstant() {
        return calendarConstant;
    }

    public static Month getByCalendarConstant(int calendarConstant) {
        for (Month month : values()) {
            if (month.getCalendarConstant() == calendarConstant) {
                return month;
            }
        }
        return null;
    }

    public Month next() {
        if (this == DECEMBER) {
            return JANUARY;
        }
        return values()[ordinal() + 1];
    }

    public Month previous() {
        if (this == JANUARY) {
            return DECEMBER;
        }
        return values()[ordinal() - 1];
    }

    @Override
    public String getName() {
        final String capitalized = toString();
        return capitalized.charAt(0) + capitalized.substring(1).toLowerCase();
    }
}
