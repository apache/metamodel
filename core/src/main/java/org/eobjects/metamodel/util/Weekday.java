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

import java.util.Calendar;

/**
 * Provides a handy and type-safe enum around the weekdays otherwise defined as
 * int constants in java.util.Calendar.
 * 
 * @author Kasper Sørensen
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
