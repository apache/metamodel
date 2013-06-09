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
 * Provides a handy and type-safe enum around the months otherwise defined as
 * int constants in java.util.Calendar.
 * 
 * @author Kasper Sørensen
 * 
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
