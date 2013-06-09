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

import junit.framework.TestCase;

public class WeekdayTest extends TestCase {

    public void testGetName() throws Exception {
        assertEquals("Monday", Weekday.MONDAY.getName());
    }
    
    public void testNext() throws Exception {
        assertEquals(Weekday.TUESDAY, Weekday.MONDAY.next());
        assertEquals(Weekday.MONDAY, Weekday.SUNDAY.next());
    }
    
    public void testPrevious() throws Exception {
        assertEquals(Weekday.SUNDAY, Weekday.MONDAY.previous());
        assertEquals(Weekday.SATURDAY, Weekday.SUNDAY.previous());
    }
}
