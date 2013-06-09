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

import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;

public class DateUtilsTest extends TestCase {

	public void testGet() throws Exception {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date christmasDay = DateUtils.get(2010, Month.DECEMBER, 24);
		assertEquals("2010-12-24 00:00:00", f.format(christmasDay));
		assertEquals(Weekday.FRIDAY, DateUtils.getWeekday(christmasDay));
		
		Date date2 = DateUtils.get(christmasDay, 1);
		assertEquals("2010-12-25 00:00:00", f.format(date2));
		
		Date date3 = DateUtils.get(christmasDay, 10);
		assertEquals("2011-01-03 00:00:00", f.format(date3));
	}
}
