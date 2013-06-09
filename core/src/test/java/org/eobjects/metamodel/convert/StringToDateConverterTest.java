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
package org.eobjects.metamodel.convert;

import java.text.DateFormat;
import java.util.Date;

import junit.framework.TestCase;

import org.eobjects.metamodel.util.DateUtils;
import org.eobjects.metamodel.util.Month;

public class StringToDateConverterTest extends TestCase {

	public void testToVirtualSimpleDateFormat() throws Exception {
		StringToDateConverter conv = new StringToDateConverter("yyyy-MM-dd");
		assertNull(conv.toVirtualValue(null));
		assertNull(conv.toVirtualValue(""));

		assertEquals(DateUtils.get(2010, Month.DECEMBER, 31),
				conv.toVirtualValue("2010-12-31"));
	}

	public void testToVirtualNoArgs() throws Exception {
		StringToDateConverter conv = new StringToDateConverter();
		assertNull(conv.toVirtualValue(null));
		assertNull(conv.toVirtualValue(""));

		assertEquals(DateUtils.get(2010, Month.DECEMBER, 31),
				conv.toVirtualValue("2010-12-31"));
	}

	public void testToPhysicalSimpleDateFormat() throws Exception {
		StringToDateConverter conv = new StringToDateConverter("yyyy-MM-dd");
		assertNull(conv.toPhysicalValue(null));
		Date input = DateUtils.get(2010, Month.DECEMBER, 31);
		String physicalValue = conv.toPhysicalValue(input);
		assertEquals("2010-12-31", physicalValue);
	}

	public void testToPhysicalNoArgs() throws Exception {
		StringToDateConverter conv = new StringToDateConverter();
		assertNull(conv.toPhysicalValue(null));
		Date input = DateUtils.get(2010, Month.DECEMBER, 31);
		String physicalValue = conv.toPhysicalValue(input);
		Date virtualValue = DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
				DateFormat.MEDIUM).parse(physicalValue);
		assertEquals(virtualValue, input);
	}
}
