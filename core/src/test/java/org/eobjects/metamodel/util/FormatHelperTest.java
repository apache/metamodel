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

import java.text.NumberFormat;
import java.util.Arrays;

import org.eobjects.metamodel.schema.ColumnType;

import junit.framework.TestCase;

public class FormatHelperTest extends TestCase {

	public void testNumberFormat() throws Exception {
		NumberFormat format = FormatHelper.getUiNumberFormat();
		assertEquals("987643.21", format.format(987643.213456343));
		assertEquals("0.22", format.format(0.218456343));
		assertEquals("20.1", format.format(20.1));
	}

	@SuppressWarnings("unchecked")
	public void testFormatSqlValue() throws Exception {
		assertEquals("'foo'", FormatHelper.formatSqlValue(null, "foo"));
		assertEquals("1", FormatHelper.formatSqlValue(null, 1));
		assertEquals("NULL", FormatHelper.formatSqlValue(null, null));
		assertEquals(
				"TIMESTAMP '2011-07-24 00:00:00'",
				FormatHelper.formatSqlValue(ColumnType.TIMESTAMP,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"DATE '2011-07-24'",
				FormatHelper.formatSqlValue(ColumnType.DATE,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"TIME '00:00:00'",
				FormatHelper.formatSqlValue(ColumnType.TIME,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"('foo' , 1 , 'bar' , 0.1234)",
				FormatHelper.formatSqlValue(null,
						Arrays.asList("foo", 1, "bar", 0.1234)));
		assertEquals(
				"('foo' , 1 , 'bar' , 0.1234)",
				FormatHelper.formatSqlValue(null, new Object[] { "foo", 1,
						"bar", 0.1234 }));
	}
}